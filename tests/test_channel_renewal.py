"""安全通道续约回归测试：把"连上 1 小时整点断线"压缩成几十秒。

背景（现场日志 dist/logs/app.log）::

    11:32:28  Connected ... SecureChannel timeout=3600000ms
    12:32:28  Could not verify security on incoming request.
    12:32:28  Got error status from server: BadTcpSecureChannelUnknown
    12:32:35  [STATE] connected -> disconnected (asyncua watchdog: Connection is closed)

根因：自研续约监督协程的存活判据写成了 self.connected（等价于状态 ==
connected），但该协程是在 transport 建立后、节点加载阶段（状态 == loading）被
create_task 出来的。它第一次被调度时看到的仍是 loading，于是直接跳过循环退出：

    [RENEW] SecureChannel renewal supervisor started (generation 1).
    [RENEW] Renew supervisor task exited.

之后安全通道再无人续约，服务端（DA→UA 网关，token 寿命 1 小时）在整点关闭
通道/拒绝旧 TokenId，客户端才被动断线。本文件分两层钉住这个缺陷：

1. test_renew_supervisor_survives_loading_state：纯单元测试，直接构造
   loading 状态，断言续约协程不会退出、并会在寿命 50% 时真的发起续约；
2. TestRenewalAgainstShortLifetimeGateway：拉起真实的 Basic256Sha256
   asyncua 服务器，把授予的通道寿命压到 6 秒，并在 token 到期未续约时主动
   断开 TCP（模拟生产网关的硬过期）。旧代码在约 6 秒即被切断，新代码连续
   跨过 3 个寿命周期仍保持 connected 且续约 >= 2 次。

运行::

    python -m unittest tests.test_channel_renewal -v
"""
import asyncio
import datetime
import ipaddress
import os
import socket
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

try:
    from asyncua import Server, ua
    from asyncua.common import connection as ua_connection
    from asyncua.crypto.permission_rules import SimpleRoleRuleset
    ASYNCUA_AVAILABLE = True
except ImportError:  # pragma: no cover
    ASYNCUA_AVAILABLE = False

from opc.client_engine import OpcClientEngine, SubHandler

APP_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"
SHORT_LIFETIME_MS = 6_000


# --------------------------------------------------------------------------------------
# 1) 纯单元测试：续约协程不得在 loading 状态被"判定未连接"而退出
# --------------------------------------------------------------------------------------
class TestRenewSupervisorLifecycle(unittest.IsolatedAsyncioTestCase):
    def _make_engine(self, lifetime_ms):
        engine = OpcClientEngine(host="127.0.0.1", port=4840)
        client = MagicMock()
        client.secure_channel_timeout = lifetime_ms
        client.open_secure_channel = AsyncMock(return_value=None)
        client.uaclient._open_secure_channel_exchange = None
        state_node = MagicMock()
        state_node.read_value = AsyncMock(return_value=0)
        client.get_node = MagicMock(return_value=state_node)
        engine.client = client
        engine._generation = 1
        now = time.monotonic()
        engine._channel_connected_at = now
        engine._last_renewed_at = now
        return engine, client

    async def test_renew_supervisor_survives_loading_state(self):
        """复现根因：create_task 时状态是 loading，协程不得立刻退出。

        旧实现第一个 while 条件是 self.connected，loading 状态下为 False，
        协程会立刻 return，任务 done；修复前本用例在第一个断言就失败。
        """
        engine, client = self._make_engine(lifetime_ms=600)
        engine._set_state(OpcClientEngine.STATE_LOADING, "test: transport established")
        try:
            await engine._ensure_link_tasks(engine._generation)
            await asyncio.sleep(0.05)

            task = engine._renew_task
            self.assertIsNotNone(task)
            self.assertFalse(
                task.done(),
                "续约监督在 loading 状态就退出了 —— 安全通道将无人续约，"
                "整点断线的根因",
            )

            # 完成 bootstrap 后进入 connected，续约应如期发生（600ms 寿命 -> 300ms 时续约）
            engine._set_state(OpcClientEngine.STATE_CONNECTED, "test: bootstrap complete")
            for _ in range(40):
                if engine.renewal_count >= 1:
                    break
                await asyncio.sleep(0.05)

            self.assertGreaterEqual(engine.renewal_count, 1, "安全通道没有发生续约")
            client.open_secure_channel.assert_awaited()
            self.assertTrue(
                any(call.kwargs.get("renew") is True for call in client.open_secure_channel.await_args_list),
                "续约调用没有携带 renew=True",
            )
        finally:
            if engine._renew_task is not None:
                engine._renew_task.cancel()
                try:
                    await engine._renew_task
                except (asyncio.CancelledError, Exception):
                    pass

    async def test_zombie_supervisor_is_replaced(self):
        """qasync/Qt processEvents 重入会让刚 create_task 的协程一步都没跑就永久 pending。

        这种任务 done() == False，只检查 done() 永远发现不了。_ensure_link_tasks 必须
        依据"创建时间 vs 真正开始执行时间"识别并替换它，否则现场仍会整点断线。
        """
        engine, _ = self._make_engine(lifetime_ms=3_600_000)
        engine._set_state(OpcClientEngine.STATE_CONNECTED, "test: connected")

        # 模拟一个被重入打断、永远不会被调度的僵尸任务（已创建 5 秒，从未执行）
        zombie = asyncio.create_task(asyncio.sleep(3600))
        engine._renew_task = zombie
        engine._renew_task_created_at = time.monotonic() - 5.0
        engine._renew_started_at = 0.0

        try:
            await engine._ensure_link_tasks(engine._generation)
            self.assertIsNot(engine._renew_task, zombie, "僵尸续约任务没有被替换")
            self.assertGreater(engine._renew_started_at, 0.0, "替换后的续约任务没有真正开始执行")
            self.assertFalse(engine._renew_task.done())
        finally:
            # 先推进世代号，阻止心跳任务把刚取消的续约任务又补拉起来
            engine._generation += 1
            zombie.cancel()
            for attr in ("_monitor_task", "_renew_task"):
                task = getattr(engine, attr, None)
                if task is not None:
                    task.cancel()
            await asyncio.sleep(0)
            for task in (zombie, engine._renew_task, engine._monitor_task):
                if task is not None and not task.done():
                    try:
                        await asyncio.wait_for(asyncio.shield(task), timeout=1.0)
                    except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                        pass

    async def test_dead_supervisor_is_restarted_by_watchdog(self):
        """续约协程静默退出后，心跳看门狗必须把它补拉起来（24/7 自愈）。"""
        engine, _ = self._make_engine(lifetime_ms=3_600_000)
        engine._set_state(OpcClientEngine.STATE_CONNECTED, "test: connected")

        async def _silently_die():
            return None

        engine._renew_task = asyncio.create_task(_silently_die())
        await asyncio.sleep(0.01)
        self.assertTrue(engine._renew_task.done())

        await engine._ensure_link_tasks(engine._generation)
        self.assertIsNotNone(engine._renew_task)
        self.assertFalse(engine._renew_task.done(), "看门狗没有补拉已退出的续约协程")

        engine._renew_task.cancel()
        try:
            await engine._renew_task
        except (asyncio.CancelledError, Exception):
            pass


# --------------------------------------------------------------------------------------
# 2) 端到端测试：短寿命网关 + 硬过期
# --------------------------------------------------------------------------------------
def _free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _make_cert(prefix):
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "opc-renewal-test-server")])
    san = [
        x509.UniformResourceIdentifier(APP_URI),
        x509.DNSName("localhost"),
        x509.IPAddress(ipaddress.IPv4Address("127.0.0.1")),
    ]
    cert = (
        x509.CertificateBuilder()
        .subject_name(name).issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.datetime(2000, 1, 1, tzinfo=datetime.timezone.utc))
        .not_valid_after(datetime.datetime(2099, 12, 31, tzinfo=datetime.timezone.utc))
        .add_extension(x509.SubjectAlternativeName(san), critical=False)
        .add_extension(x509.ExtendedKeyUsage([
            x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        ]), critical=False)
        .sign(key, hashes.SHA256())
    )
    with open(prefix + ".der", "wb") as f:
        f.write(cert.public_bytes(serialization.Encoding.DER))
    with open(prefix + ".pem", "wb") as f:
        f.write(key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        ))
    return prefix + ".der", prefix + ".pem"


_ORIGINAL_OPEN = None
_RENEW_TIMES = []
_EXPIRY_TIMES = []


def _install_lifetime_cap(lifetime_ms):
    """让服务器的 OpenSecureChannel 响应把授予寿命压到 lifetime_ms（模拟网关）。"""
    global _ORIGINAL_OPEN
    if _ORIGINAL_OPEN is None:
        _ORIGINAL_OPEN = ua_connection.SecureConnection.open

    def patched(conn, params, server):
        result = _ORIGINAL_OPEN(conn, params, server)
        result.SecurityToken.RevisedLifetime = lifetime_ms
        if params.RequestType == ua.SecurityTokenRequestType.Renew:
            _RENEW_TIMES.append(time.monotonic())
        return result

    ua_connection.SecureConnection.open = patched


def _restore_lifetime_cap():
    if _ORIGINAL_OPEN is not None:
        ua_connection.SecureConnection.open = _ORIGINAL_OPEN


async def _enforce_token_expiry(server):
    """模拟真实网关：token 超过寿命仍未续约就断开 TCP，而不是无限容忍。"""
    while True:
        await asyncio.sleep(0.3)
        now = datetime.datetime.now(datetime.timezone.utc)
        for proto in list(getattr(server.bserver, "clients", [])):
            try:
                conn = proto.processor._connection
                if not conn.is_open():
                    continue
                token = conn.security_token
                deadline = token.CreatedAt + datetime.timedelta(
                    milliseconds=token.RevisedLifetime
                )
                if now > deadline:
                    _EXPIRY_TIMES.append(time.monotonic())
                    proto.transport.close()
            except Exception:
                continue


@unittest.skipUnless(ASYNCUA_AVAILABLE, "asyncua 不可用")
class TestRenewalAgainstShortLifetimeGateway(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.port = _free_port()
        self.tmpdir = os.path.join(ROOT, "tests", ".renewal_test_tmp")
        os.makedirs(self.tmpdir, exist_ok=True)
        # 隔离客户端证书与 server_cert_0.der 缓存：避免测试覆盖仓库根目录下现场正在用的证书
        import opc.client_engine as _ce
        self._app_path_patch = patch.object(_ce, "get_app_path", return_value=self.tmpdir)
        self._app_path_patch.start()
        self.cert, self.key = _make_cert(os.path.join(self.tmpdir, f"cert_{self.port}"))
        _RENEW_TIMES.clear()
        _EXPIRY_TIMES.clear()
        _install_lifetime_cap(SHORT_LIFETIME_MS)

        self.server = Server()
        await self.server.init()
        self.server.set_endpoint(f"opc.tcp://127.0.0.1:{self.port}/")
        self.server.set_server_name("RenewalTestServer")
        self.server.set_security_policy(
            [ua.SecurityPolicyType.Basic256Sha256_SignAndEncrypt],
            permission_ruleset=SimpleRoleRuleset(),
        )
        await self.server.load_certificate(self.cert)
        await self.server.load_private_key(self.key)
        idx = await self.server.register_namespace("http://test.lighting")
        obj = await self.server.nodes.objects.add_object(idx, "Lights")
        for i in (1, 2):
            var = await obj.add_variable(f"ns={idx};s=Light_{i}", f"Light {i}", False)
            await var.set_writable()
        await self.server.__aenter__()
        self.enforcer = asyncio.create_task(_enforce_token_expiry(self.server))

        self.engine = OpcClientEngine(
            host="127.0.0.1", port=self.port, namespace_filter="ns=2;", browse_max_depth=2
        )
        self.transitions = []
        self.engine.on_state_change = lambda state, detail: self.transitions.append(state)

    async def asyncTearDown(self):
        try:
            self.enforcer.cancel()
            try:
                await self.enforcer
            except (asyncio.CancelledError, Exception):
                pass
            await self.engine.disconnect()
            await self.server.__aexit__(None, None, None)
        finally:
            _restore_lifetime_cap()
            if getattr(self, "_app_path_patch", None) is not None:
                self._app_path_patch.stop()

    async def test_no_disconnect_across_three_token_lifetimes(self):
        """6 秒寿命下连续跨 3 个周期：必须靠续约保持 connected，绝不能被断开。"""
        await self.engine.connect()
        self.engine.set_subscription_callback(lambda *args: None)
        await self.engine.bootstrap()
        await self.engine.reconnect_forever()

        self.assertEqual(self.engine.state, OpcClientEngine.STATE_CONNECTED)
        observe_s = SHORT_LIFETIME_MS / 1000.0 * 3 + 3.0
        deadline = time.monotonic() + observe_s
        while time.monotonic() < deadline:
            await asyncio.sleep(0.5)
            if not self.engine.connected:
                break

        self.assertTrue(
            self.engine.connected,
            f"链路在 {observe_s:.0f}s 内掉线；状态迁移={self.transitions}",
        )
        self.assertNotIn(
            OpcClientEngine.STATE_RECONNECTING, self.transitions,
            f"发生了非无感的重连（状态迁移={self.transitions}）",
        )
        self.assertEqual(
            _EXPIRY_TIMES, [],
            "服务端因 token 过期切断了连接 —— 客户端没有及时续约",
        )
        self.assertGreaterEqual(
            len(_RENEW_TIMES), 2,
            f"3 个寿命周期内只发起了 {len(_RENEW_TIMES)} 次续约，续约频率不足",
        )
        self.assertGreaterEqual(
            self.engine.renewal_count, 2,
            f"引擎记录的成功续约次数只有 {self.engine.renewal_count}",
        )


# --------------------------------------------------------------------------------------
# 3) DA 网关时间戳兼容：1601-01-01 / 1970 年以前的 SourceTimestamp 不得吞掉推送
# --------------------------------------------------------------------------------------
class _FakeNodeId:
    def to_string(self):
        return "ns=2;s=Light_1"


class _FakeNode:
    nodeid = _FakeNodeId()


class _FakeValue:
    def __init__(self, source_timestamp):
        self.SourceTimestamp = source_timestamp


class _FakeMonitoredItem:
    def __init__(self, source_timestamp):
        self.Value = _FakeValue(source_timestamp)


class _FakeData:
    def __init__(self, source_timestamp):
        self.monitored_item = _FakeMonitoredItem(source_timestamp)


class TestDatachangeTimestampCompatibility(unittest.TestCase):
    def test_pre_1970_source_timestamp_still_delivers_value(self):
        """DA 网关未填时标时 SourceTimestamp=1601-01-01；Windows 上 astimezone 会抛
        OSError [Errno 22]，旧实现会把每一次推送都丢掉（UI 实时值不刷新）。"""
        received = []
        handler = SubHandler(lambda node_id, value, ts: received.append((node_id, value, ts)))
        handler.datachange_notification(
            _FakeNode(), True,
            _FakeData(datetime.datetime(1601, 1, 1, tzinfo=datetime.timezone.utc)),
        )
        self.assertEqual(len(received), 1, "非法历史时间戳导致推送被丢弃")
        self.assertEqual(received[0][0], "ns=2;s=Light_1")
        self.assertTrue(received[0][2])

    def test_missing_source_timestamp_still_delivers(self):
        received = []
        handler = SubHandler(lambda node_id, value, ts: received.append(value))
        handler.datachange_notification(_FakeNode(), False, _FakeData(None))
        self.assertEqual(received, [False])


if __name__ == "__main__":
    unittest.main()
