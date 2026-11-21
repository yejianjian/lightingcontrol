"""OPC UA 通信引擎。

本模块是灯光控制系统与 OPC UA 服务器之间唯一的通信接缝（seam）：上层 UI 只与
``OpcClientEngine`` 的少数几个方法交互，而"连接保持"这一最复杂的行为——安全通道
续约、心跳看门狗、断线检测、指数退避自动重连、重连后节点与订阅的复原——全部封装
在引擎内部。

设计要点（重连稳定性）：
1. **单一状态机**：所有连接状态变更都经 ``_set_state`` 收敛，``connected`` 只是
   状态机的一个只读投影，避免多处竞态改写布尔标志位导致的 UI 状态错乱。
2. **引擎自治重连**：断线后的重试节奏由引擎自己的 supervisor 协程掌握。UI 不再
   自己 sleep 重试（那样会在节点遍历/订阅阶段卡死且无法被取消）。
3. **世代号（generation）**：每个 Client 实例关联一个单调递增的世代号，旧世代的
   回调、看门狗、续约任务一律被忽略，杜绝"僵尸连接"污染新连接。
4. **一切网络操作都有硬超时**：connect / 节点遍历 / 订阅建立 / 续约 / 心跳 /
   断开，全都有界，任何一步挂起都不会永久阻塞状态机。
5. **健康判据基于"链路"而非"数据变化"**：灯光点位状态可能数小时不变（订阅不会
   推送 datachange），因此存活判定只使用主动读（server_state）与订阅底层发布任务
   状态，绝不把"没有数据推送"当作断线。
"""

import asyncio
import datetime
import ipaddress
import os
import random
import socket
import time

from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import NameOID

from asyncua import Client, ua
from asyncua.crypto import security_policies

from utils.logger import global_logger
from utils.paths import get_app_path


# --------------------------------------------------------------------------------------
# 常量与超时预算（单位：秒）
# --------------------------------------------------------------------------------------

# 单次请求在 asyncua 层面的应答超时，同时作为请求头 TimeoutHint
DEFAULT_CLIENT_TIMEOUT = 6.0
# 建立 Session 的超时
CONNECT_TIMEOUT = 30.0
# 节点树遍历的总超时（大点位表可能需要数十秒）
BROWSE_TIMEOUT = 180.0
# 建立订阅的超时
SUBSCRIBE_TIMEOUT = 120.0
# 单次节点值读取超时
READ_TIMEOUT = 8.0
# 向服务器核验/探测其证书的超时
DISCOVERY_TIMEOUT = 20.0
# 安全通道续约单次尝试超时
RENEW_ATTEMPT_TIMEOUT = 15.0
# 断开时优雅关闭 Session 的时间预算，超出则强杀 socket
GRACEFUL_CLOSE_TIMEOUT = 3.0
# 等待自研后台任务退出的时间预算
TASK_CANCEL_GRACE = 1.0

# Session 保活窗口（毫秒）。asyncua 内部看门狗间隔 = min(session_timeout/2, watchdog_interval)，
# 默认 1 小时的 session 会让断线检测滞后到 30 分钟，对 24/7 集控系统完全不可接受。
DEFAULT_SESSION_TIMEOUT_MS = 60_000
MIN_SESSION_TIMEOUT_MS = 10_000
MAX_SESSION_TIMEOUT_MS = 3_600_000
# 服务端未返回 RevisedLifetime 时的兜底安全通道寿命（毫秒）
DEFAULT_CHANNEL_LIFETIME_MS = 3_600_000
# 安全通道续约触发比例（寿命的百分比）：50% 时续约，留足重试与网络抖动窗口
CHANNEL_RENEW_FRACTION = 0.5
# asyncua 内部看门狗轮询间隔（秒）
WATCHDOG_INTERVAL = 10.0

# 自动重连退避参数（秒）
DEFAULT_RECONNECT_BASE_DELAY = 10.0
DEFAULT_RECONNECT_MAX_DELAY = 60.0
# 退避抖动比例，避免多台集控终端在同一时刻惊群式重连
RECONNECT_JITTER = 0.2

# 刷新缓存节点值时的并发度
REFRESH_CONCURRENCY = 20

DEFAULT_APPLICATION_URI = "urn:example.org:FreeOpcUa:opcua-asyncio"

SERVER_STATE_NODE_ID = "i=2259"
SERVER_TIME_NODE_ID = "i=2258"

# 运维可以手工放置的服务器证书文件名（首次连接时会与服务器实际证书核对）
CONFIGURED_SERVER_CERT_FILE = "server_cert_0.der"
_CACHED_SERVER_CERT_FILE = "server_cert_0.der"


class ConnectionSuperseded(Exception):
    """本次连接尝试已被更新的世代取代（用户主动断开或重新发起连接）。

    这是一个内部控制流异常：它表示"放弃当前这一轮连接"，而不是网络故障，
    因此不应该被当作断线上报，也不应该触发重连。
    """


def _utc_now() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _cert_validity(cert):
    """读取证书有效期，兼容 cryptography < 42（无 not_valid_before_utc）。

    老版本只提供 naive 的 not_valid_before / not_valid_after（按规范是 UTC），
    这里统一补齐时区，避免"证书已被判定过期 → 每次连接都重新生成证书 → 服务端
    信任列表失配"的稳定性事故。
    """
    before = getattr(cert, "not_valid_before_utc", None)
    after = getattr(cert, "not_valid_after_utc", None)
    if before is None:
        before = cert.not_valid_before.replace(tzinfo=datetime.timezone.utc)
    if after is None:
        after = cert.not_valid_after.replace(tzinfo=datetime.timezone.utc)
    return before, after


def _cert_is_current(cert, now_utc: datetime.datetime) -> bool:
    try:
        before, after = _cert_validity(cert)
    except Exception:
        return False
    return before <= now_utc <= after


def generate_client_cert(cert_file="client_cert.der", key_file="client_key.pem"):
    """生成（或复用）客户端证书。

    重要：只要已有证书在有效期内就必须复用。服务端通常把本客户端证书加入了信任
    列表，一旦重新生成就会导致握手被拒（BadSecurityChecksFailed）。
    证书必须写在**可执行文件旁的可写目录**，不能写进 PyInstaller 的临时解压目录。
    """
    base_dir = get_app_path()
    if not os.path.isabs(cert_file):
        cert_file = os.path.join(base_dir, cert_file)
    if not os.path.isabs(key_file):
        key_file = os.path.join(base_dir, key_file)

    hostname = socket.gethostname()
    now_utc = _utc_now()
    if os.path.exists(cert_file) and os.path.exists(key_file):
        try:
            with open(cert_file, "rb") as f:
                existing_cert = x509.load_der_x509_certificate(f.read())
            if _cert_is_current(existing_cert, now_utc):
                return cert_file, key_file, hostname
            global_logger.warning(
                "Existing client cert is invalid for current system time. "
                "Regenerating with wide validity range..."
            )
        except Exception as e:
            global_logger.warning(f"Existing client cert could not be read ({e}); regenerating.")

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = issuer = x509.Name([
        x509.NameAttribute(NameOID.DOMAIN_COMPONENT, hostname),
        x509.NameAttribute(NameOID.COMMON_NAME, u"LightingControlClient"),
    ])
    # 宽泛有效期：2000-01-01 到 2099-12-31，免疫用户调整操作系统日期/时钟回拨造成的
    # "证书尚未生效"或"证书已过期"错误
    cert = x509.CertificateBuilder().subject_name(
        subject
    ).issuer_name(
        issuer
    ).public_key(
        key.public_key()
    ).serial_number(
        x509.random_serial_number()
    ).not_valid_before(
        datetime.datetime(2000, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    ).not_valid_after(
        datetime.datetime(2099, 12, 31, 23, 59, 59, tzinfo=datetime.timezone.utc)
    ).add_extension(
        x509.SubjectAlternativeName([
            x509.UniformResourceIdentifier(DEFAULT_APPLICATION_URI),
            x509.DNSName(hostname),
            x509.DNSName("localhost"),
            x509.IPAddress(ipaddress.IPv4Address("127.0.0.1"))
        ]), critical=False,
    ).add_extension(
        x509.KeyUsage(
            digital_signature=True, content_commitment=True, key_encipherment=True,
            data_encipherment=True, key_agreement=False, key_cert_sign=False,
            crl_sign=False, encipher_only=False, decipher_only=False
        ), critical=True,
    ).add_extension(
        x509.ExtendedKeyUsage([
            x509.oid.ExtendedKeyUsageOID.CLIENT_AUTH,
            x509.oid.ExtendedKeyUsageOID.SERVER_AUTH,
        ]), critical=True,
    ).sign(key, hashes.SHA256())

    # 先写临时文件再替换，避免断电留下半截文件
    for path, data in (
        (key_file, key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=serialization.NoEncryption(),
        )),
        (cert_file, cert.public_bytes(serialization.Encoding.DER)),
    ):
        tmp_path = path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(data)
        os.replace(tmp_path, path)

    global_logger.info("Generated a new OPC UA client certificate (wide validity 2000-2099).")
    return cert_file, key_file, hostname


def _load_configured_server_cert_bytes():
    """读取磁盘上的服务器证书缓存（若存在且当前系统时间下有效），否则返回 None。

    这只是"运维预先放好证书"的兼容路径：第一次连接时会与服务器实际出示的证书做比对，
    发现不一致就隔离旧文件并改用实际证书。
    """
    path = os.path.join(get_app_path(), CONFIGURED_SERVER_CERT_FILE)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "rb") as f:
            raw = f.read()
        cert = x509.load_der_x509_certificate(raw)
    except Exception as e:
        global_logger.warning(f"{CONFIGURED_SERVER_CERT_FILE} unreadable ({e}); will discover from server.")
        return None
    if not _cert_is_current(cert, _utc_now()):
        global_logger.warning(
            f"{CONFIGURED_SERVER_CERT_FILE} is outside current system time range; "
            f"will discover the live certificate from the server."
        )
        return None
    return raw


async def _discover_server_cert_bytes(url, timeout=DEFAULT_CLIENT_TIMEOUT):
    """以 SecurityPolicyNone 连接一次服务器的 GetEndpoints，取回其真实证书。

    为什么必须做这一步：Basic256Sha256 下客户端用服务器证书加密 OpenSecureChannel
    请求。如果本地缓存的证书与服务器当前证书不一致（服务器重装/还原/重新自签），
    服务端 RSA 解密必然失败并**直接丢弃连接**，客户端只能看到请求超时 —— 而且每次
    重连都会重蹈覆辙，表现为"永远连不上"。主动核验一次即可根除这类事故。
    """
    probe = Client(url=url, timeout=timeout, watchdog_intervall=WATCHDOG_INTERVAL)
    try:
        endpoints = await asyncio.wait_for(
            probe.connect_and_get_server_endpoints(), timeout=DISCOVERY_TIMEOUT
        )
    finally:
        try:
            probe.disconnect_socket()
        except Exception:
            pass

    endpoint = None
    for ep in endpoints:
        try:
            if ep.SecurityPolicyUri.endswith("Basic256Sha256") and \
                    ep.SecurityMode in (ua.MessageSecurityMode.Sign, ua.MessageSecurityMode.SignAndEncrypt):
                endpoint = ep
                break
        except Exception:
            continue
    if endpoint is None:
        for ep in endpoints:
            if getattr(ep, "ServerCertificate", None):
                endpoint = ep
                break
    if endpoint is None or not getattr(endpoint, "ServerCertificate", None):
        raise ConnectionError("服务器未在 GetEndpoints 中提供证书（Basic256Sha256 无法建立）")
    # 服务器证书链可能带上级证书，asyncua 只认第一张，这里按 DER 长度截取
    raw = endpoint.ServerCertificate
    try:
        cert_len = int.from_bytes(raw[2:4], byteorder="big", signed=False) + 4
        raw = raw[:cert_len]
    except Exception:
        pass
    return raw, endpoint


def _quarantine_stale_server_cert(reason):
    """把已失效的服务器证书缓存改名隔离。

    这是运维可读性的兜底：如果磁盘上的 ``server_cert_0.der`` 与服务器实际证书不一致，
    就把旧文件留下 ``.stale`` 副本以便人工比对（谁换了证书、什么时候换的）。
    """
    path = os.path.join(get_app_path(), _CACHED_SERVER_CERT_FILE)
    if not os.path.exists(path):
        return
    stale_path = path + ".stale"
    try:
        os.replace(path, stale_path)
        global_logger.error(
            f"Live server certificate differs from the cached one ({reason}); the old copy is kept as "
            f"{os.path.basename(stale_path)}. Please confirm the server certificate really was rotated."
        )
    except Exception as e:
        global_logger.warning(f"Failed to quarantine stale server certificate cache: {e}")


def _write_server_cert_cache(cert_der: bytes):
    """把已核验过的服务器证书写回磁盘（原子写入，失败不影响连接）。"""
    path = os.path.join(get_app_path(), _CACHED_SERVER_CERT_FILE)
    tmp_path = path + ".tmp"
    try:
        with open(tmp_path, "wb") as f:
            f.write(cert_der)
        os.replace(tmp_path, path)
    except Exception as e:
        global_logger.debug(f"Could not persist server certificate cache: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


def _same_certificate(a: bytes, b: bytes) -> bool:
    try:
        return x509.load_der_x509_certificate(a) == x509.load_der_x509_certificate(b)
    except Exception:
        return a == b


def _looks_like_certificate_rejection(err) -> bool:
    """判断连接失败是否可能由"服务器证书与本地缓存不一致"引起。

    服务器证书不匹配时，客户端与服务端的行为是：
    * 客户端：CreateSession 响应的 ServerCertificate 与本地不一致 →
      UaError("Server certificate mismatch")；
    * 或者更早：服务端 RSA 解密 OpenSecureChannel 失败并直接丢连接 → 客户端侧只能看到
      "Failed to send request ..."、TimeoutError 或 ConnectionError。
    因此这里把这几类都视为"需要重新核验服务器证书"的信号。
    """
    text = str(err).lower()
    for marker in (
        "certificate mismatch",
        "certificate has changed",
        "decryption failed",
        "failed to send request",
    ):
        if marker in text:
            return True
    return isinstance(err, (asyncio.TimeoutError, ConnectionError))


def _is_bad_server_uri(err):
    """判断异常是否为 BadServerUriInvalid（ServerUri 与服务器自报不一致）。"""
    code = getattr(err, "code", None)
    target = getattr(ua.StatusCodes, "BadServerUriInvalid", None)
    if code is not None and target is not None:
        try:
            if int(code) == int(target):
                return True
        except (TypeError, ValueError):
            pass
    return "ServerUri" in str(err)


# OPC UA Variant 类型映射表（避免 asyncua 版本间 VariantType 名称差异）
_TYPE_MAP = {
    "Boolean": ("Boolean", lambda v: bool(v)),
    "Int16":   ("Int16",   lambda v: int(v)),
    "Int32":   ("Int32",   lambda v: int(v)),
    "Int64":   ("Int64",   lambda v: int(v)),
    "UInt16":  ("UInt16",  lambda v: int(v)),
    "UInt32":  ("UInt32",  lambda v: int(v)),
    "UInt64":  ("UInt64",  lambda v: int(v)),
    "Float":   ("Float",   lambda v: float(v)),
    "Double":  ("Double",  lambda v: float(v)),
    "Real":    ("Float",   lambda v: float(v)),   # 别名
    "String":  ("String",  lambda v: str(v)),
}


class SubHandler:
    """订阅数据变更回调。

    注意：本处理器只做"观测"，不参与连接健康判定。灯光点位状态可能长时间不变，
    服务器不会推送 datachange，把"多久没收到推送"当作断线判据会造成大面积误重连。
    """

    def __init__(self, callback):
        self.callback = callback
        self.last_data_time = time.monotonic()  # 最后收到数据的时间（仅用于诊断展示）
        self.call_count = 0                     # 总回调计数

    def datachange_notification(self, node, val, data):
        try:
            now = time.monotonic()
            self.last_data_time = now
            self.call_count += 1

            timestamp = None
            if hasattr(data, 'monitored_item') and hasattr(data.monitored_item, 'Value'):
                timestamp = data.monitored_item.Value.SourceTimestamp

            if timestamp:
                # OPC UA 规范返回的是 UTC 时间，需要赋予 UTC 时区后转为系统本地时区。
                # 注意：DA 网关常把没有时标的点位 SourceTimestamp 填成 1601-01-01
                # （FILETIME 0），而 Windows 上 datetime.astimezone() 对 1970 年以前
                # 的时间会抛 OSError [Errno 22]；旧实现会让本该送往界面的每一次推送
                # 都在这里抛异常并被丢弃（现场 app.log 每秒刷一条
                # "datachange_notification failed: [Errno 22] Invalid argument"）。
                # 时间戳只是展示用，转换失败时退回本地当前时间，数据本身照常上报。
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)
                try:
                    ts_str = timestamp.astimezone().strftime("%Y-%m-%d %H:%M:%S")
                except (OSError, OverflowError, ValueError):
                    ts_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                ts_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            self.callback(node.nodeid.to_string(), val, ts_str)
        except Exception as e:
            global_logger.error(f"datachange_notification failed: {e}", exc_info=True)

    def status_change_notification(self, status):
        """处理服务器状态改变通知（如 BadShutdown 等）"""
        global_logger.warning(f"Subscription received server status change: {status}")

    def event_notification(self, event):
        """处理事件通知"""
        pass


class OpcClientEngine:
    """OPC UA 客户端引擎（含自治重连）。

    Interface（调用方需要知道的全部）::

        engine.configure(host, port, username, password, namespace_filter)
        engine.on_state_change = fn(state: str, detail: dict)   # UI 只读状态投影
        engine.on_nodes_recovered = fn()                        # 重连后刷新 UI 计数
        engine.set_subscription_callback(fn)                    # 数据推送回调（重连自动复用）
        await engine.connect()           # 建立连接（含安全通道/Session）
        await engine.bootstrap()         # 加载节点 + 建立订阅（失败 = 连接不可用）
        await engine.reconnect_forever() # 打开自治重连总闸
        await engine.disconnect()        # 幂等、有界、绝不挂死
        engine.connected                 # 状态机投影

    不变式：
    * ``connected`` 为真 ⟺ 状态为 ``CONNECTED``；
    * 任一时刻最多一个重连 supervisor 存活；
    * 有了世代号之后，旧连接的回调不再影响新连接；
    * ``disconnect()`` 返回后，所有由本引擎创建的 asyncio 任务都已取消/收尾。
    """

    # 状态常量
    STATE_DISCONNECTED = "disconnected"    # 未连接（初始态或用户主动断开）
    STATE_CONNECTING = "connecting"        # 正在建立连接
    STATE_LOADING = "loading"              # 已连上，正在加载节点/建立订阅
    STATE_CONNECTED = "connected"          # 正常工作
    STATE_RECONNECTING = "reconnecting"    # 断线后自动重连中
    STATE_DISCONNECTING = "disconnecting"  # 正在断开

    def __init__(self, host="localhost", port=48401, username="", password="",
                 namespace_filter="ns=2;", browse_max_depth=5):
        self.host = host
        self.port = port
        self.url = f"opc.tcp://{host}:{port}/"
        self.username = username
        self.password = password
        self.namespace_filter = namespace_filter  # 业务节点命名空间前缀，可配置
        self.browse_max_depth = browse_max_depth  # 节点树遍历最大深度

        self.client = None
        self.nodes = {}
        self.subscription = None

        self._state = self.STATE_DISCONNECTED
        self._generation = 0            # 每建立一个 Client 实例 +1
        # 锁在首次使用时惰性创建：Python 3.8 的 asyncio.Lock() 在"当前线程没有事件循环"
        # 时会直接抛 RuntimeError，而 UI 会在事件循环启动前就构造引擎。惰性创建同时保证
        # 锁总是绑定到真正在跑的那个循环上（qasync 测试里可能换了循环）。
        self._client_lock = None
        self._disconnect_lock = None

        self._sub_handler = None        # SubHandler 引用，用于诊断
        self._sub_callback = None       # 订阅回调引用，用于自动重建
        self._subscribed_nodes = []     # 本世代已订阅的 node_id 列表

        self._monitor_task = None
        self._renew_task = None
        self._reconnect_task = None
        # 排队中、尚未执行的 reconnect_forever 协程（UI 在事件循环之外 create_task 出来的），
        # 用户停止自动重连时必须一并作废，否则它会晚一步把总闸重新打开。
        self._pending_reconnect_forever = None

        # 已核验的服务器证书（DER）。进程内缓存：进程的第一个连接负责与服务器实际
        # 出示的证书核验一次，之后所有重连直接复用，不再额外往返。
        self._server_cert_der = None
        self._server_cert_verified = False

        self._channel_connected_at = 0.0  # 安全通道初次建联单调时间戳
        self._last_renewed_at = 0.0       # 安全通道上次成功续约单调时间戳
        self._last_data_at = 0.0          # 最后收到订阅推送的时间（诊断用）
        self.renewal_count = 0            # 累计成功续约次数（诊断/自动化测试断言用）
        self.last_renewal_monotonic = 0.0 # 最近一次续约成功的单调时间戳（诊断用）
        # 后台任务"创建/真正开始执行"时间戳：qasync 下 Qt 的 processEvents() 重入
        # 可能让刚 create_task 的协程一步都没跑就永久 pending（僵尸任务），必须能识别。
        self._renew_task_created_at = 0.0
        self._renew_started_at = 0.0
        self._monitor_task_created_at = 0.0
        self._monitor_started_at = 0.0

        self._consecutive_write_failures = 0
        self._max_write_failures = 3      # 连续失败阈值，超过触发重连

        # 重连策略（可由设置界面覆盖）
        self.session_timeout_ms = DEFAULT_SESSION_TIMEOUT_MS
        self.reconnect_base_delay = DEFAULT_RECONNECT_BASE_DELAY
        self.reconnect_max_delay = DEFAULT_RECONNECT_MAX_DELAY
        self._auto_reconnect = False
        self.reconnect_attempt = 0
        self.total_reconnects = 0

        # 回调钩子
        self.on_connection_lost = None      # 兼容旧接口：断线时调用一次
        self.on_state_change = None         # fn(state, detail)
        self.on_nodes_recovered = None      # 重连后节点值刷新完成

    # ----------------------------------------------------------------------------------
    # 状态机
    # ----------------------------------------------------------------------------------

    @property
    def state(self):
        return self._state

    @property
    def connected(self):
        return self._state == self.STATE_CONNECTED

    @connected.setter
    def connected(self, value):
        """兼容旧代码：``engine.connected = False`` 等价于把状态机打到断开态。"""
        if value:
            self._state = self.STATE_CONNECTED
        elif self._state == self.STATE_CONNECTED:
            self._state = self.STATE_DISCONNECTED

    def _set_state(self, new_state, reason="", **detail):
        if self._state == new_state:
            return
        old = self._state
        self._state = new_state
        payload = {"previous": old, "reason": reason}
        payload.update(detail)
        global_logger.info(f"[STATE] {old} -> {new_state} ({reason or 'n/a'})")
        cb = self.on_state_change
        if callable(cb):
            try:
                cb(new_state, payload)
            except Exception as e:
                global_logger.error(f"on_state_change callback error: {e}", exc_info=True)

    def _status_detail(self):
        return {
            "attempt": self.reconnect_attempt,
            "total_reconnects": self.total_reconnects,
            "host": self.host,
            "port": self.port,
        }

    def _ensure_locks(self):
        """惰性创建并校验 asyncio 锁（见 __init__ 中关于 Python 3.8 的说明）。"""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()
        if self._disconnect_lock is None:
            self._disconnect_lock = asyncio.Lock()
        # asyncio 原语一旦绑定到某个事件循环就不能跨循环复用
        for name in ("_client_lock", "_disconnect_lock"):
            lock = getattr(self, name)
            bound = getattr(lock, "_loop", None)
            if running is not None and bound is not None and bound is not running:
                global_logger.warning(
                    f"{name} was bound to a different event loop; recreating it for the current loop."
                )
                setattr(self, name, asyncio.Lock())

    # ----------------------------------------------------------------------------------
    # 连接
    # ----------------------------------------------------------------------------------

    def configure(self, host=None, port=None, username=None, password=None,
                  namespace_filter=None, browse_max_depth=None):
        """更新连接参数。改变目标服务器会使缓存的节点对象与已核验证书失效。"""
        endpoint_changed = False
        if host is not None and host != self.host:
            self.host = host
            endpoint_changed = True
        if port is not None and port != self.port:
            self.port = port
            endpoint_changed = True
        if username is not None:
            self.username = username
        if password is not None:
            self.password = password
        if namespace_filter is not None:
            self.namespace_filter = namespace_filter
        if browse_max_depth is not None:
            self.browse_max_depth = browse_max_depth
        self.url = f"opc.tcp://{self.host}:{self.port}/"
        if endpoint_changed:
            global_logger.info(f"OPC endpoint changed to {self.url}; cached node objects invalidated.")
            self.nodes = {}
            self._subscribed_nodes = []
            self._server_cert_der = None
            self._server_cert_verified = False

    def set_subscription_callback(self, callback):
        """登记数据推送回调。重连后引擎会自动用它重建订阅。"""
        self._sub_callback = callback

    async def connect(self):
        """建立连接（安全通道 + Session）。

        并发调用会被串行化；比新一次调用更旧的调用会抛 ``ConnectionSuperseded``。
        """
        self._ensure_locks()
        async with self._client_lock:
            my_gen = self._generation + 1
            self._generation = my_gen
            # 先让正在运行的重连 supervisor 停下来，避免它与本次连接抢 Client。
            # 注意这里只取消任务、不改动 _auto_reconnect 总闸。
            await self._stop_reconnect_supervisor()
            await self._cleanup_client()
            await self._connect_once(my_gen)
            return True

    async def _resolve_server_certificate(self):
        """准备好本次连接要信任的服务器证书（返回 DER 字节）。

        策略：
        1. 进程内已有核验过的证书 → 直接用（重连走这条路，零额外往返）；
        2. 否则读取运维放在根目录的 ``server_cert_0.der``；
        3. 与服务器实际出示的证书逐个字节比对：
           * 一致 → 记入进程内缓存；
           * 不一致 → 隔离旧文件（保留 ``.stale`` 供人工比对）、记录 ERROR 并在内存中
             改用实际证书；
           * 本地没有证书 → 直接用实际证书并写回磁盘缓存。
        """
        if self._server_cert_der is not None:
            return self._server_cert_der

        configured = _load_configured_server_cert_bytes()
        live, endpoint = await _discover_server_cert_bytes(self.url)

        if configured is not None and not _same_certificate(configured, live):
            _quarantine_stale_server_cert("server is presenting a different certificate")
            global_logger.error(
                f"Server at {self.url} presented a certificate that differs from "
                f"{CONFIGURED_SERVER_CERT_FILE}. Using the live certificate for this session."
            )
        elif configured is None:
            global_logger.info(
                f"Discovered and cached the server certificate via GetEndpoints "
                f"(EndpointUrl={getattr(endpoint, 'EndpointUrl', 'n/a')})."
            )

        _write_server_cert_cache(live)
        self._server_cert_der = live
        self._server_cert_verified = True
        return live

    async def _connect_once(self, my_gen):
        if my_gen != self._generation:
            raise ConnectionSuperseded(
                f"connection attempt superseded (gen {my_gen} != {self._generation})"
            )

        self._set_state(self.STATE_CONNECTING, "connect requested", **self._status_detail())

        cert_file, key_file, hostname = generate_client_cert()

        server_cert_der = await self._resolve_server_certificate()
        if my_gen != self._generation:
            raise ConnectionSuperseded("connect superseded while resolving the server certificate")

        client = Client(url=self.url, timeout=DEFAULT_CLIENT_TIMEOUT, watchdog_intervall=WATCHDOG_INTERVAL)
        client.application_uri = DEFAULT_APPLICATION_URI
        client.secure_channel_timeout = 3_600_000
        client.session_timeout = self._clamped_session_timeout()
        client.connection_lost_callback = (
            lambda exc, gen=my_gen: self._on_asyncua_connection_lost(exc, gen)
        )

        if self.username:
            client.set_user(self.username)
            client.set_password(self.password)

        # 统一走 set_security，并把已核验的服务器证书直接传进去，避免 asyncua 内部
        # 再走一遍"探测证书"的隐式路径（那条路径会额外建一次连接）。
        try:
            await client.set_security(
                security_policies.SecurityPolicyBasic256Sha256,
                cert_file,
                key_file,
                None,
                server_cert_der,
                ua.MessageSecurityMode.SignAndEncrypt,
            )
        except Exception as e:
            raise ConnectionError(f"配置安全策略失败（请检查客户端/服务器证书）: {e}") from e

        self._install_session_hook(client, my_gen)
        self.client = client

        try:
            await asyncio.wait_for(client.connect(), timeout=CONNECT_TIMEOUT)
        except asyncio.CancelledError:
            await self._cleanup_client()
            raise
        except Exception as e:
            await self._cleanup_client()
            # 服务器证书可能在我们离线期间被轮换，导致本次用错公钥（服务端 RSA 解密
            # 失败 → 直接丢连接 → 客户端只看到超时）。丢弃内存中已"核验过"的证书，
            # 下一轮 connect() 会重新向服务器核验。
            if self._server_cert_verified and _looks_like_certificate_rejection(e):
                global_logger.error(
                    f"Connection failed in a way that indicates the server certificate changed: {e!r}. "
                    f"Dropping the cached server certificate so the next attempt re-discovers it."
                )
                self._server_cert_der = None
                self._server_cert_verified = False
                raise ConnectionError(
                    f"{e} (the server certificate may have been rotated; "
                    f"the next reconnect will verify it again)"
                ) from e
            raise

        if my_gen != self._generation:
            # 在连接过程中被更新的一次调用/用户断开取代
            await self._cleanup_client()
            raise ConnectionSuperseded(f"connection attempt superseded during connect (gen {my_gen})")

        my_client = self.client
        if my_client is not client:
            raise ConnectionSuperseded(f"client replaced during connect (gen {my_gen})")

        self._channel_connected_at = time.monotonic()
        self._last_renewed_at = time.monotonic()
        self._last_data_at = time.monotonic()
        self._consecutive_write_failures = 0

        global_logger.info(
            f"Connected to {self.url}. SecureChannel timeout={my_client.secure_channel_timeout}ms, "
            f"Session timeout={my_client.session_timeout}ms"
        )

        # 先把状态推进到 loading，再启动后台任务：续约/看门狗协程的存活判据是
        # "链路可用"，必须能看到 LOADING 状态，否则任务会在第一次调度时立即退出。
        self._set_state(self.STATE_LOADING, "transport established", **self._status_detail())
        # 接管安全通道续约：取消 asyncua 内部脆弱任务，启动自研续约监督
        self._takeover_channel_renewal(my_gen)
        # 启动健康看门狗
        self._start_monitor(my_gen)
        # 关键：主动让出一轮事件循环，让刚创建的两个协程跑到第一行再返回 UI。
        # 现场 Qt 的 QProgressDialog.setValue() 会在 connect 流程尚未返回时重入
        # processEvents()，此时调度这些任务会触发 asyncio 的 "Cannot enter into
        # task ... while another task ... is being executed"，任务的第一步丢失后
        # 永久 pending（僵尸）——首次连接整小时既不续约也不心跳，直接导致整点断线。
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    def _clamped_session_timeout(self):
        try:
            value = int(self.session_timeout_ms)
        except (TypeError, ValueError):
            value = DEFAULT_SESSION_TIMEOUT_MS
        return max(MIN_SESSION_TIMEOUT_MS, min(MAX_SESSION_TIMEOUT_MS, value))

    def _install_session_hook(self, client, my_gen):
        """安装 Session 创建的容错钩子。

        neuopc 等网关常返回与请求不一致的 ServerUri，asyncua 的严格校验会直接让
        connect 失败。这里在失败后回读服务器自报的 URI 并重试一次；若仍失败则原样
        抛出首个异常，绝不吞掉真实错误。
        """
        uaclient = client.uaclient
        original = uaclient.create_session

        async def custom_create_session(params):
            if my_gen != self._generation:
                raise ConnectionSuperseded("connect superseded before CreateSession")
            try:
                return await original(params)
            except ua.UaStatusCodeError as first_err:
                if not _is_bad_server_uri(first_err):
                    raise
                endpoint_uri = getattr(client, "_endpoint_server_uri", None)
                if not endpoint_uri or endpoint_uri == params.ServerUri:
                    raise
                global_logger.warning(
                    f"Server rejected ServerUri={params.ServerUri!r} (BadServerUriInvalid); "
                    f"retrying with server-reported URI {endpoint_uri!r}."
                )
                params.ServerUri = endpoint_uri
                return await original(params)

        uaclient.create_session = custom_create_session

    # ----------------------------------------------------------------------------------
    # 节点加载与订阅
    # ----------------------------------------------------------------------------------

    async def bootstrap(self):
        """连接后的装配阶段：确保有点位表，然后建立订阅。

        重连时优先复用已有点位表（点位 ID 不会因为短暂断线而改变），避免每次重连都
        重新遍历上千个节点 —— 那既慢（可能几十秒）又极易被网络抖动打断。
        """
        if not self.connected and self._state != self.STATE_LOADING:
            raise ConnectionError("bootstrap() requires an established connection")

        self._set_state(self.STATE_LOADING, "loading nodes", **self._status_detail())

        if not self.nodes:
            await self.get_all_nodes()

        if self._sub_callback is not None:
            await self.start_subscription(self._sub_callback)

        await self._refresh_node_values()

        self._set_state(self.STATE_CONNECTED, "bootstrap complete", **self._status_detail())
        # 装配完成后再次确认续约/心跳任务仍在运行（防止长耗时 bootstrap 期间任务静默退出）
        await self._ensure_link_tasks(self._generation)
        cb = self.on_nodes_recovered
        if callable(cb):
            try:
                cb()
            except Exception as e:
                global_logger.error(f"on_nodes_recovered callback error: {e}", exc_info=True)

    async def get_all_nodes(self):
        """遍历节点树并缓存结果。返回剥离了 asyncua 引用的纯数据副本。"""
        if not self.connected and self._state != self.STATE_LOADING:
            return []

        my_gen = self._generation
        objects_node = self.client.nodes.objects
        discovered_nodes = []
        try:
            await asyncio.wait_for(
                self._browse_recursive(objects_node, discovered_nodes, self.browse_max_depth, my_gen),
                timeout=BROWSE_TIMEOUT,
            )
        except asyncio.TimeoutError:
            raise ConnectionError(f"节点遍历超时（>{BROWSE_TIMEOUT:.0f}s），已放弃本次加载")

        if my_gen != self._generation:
            raise ConnectionSuperseded("node browse superseded by a newer connection")

        self.nodes = {n['node_id']: n for n in discovered_nodes}
        return [{k: v for k, v in n.items() if k != 'node_obj'} for n in discovered_nodes]

    async def _browse_recursive(self, node, discovered_nodes, depth_remaining, my_gen):
        """递归遍历 OPC UA 节点树，收集业务变量节点。

        每一层都会检查世代号：用户点断开或发起新连接时，正在进行的遍历必须立刻退出，
        而不是继续用旧 Client 打服务器。
        """
        if depth_remaining <= 0:
            return
        if my_gen != self._generation:
            raise ConnectionSuperseded("browse superseded")
        try:
            children = await node.get_children()
        except ConnectionSuperseded:
            raise
        except Exception as e:
            global_logger.warning(f"Error browsing node {node}: {e}")
            return
        try:
            for child in children:
                if my_gen != self._generation:
                    raise ConnectionSuperseded("browse superseded")
                try:
                    node_class = await child.read_node_class()
                    if node_class == ua.NodeClass.Variable:
                        node_idx = child.nodeid.to_string()

                        # 仅关注业务控制命名空间的节点，过滤系统节点
                        if self.namespace_filter and not node_idx.startswith(self.namespace_filter):
                            continue

                        name = (await child.read_display_name()).Text
                        try:
                            val_obj = await child.read_data_value()
                            val = val_obj.Value.Value if val_obj.Value else None
                            timestamp = val_obj.SourceTimestamp.strftime("%Y-%m-%d %H:%M:%S") if val_obj.SourceTimestamp else ""
                            vtype = val_obj.Value.VariantType.name if val_obj.Value else None
                        except Exception:
                            val = None
                            timestamp = ""
                            vtype = None

                        dtype = vtype if vtype else (type(val).__name__ if val is not None else "Unknown")

                        if name != "ServerStatus":
                            discovered_nodes.append({
                                "name": name,
                                "node_id": node_idx,
                                "type": dtype,
                                "value": val,
                                "timestamp": timestamp,
                                "node_obj": child
                            })
                    elif node_class == ua.NodeClass.Object:
                        await self._browse_recursive(child, discovered_nodes, depth_remaining - 1, my_gen)
                except ConnectionSuperseded:
                    raise
                except Exception as child_err:
                    global_logger.warning(f"Error reading child {child}: {child_err}")
        except ConnectionSuperseded:
            raise
        except Exception as e:
            global_logger.warning(f"Error browsing node {node}: {e}")
        if my_gen != self._generation:
            raise ConnectionSuperseded("browse superseded after iterating children")

    async def _refresh_node_values(self):
        """把缓存的节点对象重绑到当前 Client，并刷新一次真实值。

        重连后一次性补齐断线期间的状态变化，同时顺带验证新通道的读能力。
        """
        if not self.nodes:
            return
        my_gen = self._generation
        client = self.client
        semaphore = asyncio.Semaphore(REFRESH_CONCURRENCY)
        refreshed = 0

        async def _refresh_one(node_id, cached):
            nonlocal refreshed
            if my_gen != self._generation:
                return
            async with semaphore:
                try:
                    node_obj = client.get_node(node_id)
                    val_obj = await asyncio.wait_for(node_obj.read_data_value(), timeout=READ_TIMEOUT)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    global_logger.debug(f"Post-reconnect refresh failed for {node_id}: {e}")
                    return
                val = val_obj.Value.Value if val_obj.Value else None
                timestamp = val_obj.SourceTimestamp.strftime("%Y-%m-%d %H:%M:%S") if val_obj.SourceTimestamp else ""
                cached['node_obj'] = node_obj
                cached['value'] = val
                cached['timestamp'] = timestamp
                if val_obj.Value is not None:
                    cached['type'] = val_obj.Value.VariantType.name
                refreshed += 1

        await asyncio.gather(*[
            _refresh_one(node_id, cached) for node_id, cached in list(self.nodes.items())
        ], return_exceptions=True)

        global_logger.info(
            f"Post-connect node refresh complete: {refreshed}/{len(self.nodes)} values re-read."
        )

    async def start_subscription(self, callback):
        """建立数据变更订阅。失败会抛异常，由调用方决定是重连还是上报。"""
        if not self.nodes:
            global_logger.warning("start_subscription skipped: node list is empty.")
            return
        if not self.connected and self._state != self.STATE_LOADING:
            raise ConnectionError("start_subscription() requires an established connection")

        self._sub_callback = callback
        my_gen = self._generation
        client = self.client

        node_objs = []
        for node_id, cached in self.nodes.items():
            node_obj = cached.get('node_obj')
            if node_obj is None:
                try:
                    node_obj = client.get_node(node_id)
                except Exception:
                    continue
                cached['node_obj'] = node_obj
            node_objs.append(node_obj)
        self._subscribed_nodes = [n['node_id'] for n in self.nodes.values()]

        # 把标准的心跳时间节点 i=2258 (CurrentTime) 也纳入订阅，便于诊断观察推送链路
        try:
            node_objs.append(client.get_node(SERVER_TIME_NODE_ID))
        except Exception as e:
            global_logger.warning(f"Could not add server current time to subscription: {e}")

        handler = SubHandler(callback)
        self._sub_handler = handler

        async def _create_and_subscribe():
            self.subscription = await client.create_subscription(500, handler)
            batch_size = 300
            failed_batches = 0
            for i in range(0, len(node_objs), batch_size):
                if my_gen != self._generation:
                    raise ConnectionSuperseded("subscription superseded")
                batch = node_objs[i:i + batch_size]
                try:
                    await self.subscription.subscribe_data_change(batch)
                    global_logger.info(
                        f"Successfully subscribed data changes for a batch of {len(batch)} nodes."
                    )
                except Exception as sub_e:
                    failed_batches += 1
                    global_logger.error(f"Batch subscribe failed: {sub_e}")
            if failed_batches and failed_batches * batch_size >= len(node_objs):
                raise ConnectionError(f"all {failed_batches} subscribe batches failed")

        await asyncio.wait_for(_create_and_subscribe(), timeout=SUBSCRIBE_TIMEOUT)
        global_logger.info(
            f"Subscription established for {len(node_objs)} monitored items (generation {my_gen})."
        )

    # ----------------------------------------------------------------------------------
    # 健康看门狗
    # ----------------------------------------------------------------------------------

    def _start_monitor(self, my_gen):
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_started_at = 0.0
        self._monitor_task_created_at = time.monotonic()
        self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))

    def _takeover_channel_renewal(self, my_gen):
        """接管安全通道续约：取消 asyncua 内部续约任务，启动自研续约监督。

        asyncua 内部 ``_renew_channel_loop`` 一旦续约失败就会抛异常退出，且
        ``pre_request_hook(check_connection)`` 会把该失败传播到后续所有请求，表现为
        每 45 分钟（1h 寿命的 75%）准时断线。我们改为：提前到 50% 寿命续约 + 失败
        重试，重试仍失败则在过期前主动触发受控重连。
        """
        if not self.client or my_gen != self._generation:
            return
        try:
            task = getattr(self.client, '_renew_channel_task', None)
            if task and not task.done():
                task.cancel()
        except Exception as e:
            global_logger.warning(f"Failed to cancel asyncua internal renew task: {e}")
        try:
            # 内部续约任务已取消，若保留 check_connection 作为 pre_request_hook，
            # 每个请求都会因 await 已取消任务而抛 CancelledError，必须置空。
            if getattr(self.client, 'uaclient', None):
                self.client.uaclient.pre_request_hook = None
        except Exception as e:
            global_logger.warning(f"Failed to clear asyncua pre_request_hook: {e}")
        if self._renew_task and not self._renew_task.done():
            self._renew_task.cancel()
        self._renew_started_at = 0.0
        self._renew_task_created_at = time.monotonic()
        self._renew_task = asyncio.create_task(self._renew_supervisor(my_gen))

    def _link_usable(self):
        """链路是否处于"需要续约/心跳"的可用状态。

        关键：不能只认 STATE_CONNECTED。续约与看门狗协程是在 transport 建立后、
        节点遍历/订阅（STATE_LOADING）期间启动的，此时 connected 仍为 False。
        旧实现用 self.connected 作为 while 条件，导致两个后台任务在第一次被调度时
        就判定"未连接"而退出：安全通道从此无人续约，连上整整 1 小时后被服务端按
        token 寿命切断（现场日志里 11:32 连接、12:32 报 BadTcpSecureChannelUnknown）。
        """
        return self._state in (self.STATE_LOADING, self.STATE_CONNECTED)

    def _channel_lifetime_ms(self):
        """取当前安全通道寿命（毫秒，优先使用服务端协商后的 RevisedLifetime）。"""
        try:
            value = int(getattr(self.client, 'secure_channel_timeout', 0) or 0)
        except (TypeError, ValueError):
            value = 0
        return value if value > 0 else DEFAULT_CHANNEL_LIFETIME_MS

    # 协程创建后超过该秒数仍未执行到第一行，就认定它被 qasync/Qt 的
    # processEvents() 重入卡成了"僵尸任务"（pending 但永远不会被调度）。
    ZOMBIE_TASK_GRACE = 2.0

    def _is_zombie_task(self, task, started_at, created_at, now):
        if task is None or task.done() or task.cancelled():
            return False
        if started_at > 0 or created_at <= 0:
            return False
        return (now - created_at) >= self.ZOMBIE_TASK_GRACE

    async def _ensure_link_tasks(self, my_gen):
        """确保续约监督与心跳看门狗都活着（含"僵尸任务"自愈）。

        24/7 运行中有两类致命但静默的后台任务故障：

        1. 协程正常 return（不抛异常）：_evaluate_health 看不到，链路"看起来正常"；
        2. 协程一步都没执行就永久 pending：qasync 里 Qt 的 QProgressDialog.setValue()
           会在 connect 协程尚未返回时重入 processEvents()，此时调度刚 create_task 的
           任务会触发 asyncio 的 "Cannot enter into task ... while another task ... is
           being executed"，该 Task 的第一次 __step 直接丢失，之后永远不会再被调度
           （现场表现：首次连接后既没有 [RENEW] 也没有 [HEARTBEAT]，1 小时后整点断线）。
           这种任务 done() 为 False，只按 done() 检查是发现不了的，必须比较
           "创建时间"和"协程真正开始执行的时间"。

        因此本函数是 async 的：补拉任务后主动让出一轮事件循环，保证新任务立刻跑到
        第一行（注册 started 时间戳并进入 sleep），避免它们再次被 UI 的 processEvents
        重入卡死。
        """
        if my_gen != self._generation or self.client is None or not self._link_usable():
            return

        now = time.monotonic()
        restarted = False

        if self._renew_task is None or self._renew_task.done():
            if self._renew_task is not None:
                detail = "cancelled" if self._renew_task.cancelled() else repr(self._renew_task.exception())
                global_logger.warning(f"[RENEW] Renewal supervisor was not running ({detail}); restarting it.")
            self._renew_started_at = 0.0
            self._renew_task_created_at = now
            self._renew_task = asyncio.create_task(self._renew_supervisor(my_gen))
            restarted = True
        elif self._is_zombie_task(self._renew_task, self._renew_started_at, self._renew_task_created_at, now):
            global_logger.error(
                "[RENEW] Renewal supervisor task has been pending for "
                f"{now - self._renew_task_created_at:.1f}s without ever starting "
                "(qasync re-entrancy zombie); replacing it."
            )
            self._renew_task.cancel()
            self._renew_started_at = 0.0
            self._renew_task_created_at = now
            self._renew_task = asyncio.create_task(self._renew_supervisor(my_gen))
            restarted = True

        if self._monitor_task is None or self._monitor_task.done():
            global_logger.warning("[HEARTBEAT] Connection monitor was not running; restarting it.")
            self._monitor_started_at = 0.0
            self._monitor_task_created_at = now
            self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))
            restarted = True
        elif self._is_zombie_task(self._monitor_task, self._monitor_started_at, self._monitor_task_created_at, now):
            global_logger.error(
                "[HEARTBEAT] Connection monitor task has been pending for "
                f"{now - self._monitor_task_created_at:.1f}s without ever starting "
                "(qasync re-entrancy zombie); replacing it."
            )
            self._monitor_task.cancel()
            self._monitor_started_at = 0.0
            self._monitor_task_created_at = now
            self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))
            restarted = True

        if restarted:
            # 让新任务在本次 await 中跑到第一行；否则回到 UI 代码后可能再次被
            # processEvents() 重入打断。执行两轮更稳妥。
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    async def _renew_supervisor(self, my_gen):
        """自研安全通道续约监督协程：提前续约、失败重试、兜底触发重连。

        续约时刻由"上次成功续约（或建链）时间 + 50% 寿命"推算，而不是从协程启动
        时刻重新计时；这样即使协程因调度抖动被重启，也不会把续约窗口整体后移。
        """
        # 第一行就登记"真正开始执行"的时间戳：_ensure_link_tasks 靠它识别僵尸任务。
        self._renew_started_at = time.monotonic()
        global_logger.info(f"[RENEW] SecureChannel renewal supervisor started (generation {my_gen}).")
        exit_reason = "unknown"
        try:
            while my_gen == self._generation and self.client is not None and self._link_usable():
                try:
                    lifetime_ms = self._channel_lifetime_ms()
                    lifetime_s = lifetime_ms / 1000.0
                    # 提前在 50% 寿命时续约（例如 1 小时寿命在 30 分钟续约），留足安全窗口
                    base = self._last_renewed_at or self._channel_connected_at or time.monotonic()
                    due = base + max(1.0, lifetime_s * CHANNEL_RENEW_FRACTION)
                    global_logger.info(
                        f"[RENEW] Next renewal in {max(0.0, due - time.monotonic()):.1f}s "
                        f"(channel lifetime {lifetime_ms}ms, renewal at "
                        f"{CHANNEL_RENEW_FRACTION * 100:.0f}% lifetime)."
                    )
                    last_status_log = time.monotonic()
                    while True:
                        now = time.monotonic()
                        if my_gen != self._generation or self.client is None or not self._link_usable():
                            break
                        rem = due - now
                        if rem <= 0:
                            break
                        if (now - last_status_log >= 60.0) or (rem <= 10.0 and now - last_status_log >= 5.0):
                            elapsed_total = now - base
                            remaining_total = max(0.0, lifetime_s - elapsed_total)
                            global_logger.info(
                                f"[RENEW] Channel health: lifetime={lifetime_s:.0f}s, "
                                f"elapsed={elapsed_total:.0f}s, channel remaining={remaining_total:.0f}s, "
                                f"next renew attempt in {rem:.0f}s"
                            )
                            last_status_log = now
                        await asyncio.sleep(min(5.0, max(0.05, rem)))
                except asyncio.CancelledError:
                    exit_reason = "cancelled"
                    raise
                if my_gen != self._generation or self.client is None or not self._link_usable():
                    exit_reason = "link no longer usable"
                    break
                try:
                    global_logger.info(
                        "[RENEW] Reached renewal trigger time. Initiating channel renewal..."
                    )
                    await self._renew_channel_with_retry(my_gen)
                    self._last_renewed_at = time.monotonic()
                    self.last_renewal_monotonic = self._last_renewed_at
                    self.renewal_count += 1
                    exit_reason = "renewal succeeded"
                except asyncio.CancelledError:
                    exit_reason = "cancelled"
                    raise
                except ConnectionSuperseded:
                    exit_reason = "superseded by a newer connection"
                    break
                except Exception as e:
                    # 重试仍失败：此时距离通道彻底过期尚有充足时间，主动触发受控重连，
                    # 避免被服务端强制切断 Socket
                    exit_reason = f"renewal failed: {e}"
                    global_logger.error(
                        f"[RENEW] Secure channel renewal failed after retries: {e}. "
                        f"Triggering proactive reconnect before channel expires.", exc_info=True
                    )
                    self._handle_connection_lost(f"secure channel renewal failed: {e}", my_gen)
                    break
        finally:
            if exit_reason in ("renewal succeeded", "link no longer usable",
                               "superseded by a newer connection", "cancelled") \
                    or exit_reason.startswith("renewal failed"):
                global_logger.info(f"[RENEW] Renew supervisor task exited ({exit_reason}).")
            else:
                # 非预期退出：链路还活着却没人续约，必须留下高等级线索；看门狗会负责补拉。
                global_logger.warning(
                    f"[RENEW] Renew supervisor exited unexpectedly ({exit_reason}); "
                    f"the heartbeat watchdog will restart it."
                )

    async def _renew_channel_with_retry(self, my_gen):
        """带重试的安全通道续约；全部失败时抛出最后一次异常。

        关键修复：asyncua 的 ``open_secure_channel`` 会把本次请求参数暂存到
        ``_open_secure_channel_exchange``，只有在**成功收到响应后**才清空。一旦首次
        续约超时，这个字段会永久残留，后续每次续约都会立刻抛出
        "Two Open Secure Channel requests can not happen too close to each other"，
        使"重试"形同虚设，只能等通道过期被服务端踢掉。因此每次重试前必须复位该字段。
        """
        max_attempts = 3
        backoffs = [5.0, 10.0, 20.0]
        last_exc = None
        for attempt in range(1, max_attempts + 1):
            if my_gen != self._generation or not self.client:
                raise ConnectionSuperseded("renewal superseded")
            client = self.client
            self._reset_secure_channel_exchange(client)
            try:
                req_lifetime = getattr(client, 'secure_channel_timeout', 0)
                global_logger.info(
                    f"[RENEW] Attempt {attempt}/{max_attempts} renewing secure channel "
                    f"(requested lifetime: {req_lifetime}ms)..."
                )
                ts_renew_start = time.monotonic()
                await asyncio.wait_for(
                    client.open_secure_channel(renew=True), timeout=RENEW_ATTEMPT_TIMEOUT
                )
                server_state_node = client.get_node(SERVER_STATE_NODE_ID)
                val = await asyncio.wait_for(server_state_node.read_value(), timeout=READ_TIMEOUT)
                revised_timeout = getattr(client, 'secure_channel_timeout', 0)
                elapsed_ms = (time.monotonic() - ts_renew_start) * 1000
                global_logger.info(
                    f"[RENEW] Secure channel renewed successfully (attempt {attempt}) in {elapsed_ms:.1f}ms. "
                    f"Active channel lifetime={revised_timeout}ms, ServerState={val}"
                )
                return
            except asyncio.CancelledError:
                raise
            except ConnectionSuperseded:
                raise
            except Exception as e:
                last_exc = e
                self._reset_secure_channel_exchange(client)
                global_logger.warning(f"[RENEW] Renewal attempt {attempt}/{max_attempts} failed: {e}")
                if attempt < max_attempts:
                    backoff = backoffs[attempt - 1]
                    global_logger.info(
                        f"[RENEW] Waiting {backoff:.1f}s before retry attempt {attempt + 1}..."
                    )
                    await asyncio.sleep(backoff)
        raise last_exc if last_exc is not None else Exception("Secure channel renewal failed")

    @staticmethod
    def _reset_secure_channel_exchange(client):
        """复位 asyncua 的 SecureChannel 交换暂存位（见 _renew_channel_with_retry 说明）。"""
        uaclient = getattr(client, 'uaclient', None)
        if uaclient is None:
            return
        exchange = getattr(uaclient, '_open_secure_channel_exchange', None)
        if exchange is not None:
            global_logger.warning(
                "[RENEW] Detected a stale OpenSecureChannel exchange left by a previous "
                "timeout; resetting it so the retry can actually reach the server."
            )
            try:
                setattr(uaclient, '_open_secure_channel_exchange', None)
            except Exception:
                pass

    async def _monitor_connection(self, my_gen):
        """健康看门狗。

        判据（全部基于链路活性，绝不依赖"是否有数据变化"）：
        * asyncua 自身的服务器看门狗任务是否意外死亡；
        * 自研续约监督任务是否意外死亡；
        * 主动读取 ServerState 是否成功；
        * 订阅的底层 Publish 任务是否还活着；
        * 安全通道是否已接近寿命上限却仍未成功续约。
        """
        # 第一行就登记"真正开始执行"的时间戳：_ensure_link_tasks 靠它识别僵尸任务。
        self._monitor_started_at = time.monotonic()
        heartbeat_ticks = 0
        try:
            while my_gen == self._generation and self.client is not None and self._link_usable():
                await asyncio.sleep(WATCHDOG_INTERVAL)
                if my_gen != self._generation or self.client is None or not self._link_usable():
                    break
                try:
                    # 自愈：续约监督若静默退出（正常 return，不抛异常），_evaluate_health
                    # 看不到它；这里先补拉任务，避免"链路健康但没人续约"直到 token 过期。
                    await self._ensure_link_tasks(my_gen)
                    failure = self._evaluate_health()
                    if failure:
                        raise RuntimeError(failure)

                    server_state = self.client.get_node(SERVER_STATE_NODE_ID)
                    state_val = await asyncio.wait_for(server_state.read_value(), timeout=READ_TIMEOUT)

                    # 刷新 data 活动时间（诊断用）
                    if self._sub_handler is not None:
                        self._last_data_at = self._sub_handler.last_data_time

                    heartbeat_ticks += 1
                    if heartbeat_ticks % 6 == 0:  # 约每分钟一条
                        total_conn_s = time.monotonic() - (self._channel_connected_at or time.monotonic())
                        since_renew_s = time.monotonic() - (self._last_renewed_at or self._channel_connected_at)
                        push_s = time.monotonic() - (self._last_data_at or time.monotonic())
                        global_logger.info(
                            f"[HEARTBEAT] Connection OK | ServerState={state_val} | uptime={total_conn_s:.0f}s, "
                            f"last renewal={since_renew_s:.0f}s ago, last push={push_s:.0f}s ago"
                        )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    global_logger.error(f"Connection monitor detected failure: {e}")
                    # 先打断底层 socket，避免 asyncua 内部请求永远悬挂在对端不响应的连接上
                    await self._force_kill_socket()
                    self._handle_connection_lost(f"heartbeat failure: {e}", my_gen)
                    break
        except asyncio.CancelledError:
            pass
        finally:
            global_logger.debug("Connection monitor task exited.")

    def _evaluate_health(self):
        """返回健康问题的描述字符串；健康时返回 None。"""
        client = self.client
        if client is None:
            return "client reference is gone"

        task = getattr(client, '_monitor_server_task', None)
        if task is not None and task.done() and not task.cancelled():
            exc = task.exception()
            if exc is not None:
                return f"asyncua server watchdog died: {exc!r}"

        if self._renew_task is not None and self._renew_task.done() and not self._renew_task.cancelled():
            exc = self._renew_task.exception()
            if exc is not None:
                return f"renew supervisor died: {exc!r}"

        uaclient = getattr(client, 'uaclient', None)
        publish_task = getattr(uaclient, '_publish_task', None) if uaclient else None
        if publish_task is not None and publish_task.done() and not publish_task.cancelled():
            exc = publish_task.exception()
            if exc is not None:
                return f"subscription publish task died: {exc!r}"

        timeout_ms = getattr(client, 'secure_channel_timeout', 0) or 0
        if timeout_ms <= 0:
            timeout_ms = 3_600_000
        lifetime_s = timeout_ms / 1000.0
        last_renew = self._last_renewed_at or self._channel_connected_at
        if last_renew > 0:
            elapsed = time.monotonic() - last_renew
            if elapsed > lifetime_s * 0.9:
                return (
                    f"secure channel renewal overdue ({elapsed:.0f}s > 90% of {lifetime_s:.0f}s lifetime)"
                )
        return None

    async def _on_asyncua_connection_lost(self, exc, my_gen):
        """asyncua 内部检测到的连接丢失回调（异步）。

        注意：本回调只处理**当前世代**的丢失。旧世代的 Client 在断开时也会触发，
        忽略它们可以避免"旧连接把新连接标记为断开"的经典竞态。
        """
        if my_gen != self._generation:
            global_logger.debug(
                f"Ignoring connection-lost from stale generation {my_gen} (current {self._generation})."
            )
            return
        global_logger.error(f"asyncua internal connection lost detected: {exc}")
        self._handle_connection_lost(f"asyncua watchdog: {exc}", my_gen)

    # ----------------------------------------------------------------------------------
    # 断线处理与自动重连
    # ----------------------------------------------------------------------------------

    def _handle_connection_lost(self, reason, my_gen):
        """幂等的断线入口。三条检测路径（续约、看门狗、asyncua 回调）都汇入这里。"""
        if my_gen != self._generation:
            return
        if self._state == self.STATE_DISCONNECTING or self._state == self.STATE_DISCONNECTED:
            return
        was_connected = self._state in (self.STATE_CONNECTED, self.STATE_LOADING)
        self.reconnect_attempt = 0
        if self._auto_reconnect:
            self._set_state(self.STATE_RECONNECTING, reason, **self._status_detail())
        else:
            self._set_state(self.STATE_DISCONNECTED, reason, **self._status_detail())

        if was_connected:
            cb = self.on_connection_lost
            if callable(cb):
                try:
                    cb()
                except Exception as e:
                    global_logger.error(f"on_connection_lost callback error: {e}", exc_info=True)

        # 关键：supervisor 可能在上一次"还连着"的时候就自然退出了
        # （reconnect_forever 可以在任何时刻被调用）。因此断线时必须确保它重新站起来，
        # 否则一次断线之后就再也不会自动恢复。
        if self._auto_reconnect and (self._reconnect_task is None or self._reconnect_task.done()):
            self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())

    async def reconnect_forever(self):
        """打开自治重连总闸，并确保 supervisor 已经在跑（幂等）。"""
        self._auto_reconnect = True
        if self._reconnect_task and not self._reconnect_task.done():
            return self._reconnect_task
        if self.connected:
            # 链路正常，无需重连；断线时 _handle_connection_lost 会自动拉起 supervisor
            return None
        if self._state not in (self.STATE_RECONNECTING, self.STATE_LOADING):
            self._set_state(self.STATE_RECONNECTING, "reconnect supervisor requested",
                            **self._status_detail())
        self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())
        return self._reconnect_task

    def stop_reconnect(self):
        """关闭自治重连总闸（同步，便于在关闭流程中调用）。"""
        self._auto_reconnect = False
        task = self._reconnect_task
        if task and not task.done():
            task.cancel()
        # 同时作废"排队中但尚未执行的 reconnect_forever 协程"：它可能在事件循环之外
        # 被 create_task 排队，晚于用户点击"停止自动重连"才真正执行，从而把总闸又打开，
        # 让停止按钮形同虚设。
        pending = getattr(self, "_pending_reconnect_forever", None)
        if pending is not None and not pending.done():
            pending.cancel()
        self._pending_reconnect_forever = None

    async def _stop_reconnect_supervisor(self):
        """取消当前 supervisor 任务，但**不改动** ``_auto_reconnect`` 总闸。

        这一点至关重要：``connect()`` 会调用本方法，而 supervisor 自己也要调用
        ``connect()``。如果这里顺手把总闸关掉，那么"第 1 次重连失败"就会把自动重连
        永久关闭，只重试一次就再也不恢复。总闸只由用户意图
        （``stop_reconnect`` / ``disconnect``）改写。
        """
        pending = getattr(self, "_pending_reconnect_forever", None)
        if pending is not None and not pending.done():
            pending.cancel()
        self._pending_reconnect_forever = None

        task = self._reconnect_task
        self._reconnect_task = None
        if task and not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=TASK_CANCEL_GRACE)
            except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
                pass

    def _next_reconnect_delay(self, attempt):
        """指数退避 + 抖动。attempt 从 1 开始。"""
        delay = self.reconnect_base_delay * (2 ** max(0, attempt - 1))
        delay = min(delay, self.reconnect_max_delay)
        if RECONNECT_JITTER > 0:
            delay *= 1.0 + random.uniform(-RECONNECT_JITTER, RECONNECT_JITTER)
        return max(1.0, delay)

    async def _reconnect_supervisor(self):
        """自治重连循环：指数退避 + 抖动，直到成功或用户要求停止。

        与旧实现（UI 里的 while 循环）的关键区别：这里的每一步都有硬超时，且任何
        异常都会回到退避重试，绝不会像以前那样卡在 ``get_all_nodes()`` 上永久不再重试。
        """
        global_logger.warning("Auto-reconnect supervisor started.")
        try:
            while self._auto_reconnect:
                if self.connected:
                    break
                self.reconnect_attempt += 1
                display_attempt = max(1, self.reconnect_attempt)
                delay = self._next_reconnect_delay(display_attempt)
                self._set_state(
                    self.STATE_RECONNECTING,
                    f"attempt {display_attempt} in {delay:.1f}s",
                    attempt=display_attempt,
                    total_reconnects=self.total_reconnects,
                    host=self.host,
                    port=self.port,
                )
                global_logger.info(f"[RECONNECT] Attempt {display_attempt}: retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
                if not self._auto_reconnect or self.connected:
                    break

                try:
                    await self._teardown_for_retry()
                    await self.connect()
                    await self.bootstrap()
                except asyncio.CancelledError:
                    raise
                except ConnectionSuperseded:
                    break
                except Exception as e:
                    global_logger.error(
                        f"[RECONNECT] Attempt {display_attempt} failed ({type(e).__name__}): {e!r}"
                    )
                    global_logger.debug("[RECONNECT] failure detail", exc_info=True)
                    # 防御：失败路径也必须留下干净的 Client 引用，否则下一轮 connect()
                    # 会跳过清理，把半死的 socket 与任务一路带下去
                    if self.client is not None:
                        await self._teardown_for_retry()
                    continue

                if self.connected:
                    self.total_reconnects += 1
                    self.reconnect_attempt = 0
                    global_logger.warning(
                        f"[RECONNECT] Reconnected successfully on attempt {display_attempt} "
                        f"(total successful recoveries: {self.total_reconnects})."
                    )
                    break
                global_logger.warning(
                    f"[RECONNECT] Attempt {display_attempt} finished but the link is not up; retrying."
                )
        except asyncio.CancelledError:
            global_logger.info("Auto-reconnect supervisor cancelled.")
            raise
        except BaseException as e:
            global_logger.error(f"Auto-reconnect supervisor crashed fatally: {e}", exc_info=True)
        finally:
            global_logger.info(
                f"Auto-reconnect supervisor exited (connected={self.connected}, "
                f"auto_reconnect={self._auto_reconnect})."
            )
            if not self.connected and not self._auto_reconnect:
                # 用户已明确不再需要重连
                self._set_state(self.STATE_DISCONNECTED, "reconnect supervisor exited",
                                **self._status_detail())
            elif not self.connected and self._auto_reconnect:
                # 兜底自愈：任何让 supervisor 意外退出的路径都不应该让自动重连永久失效
                if self._reconnect_task is None or self._reconnect_task.done():
                    global_logger.warning("Reconnect supervisor exited unexpectedly; re-arming it.")
                    self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())

    async def _teardown_for_retry(self):
        """重连前的清理：确保上一个 Client 的 socket/任务全部收尾。"""
        self._ensure_locks()
        async with self._client_lock:
            await self._cleanup_client()

    # ----------------------------------------------------------------------------------
    # 断开
    # ----------------------------------------------------------------------------------

    async def disconnect(self):
        """安全彻底断开连接（幂等、有界）。

        死锁防御原则：
        1. 先停掉自治重连与看门狗，避免清理过程中被重新拉起；
        2. 若底层 socket 仍然存活，先尝试优雅关闭 Session/SecureChannel（短超时）；
        3. 无论优雅关闭是否成功，都强制关闭底层 Socket/Transport 打断所有在途请求；
        4. 用非阻塞的 asyncio.wait(timeout=...) 等待任务收尾，绝不用 wait_for(gather)；
        5. 无论出现任何异常，必须确保所有资源引用彻底解除。
        """
        self._ensure_locks()
        async with self._disconnect_lock:
            global_logger.info(f"[disconnect] Starting disconnect sequence (state={self._state})...")
            self._auto_reconnect = False
            self._generation += 1  # 立即作废所有旧世代回调
            self._set_state(self.STATE_DISCONNECTING, "disconnect requested")

            self._cancel_owned_tasks()

            client = self.client
            if client is not None and self._socket_is_open(client):
                global_logger.info("[disconnect] Attempting graceful session/channel close...")
                try:
                    await asyncio.wait_for(client.disconnect(), timeout=GRACEFUL_CLOSE_TIMEOUT)
                    global_logger.info("[disconnect] Graceful close completed.")
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    global_logger.warning(
                        f"[disconnect] Graceful close exceeded {GRACEFUL_CLOSE_TIMEOUT:.1f}s; "
                        f"forcing socket shutdown."
                    )
                except Exception as e:
                    global_logger.debug(f"[disconnect] Graceful close failed (non-fatal): {e}")

            await self._cleanup_client()
            await self._await_owned_tasks()

            self.subscription = None
            self._sub_handler = None
            self._channel_connected_at = 0.0
            self._last_renewed_at = 0.0
            self.reconnect_attempt = 0
            self._consecutive_write_failures = 0

            self._set_state(self.STATE_DISCONNECTED, "disconnect complete")
            global_logger.info("[disconnect] Disconnect sequence complete.")

    def _cancel_owned_tasks(self):
        for name in ("_monitor_task", "_renew_task", "_reconnect_task"):
            task = getattr(self, name, None)
            if task and not task.done():
                global_logger.info(f"[disconnect] Cancelling {name}...")
                task.cancel()

    async def _await_owned_tasks(self):
        pending = [t for t in (self._monitor_task, self._renew_task, self._reconnect_task)
                   if t and not t.done()]
        self._monitor_task = None
        self._renew_task = None
        self._reconnect_task = None
        if not pending:
            return
        try:
            done, still_pending = await asyncio.wait(pending, timeout=TASK_CANCEL_GRACE)
            if still_pending:
                global_logger.warning(
                    f"[disconnect] {len(still_pending)} background task(s) did not finish within "
                    f"{TASK_CANCEL_GRACE:.1f}s (non-fatal, abandoned)."
                )
            else:
                global_logger.info("[disconnect] All background tasks stopped cleanly.")
        except Exception as e:
            global_logger.debug(f"[disconnect] Task wait exception suppressed: {e}")

    @staticmethod
    def _socket_is_open(client):
        """判断 client 的底层 socket 是否还活着（决定是否需要优雅关闭）。"""
        try:
            protocol = getattr(getattr(client, 'uaclient', None), 'protocol', None)
            if protocol is None:
                return False
            return getattr(protocol, 'state', None) == 0  # UASocketProtocol.OPEN == 0
        except Exception:
            return True

    async def _cleanup_client(self):
        """强行拆掉当前 Client 的所有资源引用（幂等）。

        只做"打断"和"引用解除"：不依赖任何网络往返，因此绝不会挂起。
        """
        client = self.client
        if client is None:
            return
        try:
            client._closing = True
        except Exception:
            pass
        for attr in ('_monitor_server_task', '_renew_channel_task'):
            task = getattr(client, attr, None)
            if task and not task.done():
                task.cancel()
        uaclient = getattr(client, 'uaclient', None)
        if uaclient is not None:
            try:
                uaclient._closing = True
                uaclient.pre_request_hook = None
                publish_task = getattr(uaclient, '_publish_task', None)
                if publish_task and not publish_task.done():
                    publish_task.cancel()
            except Exception:
                pass
        try:
            client.disconnect_socket()
        except Exception as e:
            global_logger.debug(f"[cleanup] Socket disconnect suppressed: {e}")
        if uaclient is not None:
            try:
                proto = getattr(uaclient, 'protocol', None)
                if proto is not None:
                    transport = getattr(proto, 'transport', None)
                    if transport is not None and hasattr(transport, 'close'):
                        try:
                            transport.close()
                        except Exception:
                            pass
                        proto.transport = None
                    uaclient.protocol = None
            except Exception:
                pass
            # 清空 asyncua 的请求回调表：断线时 asyncua 不会主动失败这些 Future，
            # 残留项会让下一次连接的迟到响应命中旧 request_id 并抛 KeyError。
            try:
                callback_map = getattr(uaclient, '_callbackmap', None)
                if callback_map is not None:
                    callback_map.clear()
            except Exception:
                pass
        self.client = None
        self.subscription = None
        self._sub_handler = None

    async def _force_kill_socket(self):
        """只打断底层 socket（保留 Client 引用，供后续 _cleanup_client 统一收尾）。"""
        client = self.client
        if client is None:
            return
        try:
            client.disconnect_socket()
        except Exception:
            pass

    # ----------------------------------------------------------------------------------
    # 写入
    # ----------------------------------------------------------------------------------

    def _get_variant_type(self, node_id, value):
        """使用显式 _TYPE_MAP 映射表获取 Variant 类型，避免依赖不同 asyncua 版本差异。"""
        # Python 中 bool 是 int 的子类，必须先检查 bool
        if isinstance(value, bool):
            variant_type = ua.VariantType.Boolean
        elif isinstance(value, int):
            variant_type = ua.VariantType.Int16
        elif isinstance(value, float):
            variant_type = ua.VariantType.Float
        else:
            variant_type = ua.VariantType.String

        cached = self.nodes.get(node_id)
        cached_type = cached.get('type', '') if cached else ''
        if cached_type in _TYPE_MAP:
            variant_name, converter = _TYPE_MAP[cached_type]
            try:
                value = converter(value)
                variant_type = getattr(ua.VariantType, variant_name)
            except Exception:
                pass  # 转换失败时保留默认类型
        # 模糊匹配兜底（向后兼容旧数据）
        elif 'Int' in cached_type:
            variant_type = getattr(ua.VariantType, cached_type, ua.VariantType.Int16)
            try:
                value = int(value)
            except Exception:
                pass
        elif 'Float' in cached_type or 'Double' in cached_type or 'Real' in cached_type:
            variant_type = getattr(ua.VariantType, cached_type, ua.VariantType.Float)
            try:
                value = float(value)
            except Exception:
                pass
        elif 'String' in cached_type:
            variant_type = ua.VariantType.String
            value = str(value)
        elif 'Boolean' in cached_type:
            variant_type = ua.VariantType.Boolean
            value = bool(value)

        return variant_type, value

    async def write_node_value(self, node_id, value, display_name=None):
        """向 OPC 服务器下发控制指令"""
        if not self.connected:
            global_logger.warning("Attempted to write while not connected.")
            return False

        target_display = display_name if display_name else node_id
        ts_start = time.monotonic()
        my_gen = self._generation
        try:
            node = self.client.get_node(node_id)
            variant_type, value = self._get_variant_type(node_id, value)
            data_value = ua.DataValue(ua.Variant(value, variant_type))
            await asyncio.wait_for(node.write_value(data_value), timeout=READ_TIMEOUT)

            elapsed_ms = (time.monotonic() - ts_start) * 1000
            global_logger.info(f"[WRITE] {target_display} = {value} | {elapsed_ms:.1f}ms")
            self._consecutive_write_failures = 0
            return True
        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            global_logger.error(
                f"Timeout ({READ_TIMEOUT:.1f}s) while writing to node {target_display} "
                f"| elapsed={elapsed_ms:.1f}ms"
            )
            self._on_write_failure(my_gen)
            return False
        except Exception as e:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            global_logger.error(
                f"Failed to write to node {target_display} | elapsed={elapsed_ms:.1f}ms | {e}"
            )
            self._on_write_failure(my_gen)
            return False

    async def write_values_batch(self, node_ids, value, display_names=None):
        """批量写入多个节点（使用 WriteList 减少 RTT）。

        :returns: (成功数, 失败数)
        """
        if not self.connected:
            global_logger.warning("Attempted batch write while not connected.")
            return 0, len(node_ids)

        if not node_ids:
            return 0, 0

        ts_start = time.monotonic()
        my_gen = self._generation
        display_names = display_names or [None] * len(node_ids)

        try:
            nodes = [self.client.get_node(nid) for nid in node_ids]

            variant_types = []
            values = []
            for nid in node_ids:
                vt, val = self._get_variant_type(nid, value)
                variant_types.append(vt)
                values.append(val)

            data_values = [ua.DataValue(ua.Variant(v, vt)) for v, vt in zip(values, variant_types)]

            # 关键：设置 raise_on_partial_error=False，允许部分失败而不抛出全局异常
            status_codes = await asyncio.wait_for(
                self.client.write_values(nodes, data_values, raise_on_partial_error=False),
                timeout=max(READ_TIMEOUT, 30.0)
            )

            success_count = sum(1 for sc in status_codes if hasattr(sc, 'is_good') and sc.is_good())
            fail_count = len(status_codes) - success_count

            elapsed_ms = (time.monotonic() - ts_start) * 1000
            if fail_count == 0:
                global_logger.info(
                    f"[BATCH_WRITE] All {len(node_ids)} nodes = {value} | {elapsed_ms:.1f}ms "
                    f"| avg={elapsed_ms / max(1, len(node_ids)):.2f}ms/node"
                )
                self._consecutive_write_failures = 0
            else:
                global_logger.warning(
                    f"[BATCH_WRITE] Partial result: {success_count} ok, {fail_count} failed "
                    f"out of {len(node_ids)} nodes | {elapsed_ms:.1f}ms"
                )
                for nid, dv, sc in zip(node_ids, display_names, status_codes):
                    if hasattr(sc, 'is_good') and not sc.is_good():
                        global_logger.warning(
                            f"  -> Node write rejected: {dv or nid} (ID: {nid}) | StatusCode: {sc}"
                        )

                # 只要该批次中存在任意 1 个节点写入成功，即证明网络链路是健康的，
                # 因此重置连续失败计数
                if success_count > 0:
                    self._consecutive_write_failures = 0
                else:
                    self._on_write_failure(my_gen)

            return success_count, fail_count

        except asyncio.CancelledError:
            raise
        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            global_logger.error(
                f"Batch write timeout | {len(node_ids)} nodes | elapsed={elapsed_ms:.1f}ms"
            )
            self._on_write_failure(my_gen)
            return 0, len(node_ids)
        except Exception as e:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            global_logger.error(
                f"Batch write failed | {len(node_ids)} nodes | elapsed={elapsed_ms:.1f}ms | {e}"
            )
            self._on_write_failure(my_gen)
            return 0, len(node_ids)

    def _on_write_failure(self, my_gen=None):
        """写入失败时累加计数，连续失败超过阈值时触发受控重连。"""
        if my_gen is not None and my_gen != self._generation:
            return
        self._consecutive_write_failures += 1
        global_logger.warning(
            f"Write failure count: {self._consecutive_write_failures}/{self._max_write_failures}"
        )
        if self._consecutive_write_failures >= self._max_write_failures:
            global_logger.error(
                f"Consecutive write failures reached threshold ({self._max_write_failures}). "
                f"Connection appears stale. Triggering reconnection..."
            )
            self._consecutive_write_failures = 0
            # 统一走幂等的断线入口，避免与看门狗/续约检测重复触发重连
            self._handle_connection_lost("consecutive write failures", self._generation)
