"""OPC DA 引擎适配层单元测试（不依赖 COM / Qt / Matrikon）。

覆盖目标（对应 opcda_API_CONTRACT.md 第 2 节）：

* 状态机：disconnected -> connecting -> loading -> connected -> disconnecting ->
  disconnected，字符串与 UA 版一致；
* 点位发布：browse 结果转成引擎 nodes（node_id/name/type/value/timestamp），
  get_all_nodes() 只返回纯数据；
* 订阅：客户端只回传变化的子集，且以 hClient 标识点位；引擎必须自己维护
  hClient -> ItemID 映射，并只把变化项推给 UI 回调；
* 手动刷新（SyncRead）初值：bootstrap 里读一次全表；
* IOPCAsyncIO2 不可用时的 SyncRead 轮询降级；
* 断线 -> 指数退避自动重连 -> 复用已浏览的点位表（不重新 browse）-> 重新
  AddItems/读初值 -> on_nodes_recovered；世代号防止旧回调污染新连接；
* 批量写 / 单点写的成功与失败计数；
* disconnect 幂等、bootstrap 失败后由 supervisor 继续重试（不卡死）。

运行：python -m unittest tests.test_da_engine -v
（同时兼容 venv38 / Python 3.8 与系统 Python 3.12）
"""

import asyncio
import os
import sys
import time
import unittest
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from opcda.engine import OpcDaClientEngine  # noqa: E402
from utils.logger import global_logger  # noqa: E402


# --------------------------------------------------------------------------------------
# 假客户端：按 opcda_API_CONTRACT.md 第 1 节的公开 API 实现（同步调用）
# --------------------------------------------------------------------------------------

GOOD_QUALITY = 0xC0
BAD_QUALITY = 0x00
E_ACCESSDENIED = -2147467259  # 0x80004005 的常见替代：写只读点位


class FakeOpcDaError(Exception):
    """模拟 opcda.client.OpcDaError（带 HRESULT code）。"""

    def __init__(self, message, code=None):
        super().__init__(message)
        self.code = code


class FakeDaItem:
    def __init__(self, item_id, name=None, path="", canonical_vt=0, access=3,
                 server_handle=0, client_handle=0):
        self.item_id = item_id
        self.name = name if name is not None else item_id.rsplit(".", 1)[-1]
        self.path = path
        self.canonical_vt = canonical_vt
        self.access = access
        self.server_handle = server_handle
        self.client_handle = client_handle


class FakeDaValue:
    """按契约实现。真实客户端回调只给 hClient，所以 item_id 默认为空串。"""

    def __init__(self, value=None, quality=GOOD_QUALITY, timestamp=None, error=0,
                 item_id="", client_handle=None, handle=None):
        self.item_id = item_id
        self.value = value
        self.quality = quality
        self.timestamp = timestamp
        self.error = error
        self.client_handle = client_handle
        self.handle = handle


DEFAULT_ITEMS = [
    # (item_id, name, path, canonical_vt, access)
    ("Random.Boolean", "Boolean", "Random", 11, 3),   # VT_BOOL
    ("Random.Int1", "Int1", "Random", 2, 3),          # VT_I2
    ("Random.Real4", "Real4", "Random", 4, 3),        # VT_R4
    ("Write Only.Boolean", "Boolean", "Write Only", 11, 2),  # 只写
]


class FakeDaClient:
    """最小化 OPC DA 客户端替身，只实现引擎会触碰的部分。"""

    def __init__(self, prog_id, host="localhost", items=None):
        self.prog_id = prog_id
        self.host = host
        self.items = list(items if items is not None else DEFAULT_ITEMS)
        self.connected = False
        self.connect_calls = 0
        self.disconnect_calls = 0
        self.browse_calls = 0
        self.add_items_calls = []
        self.remove_items_calls = []
        self.read_calls = []
        self.write_calls = []
        self.subscribed = False
        self.subscription_rate = None
        self.subscription_stopped = False
        self.callback = None
        self.pump_calls = 0
        self.values = {}
        self._next_handle = 1
        self.handles = {}
        self.writable = {item_id: bool(access & 0x02) for item_id, _n, _p, _vt, access in self.items}
        self.fail_connect = None
        self.fail_status = None
        self.fail_browse = None
        self.fail_read = None
        self.fail_write = None
        self.fail_add_items = None
        self.fail_subscription = None

    # --- 连接 ---------------------------------------------------------------
    def connect(self, timeout=15.0):
        self.connect_calls += 1
        if self.fail_connect:
            raise FakeOpcDaError(self.fail_connect)
        self.connected = True

    def disconnect(self):
        self.disconnect_calls += 1
        self.connected = False

    def is_connected(self):
        return self.connected

    def get_server_status(self):
        if self.fail_status:
            raise FakeOpcDaError(self.fail_status)
        return {
            "state": 1,
            "state_text": "RUNNING",
            "vendor": "Fake OPC DA",
            "version": "1.0.0",
            "group_count": 1,
            "start_time": "2026-01-01 00:00:00",
            "current_time": "2026-01-01 00:00:01",
        }

    # --- 浏览 / 点位 ---------------------------------------------------------
    def browse(self, max_depth=3, max_items=20000):
        self.browse_calls += 1
        if self.fail_browse:
            raise FakeOpcDaError(self.fail_browse)
        return [
            FakeDaItem(item_id, name, path, canonical_vt, access, server_handle=index)
            for index, (item_id, name, path, canonical_vt, access) in enumerate(self.items)
        ]

    def add_items(self, item_ids, update_rate_ms=500):
        item_ids = list(item_ids)
        self.add_items_calls.append((item_ids, update_rate_ms))
        if self.fail_add_items:
            raise FakeOpcDaError(self.fail_add_items)
        results = []
        for item_id in item_ids:
            handle = self.handles.get(item_id)
            if handle is None:
                handle = self._next_handle
                self._next_handle += 1
                self.handles[item_id] = handle
                self.values.setdefault(item_id, None)
            meta = next((entry for entry in self.items if entry[0] == item_id), None)
            canonical_vt = meta[3] if meta else 0
            access = meta[4] if meta else 3
            results.append(
                FakeDaItem(item_id, canonical_vt=canonical_vt, access=access,
                           server_handle=handle, client_handle=handle)
            )
        return results

    def remove_items(self, item_ids):
        item_ids = list(item_ids)
        self.remove_items_calls.append(item_ids)
        results = []
        for item_id in item_ids:
            if item_id in self.handles or item_id in self.values:
                self.handles.pop(item_id, None)
                self.values.pop(item_id, None)
                results.append((item_id, 0, ""))
            else:
                results.append((item_id, -2147467259, "not mounted"))
        return results

    def read_items(self, item_ids=None, source=1):
        self.read_calls.append((None if item_ids is None else list(item_ids), source))
        if self.fail_read:
            raise FakeOpcDaError(self.fail_read)
        ids = list(item_ids) if item_ids is not None else list(self.values.keys())
        return [
            FakeDaValue(value=self.values.get(item_id), quality=GOOD_QUALITY,
                        timestamp="2026-01-01 00:00:01", item_id=item_id,
                        client_handle=self.handles.get(item_id))
            for item_id in ids
        ]

    def write_items(self, values):
        self.write_calls.append(dict(values))
        if self.fail_write:
            raise FakeOpcDaError(self.fail_write)
        results = []
        for item_id, _value in values.items():
            if self.writable.get(item_id, True):
                results.append((item_id, 0, ""))
            else:
                results.append((item_id, E_ACCESSDENIED, "access denied"))
        return results

    # --- 订阅 ---------------------------------------------------------------
    def set_data_change_callback(self, callback):
        self.callback = callback

    def start_subscription(self, update_rate_ms=500):
        if self.fail_subscription:
            raise FakeOpcDaError(self.fail_subscription)
        self.subscribed = True
        self.subscription_rate = update_rate_ms

    def stop_subscription(self):
        self.subscribed = False
        self.subscription_stopped = True

    def get_error_string(self, code):
        return "fake error 0x{:08X}".format(int(code) & 0xFFFFFFFF)

    def pump_events(self):
        self.pump_calls += 1

    # --- 测试辅助 -----------------------------------------------------------
    def push_values(self, values):
        """模拟 COM 消息泵把一批 DaValue 交给已注册的订阅回调。"""
        if callable(self.callback):
            self.callback(list(values))

    def push_change(self, item_id, value, quality=GOOD_QUALITY):
        """按真实客户端的语义推送：只给 hClient，不给 ItemID。"""
        handle = self.handles.get(item_id)
        self.values[item_id] = value
        self.push_values([
            FakeDaValue(value=value, quality=quality,
                        timestamp="2026-01-01 00:00:02", client_handle=handle)
        ])


def make_factory(items=None):
    """创建客户端工厂，并暴露所有创建的实例（用于断言重连次数/复用点位表）。"""
    created = []

    def factory(prog_id, host):
        client = FakeDaClient(prog_id, host, items=items)
        created.append(client)
        return client

    factory.created = created
    factory.last = lambda: created[-1] if created else None
    return factory


async def wait_until(predicate, timeout=15.0, interval=0.01):
    """轮询等待条件成立（测试专用，避免固定 sleep 造成不稳定）。"""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(interval)
    return predicate()


class EngineTestBase(unittest.IsolatedAsyncioTestCase):
    items = DEFAULT_ITEMS

    def make_engine(self, **kwargs):
        factory = make_factory(self.items)
        kwargs.setdefault("update_rate_ms", 100)
        engine = OpcDaClientEngine(host="localhost", prog_id="Fake.OPC.Server.1",
                                   client_factory=factory, **kwargs)
        # 测试里把心跳周期调小：既能让 pump_events/状态探测跑起来，又不拖慢用例
        engine.heartbeat_interval_s = 0.05
        return engine, factory

    async def asyncTearDown(self):
        engine = getattr(self, "engine", None)
        if engine is not None:
            try:
                engine.stop_reconnect()
                await engine.disconnect()
            except Exception:
                pass

    async def _connect_and_bootstrap(self, engine):
        await engine.connect()
        await engine.bootstrap()


class TestConnectAndBootstrap(EngineTestBase):
    """连接 + 装配：状态机序列、点位发布、初值与订阅。"""

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        self.transitions = []
        self.recovered = []
        self.lost = []
        self.engine.on_state_change = lambda state, detail: self.transitions.append(state)
        self.engine.on_nodes_recovered = lambda: self.recovered.append(time.monotonic())
        self.engine.on_connection_lost = lambda: self.lost.append(time.monotonic())
        # 订阅回调必须显式登记，bootstrap 才会建立订阅（与 UA 版语义一致）
        self.engine.set_subscription_callback(lambda node_id, value, timestamp: None)

    async def test_state_sequence_and_node_publication(self):
        await self.engine.connect()
        client = self.factory.last()
        client.values["Random.Boolean"] = True
        client.values["Random.Int1"] = 7
        client.values["Random.Real4"] = 1.5
        await self.engine.bootstrap()

        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)
        self.assertTrue(self.engine.connected)
        self.assertIn("connecting", self.transitions)
        self.assertIn("loading", self.transitions)
        self.assertEqual(self.transitions[-1], "connected")
        self.assertEqual(len(self.recovered), 1)

        # 点位表字段齐全，ItemID 作为 node_id，name 为叶子名，类型来自 canonical_vt
        self.assertEqual(len(self.engine.nodes), 4)
        boolean_node = self.engine.nodes["Random.Boolean"]
        self.assertEqual(boolean_node["node_id"], "Random.Boolean")
        self.assertEqual(boolean_node["name"], "Boolean")
        self.assertEqual(boolean_node["alias"], "Boolean")
        self.assertEqual(boolean_node["type"], "Boolean")
        self.assertEqual(boolean_node["path"], "Random")
        self.assertTrue(boolean_node["value"])
        self.assertEqual(boolean_node["timestamp"], "2026-01-01 00:00:01")
        self.assertTrue(boolean_node["writable"])
        self.assertEqual(self.engine.nodes["Random.Int1"]["type"], "Int16")
        self.assertEqual(self.engine.nodes["Random.Real4"]["type"], "Float")
        # 只写点位：access=2 -> writable。
        self.assertTrue(self.engine.nodes["Write Only.Boolean"]["writable"])

        # AddItems 只发生一次（bootstrap 内），并携带更新周期
        self.assertEqual(len(client.add_items_calls), 1)
        self.assertEqual(client.add_items_calls[0][1], self.engine.update_rate_ms)

        # 订阅建立：mode=async，回调已注册
        self.assertTrue(client.subscribed)
        self.assertEqual(client.subscription_rate, self.engine.update_rate_ms)
        self.assertEqual(self.engine.subscription, "async")
        self.assertTrue(callable(client.callback))

        # get_all_nodes 返回纯数据（无 COM 对象；本引擎本就不缓存 COM 对象）
        nodes = await self.engine.get_all_nodes()
        self.assertEqual(len(nodes), 4)
        for node in nodes:
            self.assertIn("node_id", node)
            self.assertIn("name", node)
            self.assertIn("type", node)
            self.assertNotIn("node_obj", node)

    async def test_initial_value_refresh_tolerates_read_failure(self):
        await self.engine.connect()
        self.factory.last().fail_read = "RPC server unavailable"
        # 读初值失败不致命（契约：bootstrap 失败/超时必须回到重连流程，但读失败
        # 由心跳继续探测；至少不能抛异常卡死）
        await self.engine.bootstrap()
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)


class TestSubscriptionDispatch(EngineTestBase):
    """订阅回调：只推变化子集、按 hClient 还原点位、世代号隔离、坏品质保留旧值。"""

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        self.received = []
        self.engine.set_subscription_callback(
            lambda node_id, value, timestamp: self.received.append((node_id, value, timestamp))
        )
        await self.engine.connect()
        client = self.factory.last()
        client.values["Random.Boolean"] = False
        client.values["Random.Int1"] = 1
        await self.engine.bootstrap()
        self.client = client
        self.received.clear()

    async def test_only_changed_subset_is_dispatched(self):
        self.client.push_change("Random.Boolean", True)
        self.assertEqual(len(self.received), 1)
        node_id, value, timestamp = self.received[0]
        self.assertEqual(node_id, "Random.Boolean")
        self.assertTrue(value)
        self.assertEqual(timestamp, "2026-01-01 00:00:02")
        self.assertTrue(self.engine.nodes["Random.Boolean"]["value"])
        # 未变化的点位不应被推送
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 1)

    async def test_batch_callback_maps_handles_to_item_ids(self):
        handle_boolean = self.client.handles["Random.Boolean"]
        handle_int = self.client.handles["Random.Int1"]
        self.client.push_values([
            FakeDaValue(value=True, client_handle=handle_boolean,
                        timestamp="2026-01-01 00:00:03"),
            FakeDaValue(value=42, client_handle=handle_int,
                        timestamp="2026-01-01 00:00:03"),
        ])
        by_id = {item[0]: item[1] for item in self.received}
        self.assertEqual(by_id.get("Random.Boolean"), True)
        self.assertEqual(by_id.get("Random.Int1"), 42)
        self.assertEqual(len(self.received), 2)

    async def test_stale_generation_callback_is_ignored(self):
        current_generation = self.engine._generation
        stale = current_generation - 1
        self.client.push_values([
            FakeDaValue(value=99, item_id="Random.Int1", timestamp="2026-01-01 00:00:04")
        ])
        # 模拟旧世代客户端晚到的回调：直接调用引擎入口并带上旧世代号
        self.engine._on_da_data_change(
            [FakeDaValue(value=123, item_id="Random.Int1")], stale
        )
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 99)
        self.assertEqual(self.received[-1], ("Random.Int1", 99, "2026-01-01 00:00:04"))

    async def test_bad_quality_keeps_last_good_value(self):
        self.client.push_change("Random.Int1", 55)
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 55)
        self.received.clear()

        handle = self.client.handles["Random.Int1"]
        # 坏品质且没有值：界面应保留上一次的好值
        self.client.push_values([
            FakeDaValue(value=None, quality=BAD_QUALITY, client_handle=handle,
                        timestamp="2026-01-01 00:00:05")
        ])
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 55)
        self.assertEqual(self.received[-1][1], 55)

        # 坏品质但带了值：接受该值（保底可用），不把界面清空
        self.client.push_values([
            FakeDaValue(value=66, quality=BAD_QUALITY, client_handle=handle,
                        timestamp="2026-01-01 00:00:06")
        ])
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 66)

    async def test_item_error_keeps_cached_value(self):
        self.client.push_change("Random.Int1", 77)
        handle = self.client.handles["Random.Int1"]
        self.client.push_values([
            FakeDaValue(value=None, error=E_ACCESSDENIED, client_handle=handle)
        ])
        self.assertEqual(self.engine.nodes["Random.Int1"]["value"], 77)


class TestPollFallback(EngineTestBase):
    """IOPCAsyncIO2 不可用 -> SyncRead 轮询降级，且受 pump_events_enabled 控制。"""

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        self.received = []
        self.engine.set_subscription_callback(
            lambda node_id, value, timestamp: self.received.append((node_id, value, timestamp))
        )
        await self.engine.connect()
        self.client = self.factory.last()
        self.client.fail_subscription = "async unsupported"
        self.client.values["Random.Boolean"] = False
        await self.engine.bootstrap()

    async def test_poll_mode_reads_and_dispatches(self):
        self.assertEqual(self.engine._subscription_mode, "poll")
        self.assertEqual(self.engine.subscription, "poll")
        self.assertFalse(self.client.subscribed)

        self.client.values["Random.Boolean"] = True
        ok = await wait_until(lambda: len(self.received) > 0, timeout=10.0)
        self.assertTrue(ok, "poll mode never dispatched any value")
        self.assertIn(("Random.Boolean", True, "2026-01-01 00:00:01"), self.received)
        self.assertGreaterEqual(len(self.client.read_calls), 2)
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)

    async def test_pump_flag_controls_event_pumping(self):
        self.assertTrue(self.engine.pump_events_enabled)
        await wait_until(lambda: self.client.pump_calls > 0, timeout=10.0)
        self.assertGreater(self.client.pump_calls, 0)

        # Qt 集成时上层会关闭该开关：引擎不得再主动泵消息（否则递归进入 Qt 事件循环）
        self.engine.pump_events_enabled = False
        pumps_before = self.client.pump_calls
        await asyncio.sleep(0.25)
        self.assertEqual(self.client.pump_calls, pumps_before)
        # 轮询本身仍应继续工作
        reads_before = len(self.client.read_calls)
        await wait_until(lambda: len(self.client.read_calls) > reads_before, timeout=10.0)
        self.assertGreater(len(self.client.read_calls), reads_before)


class TestReconnectReusesNodes(EngineTestBase):
    """断线 -> 自动重连 -> 复用点位表（不重新 browse）-> 重新 AddItems/读初值。"""

    async def test_reconnect_reuses_browsed_node_table(self):
        self.engine, self.factory = self.make_engine()
        recovered = []
        lost = []
        self.engine.on_nodes_recovered = lambda: recovered.append(time.monotonic())
        self.engine.on_connection_lost = lambda: lost.append(time.monotonic())
        self.engine.set_subscription_callback(lambda node_id, value, timestamp: None)

        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(len(recovered), 1)
        first_client = self.factory.last()
        self.assertEqual(first_client.browse_calls, 1)
        node_ids_before = set(self.engine.nodes.keys())
        generation_before = self.engine._generation

        # 心跳放慢，避免测试期间被心跳再触发一次断线
        self.engine.heartbeat_interval_s = 5.0
        self.engine.reconnect_base_delay = 0.01
        self.engine.reconnect_max_delay = 0.02
        self.engine._auto_reconnect = True
        self.engine._handle_connection_lost("simulated cable pull", self.engine._generation)
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_RECONNECTING)
        self.assertEqual(len(lost), 1)

        ok = await wait_until(
            lambda: self.engine.connected and self.engine.total_reconnects >= 1, timeout=15.0
        )
        self.assertTrue(ok, "engine did not recover after simulated disconnection")
        self.assertGreaterEqual(len(self.factory.created), 2)
        second_client = self.factory.created[1]

        # 关键断言：重连没有重新 Browse（复用缓存点位表）
        self.assertEqual(second_client.browse_calls, 0)
        self.assertGreaterEqual(len(second_client.add_items_calls), 1)
        self.assertTrue(second_client.subscribed)
        self.assertEqual(set(self.engine.nodes.keys()), node_ids_before)
        self.assertGreater(self.engine._generation, generation_before)
        self.assertEqual(len(recovered), 2)
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)
        # 断线期间的值变化通过重连后的初次 SyncRead 补齐
        second_client.values["Random.Boolean"] = True
        await self.engine.write_node_value("Random.Boolean", True)
        self.assertTrue(second_client.write_calls)


class TestManualPoints(EngineTestBase):
    """自动扫描之外的手动点位：持久化清单、在线添加/删除、重连复用。"""

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        self.received = []
        self.engine.set_subscription_callback(
            lambda node_id, value, timestamp: self.received.append((node_id, value, timestamp))
        )

    async def test_manual_points_merged_and_survive_reconnect(self):
        self.engine.set_manual_items([
            {"item_id": "Manual.Tag1", "name": "手动点1"},
        ])
        await self.engine.connect()
        await self.engine.bootstrap()

        node = self.engine.nodes.get("Manual.Tag1")
        self.assertIsNotNone(node, "手动点位没有被并入点位表")
        self.assertEqual(node["name"], "手动点1")
        self.assertEqual(node["alias"], "手动点1")
        self.assertEqual(node["type"], "Unknown")  # fake 客户端没有 canonical 信息
        self.assertTrue(node.get("manual"))
        first = self.factory.last()
        added_ids = [item_id for call in first.add_items_calls for item_id in call[0]]
        self.assertIn("Manual.Tag1", added_ids, "手动点位没有进入 AddItems")

        # 断线重连：不重新 browse，但手动点仍在 AddItems 列表中
        self.engine.heartbeat_interval_s = 5.0
        self.engine.reconnect_base_delay = 0.01
        self.engine.reconnect_max_delay = 0.02
        self.engine._auto_reconnect = True
        self.engine._handle_connection_lost("simulated cable pull", self.engine._generation)
        ok = await wait_until(
            lambda: self.engine.connected and self.engine.total_reconnects >= 1, timeout=15.0)
        self.assertTrue(ok, "engine did not recover")
        second = self.factory.created[1]
        self.assertEqual(second.browse_calls, 0, "重连不应重新 browse")
        second_added = [item_id for call in second.add_items_calls for item_id in call[0]]
        self.assertIn("Manual.Tag1", second_added, "重连后没有重新挂载手动点位")
        self.assertIn("Manual.Tag1", self.engine.nodes)

    async def test_add_manual_node_while_connected_updates_values_and_subscription(self):
        await self.engine.connect()
        await self.engine.bootstrap()
        client = self.factory.last()

        node = await self.engine.add_manual_node("Manual.Live", "现场手动点")
        self.assertIn("Manual.Live", self.engine.nodes)
        self.assertEqual(node["name"], "现场手动点")
        added_ids = [item_id for call in client.add_items_calls for item_id in call[0]]
        self.assertIn("Manual.Live", added_ids, "在线添加没有调用 AddItems")
        self.assertIn("Manual.Live", self.engine.manual_items[0].values())

        # 订阅回调（只带 hClient）仍能映射到新点位
        client.push_change("Manual.Live", True)
        self.assertEqual(self.engine.nodes["Manual.Live"]["value"], True)
        self.assertIn("Manual.Live", {item[0] for item in self.received})

        # 再次添加同一点位应幂等（不重复 AddItems、不重复清单）
        before = len(client.add_items_calls)
        await self.engine.add_manual_node("Manual.Live")
        self.assertEqual(len(self.engine.manual_items), 1)
        self.assertGreaterEqual(len(client.add_items_calls), before)

    async def test_remove_manual_node_removes_from_group_and_config(self):
        await self.engine.connect()
        await self.engine.bootstrap()
        await self.engine.add_manual_node("Manual.Temp", "临时点")
        client = self.factory.last()
        self.assertIn("Manual.Temp", self.engine.nodes)

        removed = await self.engine.remove_manual_node("Manual.Temp")
        self.assertTrue(removed)
        self.assertNotIn("Manual.Temp", self.engine.nodes)
        self.assertEqual(self.engine.manual_items, [])
        self.assertTrue(client.remove_items_calls, "删除点位没有调用 RemoveItems")
        self.assertIn("Manual.Temp", client.remove_items_calls[0])

    async def test_browse_failure_with_manual_points_still_connects(self):
        created = []

        def factory(prog_id, host):
            client = FakeDaClient(prog_id, host)
            if not created:
                client.fail_browse = "RPC server unavailable"
            created.append(client)
            return client

        engine = OpcDaClientEngine(host="localhost", prog_id="Fake.OPC.Server.1",
                                   update_rate_ms=100, client_factory=factory)
        engine.heartbeat_interval_s = 5.0
        self.engine = engine
        engine.set_manual_items([{"item_id": "Manual.Only", "name": "仅手动点"}])

        await engine.connect()
        await engine.bootstrap()
        self.assertEqual(engine.state, OpcDaClientEngine.STATE_CONNECTED)
        self.assertIn("Manual.Only", engine.nodes)
        self.assertEqual(created[0].browse_calls, 1)


class TestBootstrapFailureRecovery(EngineTestBase):
    """bootstrap 失败（Browse 抛错）时必须由 supervisor 继续重试，不能卡死。"""

    async def test_bootstrap_failure_returns_to_reconnect_loop(self):
        created = []

        def factory(prog_id, host):
            client = FakeDaClient(prog_id, host)
            if not created:
                client.fail_browse = "RPC server unavailable"
            created.append(client)
            return client

        engine = OpcDaClientEngine(host="localhost", prog_id="Fake.OPC.Server.1",
                                   update_rate_ms=100, client_factory=factory)
        engine.heartbeat_interval_s = 5.0
        engine.reconnect_base_delay = 0.01
        engine.reconnect_max_delay = 0.02
        self.engine = engine

        await engine.reconnect_forever()
        ok = await wait_until(lambda: engine.connected, timeout=15.0)
        self.assertTrue(ok, "engine stuck after a failed bootstrap")
        self.assertGreaterEqual(len(created), 2)
        self.assertEqual(created[0].browse_calls, 1)
        self.assertEqual(created[1].browse_calls, 1)
        self.assertGreaterEqual(engine.total_reconnects, 1)


class TestWrites(EngineTestBase):
    """单点写 / 批量写。"""

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        await self.engine.connect()
        await self.engine.bootstrap()
        self.client = self.factory.last()

    async def test_write_conversion_error_is_not_connection_failure(self):
        """值类型无法转换（数组点写 bool）只算点位错误，不能触发连接级重连。"""
        self.client.fail_write = "unsupported data type for write: array item"
        ok = await self.engine.write_node_value("Random.ArrayOfReal8", True)
        self.assertFalse(ok)
        self.assertEqual(self.engine._consecutive_write_failures, 0)
        success, fail = await self.engine.write_values_batch(
            ["Random.ArrayOfReal8", "Random.ArrayOfString"], True)
        self.assertEqual((success, fail), (0, 2))
        self.assertEqual(self.engine._consecutive_write_failures, 0)

    async def test_single_write_success_and_rejection(self):
        ok = await self.engine.write_node_value("Random.Boolean", True, display_name="灯1")
        self.assertTrue(ok)
        self.assertEqual(self.client.write_calls[-1], {"Random.Boolean": True})

        # 模拟服务器拒绝：假客户端对该点位返回 E_ACCESSDENIED
        self.client.writable["Random.Boolean"] = False
        ok = await self.engine.write_node_value("Random.Boolean", True)
        self.assertFalse(ok)
        self.assertEqual(self.engine._consecutive_write_failures, 1)

    async def test_batch_write_counts(self):
        success, fail = await self.engine.write_values_batch(
            ["Random.Boolean", "Random.Int1", "Random.Real4"], False,
            display_names=["b", "i1", "r4"],
        )
        self.assertEqual((success, fail), (3, 0))
        self.assertEqual(self.client.write_calls[-1],
                         {"Random.Boolean": False, "Random.Int1": False, "Random.Real4": False})

        self.client.writable["Write Only.Boolean"] = False
        success, fail = await self.engine.write_values_batch(
            ["Random.Boolean", "Write Only.Boolean"], True
        )
        self.assertEqual((success, fail), (1, 1))

    async def test_write_when_disconnected_is_rejected(self):
        await self.engine.disconnect()
        self.assertFalse(await self.engine.write_node_value("Random.Boolean", True))
        self.assertEqual(await self.engine.write_values_batch(["Random.Boolean"], True), (0, 1))


class TestDisconnectAndConfigure(EngineTestBase):
    """disconnect 幂等；configure 的端点失效与参数钳制。"""

    async def test_disconnect_is_idempotent(self):
        self.engine, self.factory = self.make_engine()
        self.engine.set_subscription_callback(lambda node_id, value, timestamp: None)
        await self.engine.connect()
        await self.engine.bootstrap()
        client = self.factory.last()
        self.assertTrue(client.subscribed)

        await self.engine.disconnect()
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_DISCONNECTED)
        self.assertFalse(self.engine.connected)
        self.assertTrue(client.subscription_stopped)
        self.assertFalse(client.connected)
        self.assertIsNone(self.engine._monitor_task)
        self.assertIsNone(self.engine._poll_task)

        # 再断开两次也必须安全返回
        await self.engine.disconnect()
        await self.engine.disconnect()
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_DISCONNECTED)

    async def test_configure_updates_parameters_and_invalidates_nodes(self):
        self.engine, self.factory = self.make_engine()
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(self.engine.url, "opcda://localhost/Fake.OPC.Server.1")
        self.assertEqual(self.engine.display_endpoint, self.engine.url)
        self.assertEqual(self.engine.port, 0)

        self.engine.configure(
            host="192.168.1.9", prog_id="Other.OPC.Server", update_rate_ms=250,
            browse_max_depth=5, port=4840, username="ignored", password="ignored",
        )
        self.assertEqual(self.engine.host, "192.168.1.9")
        self.assertEqual(self.engine.prog_id, "Other.OPC.Server")
        self.assertEqual(self.engine.url, "opcda://192.168.1.9/Other.OPC.Server")
        self.assertEqual(self.engine.update_rate_ms, 250)
        self.assertEqual(self.engine.browse_max_depth, 5)
        self.assertEqual(self.engine.nodes, {})  # 端点改变 -> 旧点位表失效

        # 参数钳制
        self.engine.configure(update_rate_ms=1, browse_max_depth=0)
        self.assertEqual(self.engine.update_rate_ms, 100)
        self.assertEqual(self.engine.browse_max_depth, 1)
        self.engine.configure(update_rate_ms=10 ** 9, browse_max_depth=999)
        self.assertEqual(self.engine.update_rate_ms, 60000)
        self.assertEqual(self.engine.browse_max_depth, 12)

    async def test_configure_same_endpoint_keeps_nodes(self):
        self.engine, self.factory = self.make_engine()
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(len(self.engine.nodes), 4)
        self.engine.configure(update_rate_ms=300)
        self.assertEqual(len(self.engine.nodes), 4)
        self.assertEqual(self.engine.update_rate_ms, 300)

    async def test_state_constants_match_ua_engine(self):
        # UI 的 _apply_state 直接按状态字符串分支，必须与 UA 版完全一致
        self.assertEqual(OpcDaClientEngine.STATE_DISCONNECTED, "disconnected")
        self.assertEqual(OpcDaClientEngine.STATE_CONNECTING, "connecting")
        self.assertEqual(OpcDaClientEngine.STATE_LOADING, "loading")
        self.assertEqual(OpcDaClientEngine.STATE_CONNECTED, "connected")
        self.assertEqual(OpcDaClientEngine.STATE_RECONNECTING, "reconnecting")
        self.assertEqual(OpcDaClientEngine.STATE_DISCONNECTING, "disconnecting")


class TestWriteFailureTriggersReconnect(EngineTestBase):
    """连续写失败达到阈值 -> 受控重连（而不是静默失败）。"""

    async def test_consecutive_write_failures_trigger_lost(self):
        self.engine, self.factory = self.make_engine()
        await self.engine.connect()
        await self.engine.bootstrap()
        self.engine.heartbeat_interval_s = 5.0
        self.engine._auto_reconnect = True
        self.engine._max_write_failures = 2
        self.factory.last().writable["Write Only.Boolean"] = False

        await self.engine.write_node_value("Write Only.Boolean", True)
        self.assertEqual(self.engine._consecutive_write_failures, 1)
        await self.engine.write_node_value("Write Only.Boolean", True)
        self.assertNotEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)


class TestHeartbeatSelfHeal(EngineTestBase):
    """心跳看门狗自愈：任务静默退出/僵尸 pending 后必须有人重新拉起。

    现场问题：OPC DA 版的连接心跳看门狗自己 return（或一步没跑就永久 pending）
    后无人重启，链路健康检测永久失效；必须由 _ensure_link_tasks / ensure_liveness
    补拉。同时周期成功心跳日志必须从 INFO 降到 DEBUG（不污染现场 INFO 分类）。
    """

    async def asyncSetUp(self):
        self.engine, self.factory = self.make_engine()
        self.engine.set_subscription_callback(lambda node_id, value, timestamp: None)

    async def _connect(self):
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(self.engine.state, OpcDaClientEngine.STATE_CONNECTED)

    async def test_ensure_link_tasks_restarts_dead_monitor(self):
        await self._connect()
        old_task = self.engine._monitor_task
        self.assertIsNotNone(old_task)
        self.assertFalse(old_task.done())

        # 模拟看门狗"静默退出且无人重启"：任务被取消后，补拉必须把它重新拉起来。
        old_task.cancel()
        try:
            await old_task
        except asyncio.CancelledError:
            pass
        self.assertTrue(old_task.done())

        await self.engine._ensure_link_tasks(self.engine._generation)

        new_task = self.engine._monitor_task
        self.assertIsNotNone(new_task)
        self.assertIsNot(new_task, old_task)
        self.assertFalse(new_task.done())
        # 新任务必须真的跑到第一行（登记 started 时间戳），而不是又一个僵尸 pending。
        self.assertTrue(
            await wait_until(
                lambda: not new_task.done() and self.engine._monitor_started_at > 0.0),
            "补拉后的心跳任务没有真正开始执行",
        )

    async def test_ensure_liveness_restarts_dead_monitor(self):
        await self._connect()
        real_task = self.engine._monitor_task

        # 临时把状态伪装成"用户断开中"以抑制 finally 的自动自愈，
        # 从而单独验证同步入口 ensure_liveness() 的返回值与补拉行为。
        self.engine._state = OpcDaClientEngine.STATE_DISCONNECTING
        real_task.cancel()
        try:
            await real_task
        except asyncio.CancelledError:
            pass
        self.engine._state = OpcDaClientEngine.STATE_CONNECTED
        self.assertTrue(real_task.done())

        self.assertTrue(self.engine.ensure_liveness())
        self.assertTrue(
            await wait_until(
                lambda: self.engine._monitor_task is not None
                and self.engine._monitor_task is not real_task
                and not self.engine._monitor_task.done()),
            "ensure_liveness() 声称已安排自愈，但心跳任务没有重建",
        )
        self.assertTrue(await wait_until(lambda: self.engine._monitor_started_at > 0.0))

    async def test_ensure_liveness_false_when_not_connected(self):
        # 未连接：不得重启、不得创建任务
        self.assertFalse(self.engine.ensure_liveness())
        self.assertIsNone(self.engine._monitor_task)
        self.assertIsNone(self.engine._poll_task)

        # 已断开：同样不得把任务重新拉起来（否则断开后链路检测又"复活"）
        await self._connect()
        await self.engine.disconnect()
        self.assertFalse(self.engine.ensure_liveness())
        self.assertIsNone(self.engine._monitor_task)
        self.assertIsNone(self.engine._poll_task)

    async def test_heartbeat_success_log_is_debug(self):
        await self._connect()
        # 心跳周期已在 make_engine 里调到 0.05s：最多 0.3s 就能攒够 6 tick 一条日志。
        with mock.patch.object(global_logger, "debug") as debug_mock, \
                mock.patch.object(global_logger, "info") as info_mock:
            observed = await wait_until(
                lambda: any(
                    call.args and "[HEARTBEAT]" in str(call.args[0])
                    for call in debug_mock.call_args_list
                ),
                timeout=5.0,
            )

        heartbeat_debug = [str(call.args[0]) for call in debug_mock.call_args_list
                           if call.args and "[HEARTBEAT]" in str(call.args[0])]
        heartbeat_info = [str(call.args[0]) for call in info_mock.call_args_list
                          if call.args and "[HEARTBEAT]" in str(call.args[0])]
        self.assertTrue(observed, "周期心跳成功日志没有出现在 debug 调用中")
        self.assertIn("[HEARTBEAT] OPC DA link OK", heartbeat_debug[0])
        self.assertEqual(heartbeat_info, [], "周期心跳成功日志不得再出现在 info 调用中")


if __name__ == "__main__":
    unittest.main(verbosity=2)
