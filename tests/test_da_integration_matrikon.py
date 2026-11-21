"""Matrikon OPC Simulation Server 集成测试（真实 COM/DCOM 直连）。

只有本机安装了 Matrikon OPC Simulation Server（ProgID Matrikon.OPC.Simulation.1）
时才会执行，否则整类 skip；不影响无 COM 环境的 CI。

运行：
    python -m unittest tests.test_da_integration_matrikon -v

可选环境变量：
    LC_DA_HOST       DCOM 主机（默认 localhost）
    LC_DA_PROGID     OPC 服务器 ProgID（默认 Matrikon.OPC.Simulation.1）
"""
import asyncio
import os
import sys
import time
import unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

HOST = os.environ.get("LC_DA_HOST", "localhost")
PROGID = os.environ.get("LC_DA_PROGID", "Matrikon.OPC.Simulation.1")

WINDOWS = sys.platform == "win32"
try:
    import comtypes  # noqa: F401
    COMTYPES_AVAILABLE = True
except Exception:
    COMTYPES_AVAILABLE = False

if WINDOWS and COMTYPES_AVAILABLE:
    from opcda.client import OpcDaClient
    from opcda.engine import OpcDaClientEngine
else:  # pragma: no cover
    OpcDaClient = None
    OpcDaClientEngine = None

# 测试用可读/可写点位（Matrikon 默认地址空间）。
# 选 Bucket Brigade 而不是 Random：某些部署里 Random 分支可能处于 OutOfService，
# 而 Bucket Brigade 是持续变化的读写仿真点，最适合验证订阅与写回读。
READ_TAGS = ["Bucket Brigade.Boolean", "Bucket Brigade.Int1", "Bucket Brigade.Real4"]
RW_TAG = "Bucket Brigade.Boolean"


def _server_reachable():
    if not (WINDOWS and COMTYPES_AVAILABLE):
        return False, "非 Windows 或无 comtypes"
    try:
        client = OpcDaClient(PROGID, host=HOST)
        client.connect(timeout=8.0)
        try:
            # 已安装但模拟引擎处于 OutOfService/Failed 时，测试没有意义（读到的点位
            # 全是 Bad 质量），跳过而不是误报客户端失败。
            client.add_items([RW_TAG], update_rate_ms=200)
            values = client.read_items([RW_TAG], source=1)
            if values and (int(values[0].quality) & 0xC0) != 0xC0:
                return False, "服务器模拟引擎不在服务状态（quality=0x{:04X}）".format(
                    int(values[0].quality))
        finally:
            client.disconnect()
        return True, ""
    except Exception as exc:
        return False, "{}: {}".format(type(exc).__name__, exc)


@unittest.skipUnless(WINDOWS and COMTYPES_AVAILABLE, "需要 Windows + comtypes")
class TestOpcDaClientAgainstMatrikon(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ok, reason = _server_reachable()
        if not ok:
            raise unittest.SkipTest("OPC DA 服务器不可用（{}）: {}".format(PROGID, reason))

    def setUp(self):
        self.client = OpcDaClient(PROGID, host=HOST)
        self.client.connect(timeout=10.0)

    def tearDown(self):
        try:
            self.client.disconnect()
        except Exception:
            pass

    def test_server_status(self):
        status = self.client.get_server_status()
        self.assertEqual(int(status.get("state", 0)), 1, status)
        self.assertTrue(status.get("vendor"), status)
        self.assertTrue(str(status.get("version") or ""), status)

    def test_browse_address_space(self):
        items = self.client.browse(max_depth=3)
        self.assertGreater(len(items), 10)
        ids = {item.item_id for item in items}
        self.assertIn("Random.Int1", ids)
        self.assertIn(RW_TAG, ids)
        for item in items:
            self.assertTrue(item.item_id)
            self.assertTrue(item.name)

    def test_batch_add_read_write(self):
        added = self.client.add_items(READ_TAGS + [RW_TAG], update_rate_ms=200)
        by_id = {item.item_id: item for item in added}
        self.assertIn(RW_TAG, by_id)

        values = self.client.read_items(READ_TAGS, source=1)
        self.assertEqual(len(values), len(READ_TAGS))
        for value in values:
            self.assertEqual(int(value.error), 0, value)
            self.assertEqual(int(value.quality) & 0xC0, 0xC0, value)  # 0xC0 = OPC Quality Good

        # 写一个真实可读写点位并读回（Bucket Brigade.Boolean 是 RW bool）
        writes = self.client.write_items({RW_TAG: True})
        self.assertEqual(len(writes), 1)
        item_id, hr, text = writes[0]
        self.assertEqual(int(hr), 0, "write failed: {} {}".format(hr, text))
        time.sleep(0.2)
        back = self.client.read_items([RW_TAG], source=2)
        self.assertEqual(len(back), 1)
        self.assertEqual(int(back[0].error), 0, back[0])
        self.assertEqual(bool(back[0].value), True)

    def test_add_and_remove_items_incrementally(self):
        """手动添加点位依赖的增量 AddItems/RemoveItems 必须能在同一 group 上工作。"""
        added = self.client.add_items(["Random.Int1"], update_rate_ms=200)
        self.assertEqual(len(added), 1)
        self.assertIn("Random.Int1", {item.item_id for item in self.client.mounted_items()})

        removed = self.client.remove_items(["Random.Int1"])
        self.assertEqual(len(removed), 1)
        self.assertEqual(int(removed[0][1]), 0, removed[0])
        self.assertNotIn("Random.Int1", {item.item_id for item in self.client.mounted_items()})

        # 移除后重新添加应成功（模拟手动删除后再加回）
        added_again = self.client.add_items(["Random.Int1"], update_rate_ms=200)
        self.assertEqual(len(added_again), 1)

    def test_subscription_delivers_changed_items(self):
        import opcda.client as da_client_module

        self.client.add_items(READ_TAGS, update_rate_ms=200)
        received = []

        def on_change(values):
            received.append(list(values))

        self.client.set_data_change_callback(on_change)
        try:
            self.client.start_subscription(update_rate_ms=200)
            # Matrikon 的 Bucket Brigade 约 1s 变化一次，留 8s 观察窗口并只要求
            # 至少 2 批回调（不同部署的仿真更新周期可能更慢）。
            deadline = time.time() + 8.0
            while time.time() < deadline and len(received) < 2:
                if hasattr(da_client_module, "pump_events"):
                    da_client_module.pump_events()
                else:
                    self.client.pump_events()
                time.sleep(0.02)
        finally:
            self.client.stop_subscription()
            self.client.set_data_change_callback(None)

        self.assertGreaterEqual(len(received), 2, "订阅 8 秒内收到回调次数不足")
        seen_ids = set()
        for batch in received:
            for value in batch:
                seen_ids.add(value.item_id)
        self.assertTrue(seen_ids & set(READ_TAGS), "回调没有映射回 ItemID: {}".format(seen_ids))


@unittest.skipUnless(WINDOWS and COMTYPES_AVAILABLE, "需要 Windows + comtypes")
class TestOpcDaEngineAgainstMatrikon(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        ok, reason = _server_reachable()
        if not ok:
            raise unittest.SkipTest("OPC DA 服务器不可用（{}）: {}".format(PROGID, reason))

    async def asyncSetUp(self):
        self.engine = OpcDaClientEngine(host=HOST, prog_id=PROGID,
                                        update_rate_ms=200, browse_max_depth=3)
        # 无 Qt 环境：由引擎心跳主动泵 COM 消息（Qt 版会关闭该开关）
        self.engine.pump_events_enabled = True
        self.engine.heartbeat_interval_s = 0.1
        self.subscribed = []
        self.transitions = []
        self.engine.on_state_change = lambda state, detail: self.transitions.append(state)
        self.engine.set_subscription_callback(
            lambda node_id, value, ts: self.subscribed.append((node_id, value, ts))
        )

    async def asyncTearDown(self):
        try:
            await self.engine.disconnect()
        except Exception:
            pass

    async def test_engine_connect_browse_subscribe_write(self):
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(self.engine.state, "connected")
        self.assertGreater(len(self.engine.nodes), 10)
        self.assertIn(RW_TAG, self.engine.nodes)

        # 等订阅回调 / 心跳泵运行
        for _ in range(30):
            await asyncio.sleep(0.1)
            if self.subscribed:
                break
        self.assertTrue(self.subscribed, "引擎订阅没有收到任何推送")

        ok = await self.engine.write_node_value(RW_TAG, True)
        self.assertTrue(ok, "引擎写点位失败")
        await asyncio.sleep(0.3)
        status = self.engine.nodes[RW_TAG]
        self.assertEqual(bool(status.get("value")), True)

        await self.engine.disconnect()
        self.assertEqual(self.engine.state, "disconnected")

    async def test_engine_manual_add_and_remove_point(self):
        """引擎手动添加点位：AddItems、初值、订阅映射、删除（RemoveItems）全链路。"""
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertEqual(self.engine.state, "connected")

        node = await self.engine.add_manual_node(RW_TAG, "手动布尔")
        self.assertIn(RW_TAG, self.engine.nodes)
        self.assertEqual(node["name"], "手动布尔")
        self.assertIn(RW_TAG, [entry["item_id"] for entry in self.engine.manual_items])

        ok = await self.engine.write_node_value(RW_TAG, True)
        self.assertTrue(ok)
        await asyncio.sleep(0.3)
        self.assertTrue(bool(self.engine.nodes[RW_TAG].get("value")))

        removed = await self.engine.remove_manual_node(RW_TAG)
        self.assertTrue(removed)
        self.assertNotIn(RW_TAG, self.engine.nodes)
        self.assertNotIn(RW_TAG, [entry["item_id"] for entry in self.engine.manual_items])

        await self.engine.disconnect()
        self.assertEqual(self.engine.state, "disconnected")


if __name__ == "__main__":
    unittest.main()
