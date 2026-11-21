"""DA 设置页"手动添加控制点位"UI 回归测试（无 COM / 无真实服务器）。"""
import asyncio
import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ["QT_QPA_PLATFORM"] = "offscreen"

try:
    from PyQt5.QtWidgets import QApplication
    QT_AVAILABLE = True
except ImportError:
    try:
        from PySide6.QtWidgets import QApplication
        QT_AVAILABLE = True
    except ImportError:
        QT_AVAILABLE = False

from opcda.engine import OpcDaClientEngine
from tests.test_da_engine import FakeDaClient


def _write_config(cfgdir, auto_reconnect=False, manual_nodes=None):
    cfg = {
        "aliases": {}, "groups": [], "schedules": [],
        "auto_connect": False, "auto_reconnect": auto_reconnect,
        "da_connection": {"host": "localhost", "prog_id": "Fake.OPC.Server.1",
                          "update_rate_ms": 100, "browse_max_depth": 3},
        "da_manual_nodes": manual_nodes or [],
    }
    os.makedirs(os.path.join(cfgdir, "data"), exist_ok=True)
    with open(os.path.join(cfgdir, "data", "lighting_config.json"), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)


@unittest.skipUnless(QT_AVAILABLE, "PyQt5/PySide6 不可用")
class TestDaManualPointUi(unittest.IsolatedAsyncioTestCase):
    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    async def asyncSetUp(self):
        self.cfgdir = tempfile.mkdtemp(prefix="lc_da_manual_")
        self._patch = patch("utils.persistence._get_base_dir", return_value=self.cfgdir)
        self._patch.start()
        self.engine = None
        self.tab = None

    async def asyncTearDown(self):
        try:
            if self.engine is not None:
                self.engine.stop_reconnect()
                await self.engine.disconnect()
        finally:
            if self.tab is not None:
                self.tab.deleteLater()
                self.app.processEvents()
            self._patch.stop()
            shutil.rmtree(self.cfgdir, ignore_errors=True)

    def _build_tab(self, manual_nodes=None):
        _write_config(self.cfgdir, auto_reconnect=False, manual_nodes=manual_nodes)
        from utils.persistence import PersistenceManager
        from core.data_manager import DataManager
        from ui.tabs.tab_settings_da import DaTabSettings
        pm = PersistenceManager()
        dm = DataManager(pm)
        self.engine = OpcDaClientEngine(host="localhost", prog_id="Fake.OPC.Server.1",
                                       update_rate_ms=100, browse_max_depth=3)
        self.engine.heartbeat_interval_s = 5.0

        def factory(prog_id, host):
            return FakeDaClient(prog_id, host)
        self.engine._client_factory = factory
        self.tab = DaTabSettings(self.engine, dm)
        self.dm = dm
        return self.tab

    async def test_saved_manual_entries_are_loaded_into_engine_and_ui(self):
        tab = self._build_tab(manual_nodes=[{"item_id": "Manual.Saved", "name": "已保存点"}])
        self.assertEqual(len(tab._manual_entries), 1)
        self.assertEqual(tab.list_manual_points.count(), 1)
        self.assertIn("Manual.Saved", [entry["item_id"] for entry in self.engine.manual_items])
        # 未连接时新增：只保存配置，不联网
        tab.le_manual_item_id.setText("Manual.Offline")
        tab.le_manual_name.setText("离线点")
        tab._on_add_manual_point()
        await asyncio.sleep(0)
        ids = [entry["item_id"] for entry in tab._manual_entries]
        self.assertIn("Manual.Offline", ids)
        saved = self.dm.pm.data_store.get("da_manual_nodes")
        self.assertEqual(len(saved), 2)
        self.assertIn("Manual.Offline", [entry["item_id"] for entry in saved])
        self.assertEqual(tab.list_manual_points.count(), 2)

    async def test_add_manual_point_while_connected_publishes_to_data_bus(self):
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()
        self.assertTrue(self.engine.connected)

        tab.le_manual_item_id.setText("Manual.Live")
        tab.le_manual_name.setText("在线点")
        tab._on_add_manual_point()
        # _on_add_manual_point 创建 asyncio task，让它完成
        await asyncio.sleep(0.1)
        self.assertIn("Manual.Live", self.engine.nodes)
        dm_nodes = [node["node_id"] for node in self.dm.get_node_list()]
        self.assertIn("Manual.Live", dm_nodes, "在线添加后没有发布到 DataManager")
        self.assertIn("Manual.Live", [entry["item_id"] for entry in tab._manual_entries])

    async def test_heartbeat_status_label_and_policy_log(self):
        """心跳成功日志降为 DEBUG 后，界面必须能证明心跳检测仍在运行。"""
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()

        # 新鲜心跳：链路状态显示"心跳正常（Xs 前）"
        self.engine._last_heartbeat_at = time.monotonic()
        tab.update_heartbeat_status()
        text = tab.lbl_conn_status.text()
        self.assertIn("心跳", text)
        self.assertIn("正常", text)

        # 心跳长时间未更新：状态栏必须给出告警（引擎会据此触发自动重连）
        self.engine._last_heartbeat_at = time.monotonic() - 9999
        tab.update_heartbeat_status()
        self.assertIn("未更新", tab.lbl_conn_status.text())

        # 连接成功时输出一行心跳策略说明（INFO，只一次，不是周期刷屏）
        with self.assertLogs("LightingControl", level="INFO") as captured:
            tab._log_heartbeat_policy()
        joined = "\n".join(captured.output)
        self.assertIn("心跳", joined)
        self.assertIn("DEBUG", joined)

    async def test_manual_add_keeps_imported_aliases(self):
        """回归：在线手动添加点位不得把已导入的别名冲掉。

        现场问题：添加手动点位后 _publish_nodes_to_bus() 会重新发布引擎点位表，
        旧实现把每个点位自带的默认 alias（原始 ItemID）写回 DataManager，
        用户导入的别名全部消失，只能重新导入。
        """
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()

        tab.le_manual_item_id.setText("Manual.First")
        tab._on_add_manual_point()
        await asyncio.sleep(0.1)
        self.assertIn("Manual.First", self.dm.nodes)

        self.dm.set_alias("Manual.First", "一号灯")
        self.assertEqual(self.dm.get_alias_by_node_id("Manual.First"), "一号灯")

        # 再加一个点位：内部会再次发布全量点位表
        tab.le_manual_item_id.setText("Manual.Second")
        tab._on_add_manual_point()
        await asyncio.sleep(0.1)

        self.assertIn("Manual.Second", self.dm.nodes)
        self.assertEqual(
            self.dm.get_alias_by_node_id("Manual.First"), "一号灯",
            "手动加点重新发布点位表后别名被覆盖了",
        )
        self.assertEqual(
            self.dm.pm.get_all_aliases().get("Manual.First"), "一号灯",
            "别名持久化记录被覆盖了",
        )

    async def test_remove_manual_point_removes_from_engine_and_data_bus(self):
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()
        tab.le_manual_item_id.setText("Manual.Del")
        tab._on_add_manual_point()
        await asyncio.sleep(0.1)
        self.assertIn("Manual.Del", self.engine.nodes)

        # 选中第 1 行并删除
        tab.list_manual_points.setCurrentRow(0)
        tab._on_remove_manual_point()
        await asyncio.sleep(0.1)
        self.assertNotIn("Manual.Del", self.engine.nodes)
        dm_ids = [node["node_id"] for node in self.dm.get_node_list()]
        self.assertNotIn("Manual.Del", dm_ids)
        saved = self.dm.pm.data_store.get("da_manual_nodes")
        self.assertEqual(saved, [])


if __name__ == "__main__":
    unittest.main()
