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
        # 在线挂载任务进行中：两个按钮必须禁用，防快速连击并发创建多个 live 任务
        self.assertFalse(tab.btn_manual_add.isEnabled())
        self.assertFalse(tab.btn_manual_remove.isEnabled())
        # _on_add_manual_point 创建 asyncio task，让它完成
        await asyncio.sleep(0.1)
        # 任务结束后恢复可用
        self.assertTrue(tab.btn_manual_add.isEnabled())
        self.assertTrue(tab.btn_manual_remove.isEnabled())
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

    async def test_update_rate_change_applies_immediately_when_connected(self):
        """已连接时修改「更新周期」必须热生效并落盘，不能等重启。"""
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()

        tab.sb_update_rate.setValue(1500)
        self.assertEqual(self.engine.update_rate_ms, 1500)
        self.assertEqual(
            self.dm.pm.data_store.get("da_connection", {}).get("update_rate_ms"), 1500,
            "热更新后没有写回 da_connection 配置",
        )
        self.assertIn("更新周期已应用 1500ms", tab.lbl_conn_status.text())

    async def test_cancel_progress_keeps_ui_disconnected(self):
        """点 QProgressDialog 的「取消」必须真的取消连接，且界面不得闪成已连接。"""
        tab = self._build_tab()
        started = asyncio.Event()

        async def _slow_connect():
            started.set()
            await asyncio.sleep(0.05)

        # 用可控的慢连接替换引擎连接，模拟用户点击「取消」
        self.engine.connect = _slow_connect
        tab._connect_task = asyncio.create_task(tab._connect_and_load())
        await started.wait()
        self.assertFalse(tab._connect_task.done())

        tab._on_connect_progress_canceled()
        await asyncio.sleep(0.15)

        # 连接流程内部会捕获 CancelledError 做收尾，因此以"任务已结束 + 引擎已断开"为准
        self.assertTrue(tab._connect_task.done(), "取消后连接任务应已结束")
        self.assertTrue(tab._connect_cancelled, "取消标记应保持为 True")
        self.assertFalse(self.engine.connected, "取消后引擎不应停留在已连接状态")
        self.assertNotIn("已连接", tab.lbl_conn_status.text())
        self.assertEqual(tab.btn_connect.text(), "连接并挂载服务器节点")

    async def test_empty_node_tree_hint_is_protocol_neutral(self):
        """DA 空点位表是正常状态：提示不能写死 UA 的命名空间前缀。"""
        tab = self._build_tab()
        with self.assertLogs("LightingControl", level="INFO") as captured:
            published = tab._publish_nodes_to_bus()
        self.assertEqual(published, 0)
        joined = "\n".join(captured.output)
        self.assertIn("当前没有点位", joined)
        self.assertNotIn("命名空间前缀", joined)

    def test_server_worker_is_retired_from_keepalive_set(self):
        """枚举线程不能挂 parent，结束后必须从保活集合移除并 deleteLater。"""
        from ui.tabs.tab_settings_da import _ServerListWorker, _RUNNING_WORKERS
        tab = self._build_tab()
        worker = _ServerListWorker("localhost")
        try:
            self.assertIsNone(
                worker.parent(), "枚举线程挂 parent 后关窗会销毁运行中线程并崩溃")
        finally:
            worker.deleteLater()

        class _FakeWorker:
            def __init__(self):
                self.deleted = False

            def deleteLater(self):
                self.deleted = True

        fake = _FakeWorker()
        tab._server_worker = fake
        _RUNNING_WORKERS.add(fake)
        tab._on_server_worker_finished()
        self.assertNotIn(fake, _RUNNING_WORKERS)
        self.assertTrue(fake.deleted)
        self.assertIsNone(tab._server_worker)
        self.assertTrue(tab.btn_refresh_servers.isEnabled())

    async def test_subscription_quality_synced_to_data_manager(self):
        """实时推送必须把 quality 同步到 DataManager（通信异常可动态出现/恢复）。"""
        tab = self._build_tab()
        await self.engine.connect()
        await self.engine.bootstrap()
        tab.le_manual_item_id.setText("Manual.Quality")
        tab._on_add_manual_point()
        await asyncio.sleep(0.1)
        self.assertIn("Manual.Quality", self.dm.nodes)

        tab._on_sub_data("Manual.Quality", 1, "2026-01-01 00:00:01", 0x00)
        self.assertEqual(self.dm.nodes["Manual.Quality"]["quality"], 0x00)
        tab._on_sub_data("Manual.Quality", 1, "2026-01-01 00:00:02", 0xC0)
        self.assertEqual(self.dm.nodes["Manual.Quality"]["quality"], 0xC0)

    def test_server_worker_daemon_thread_emits_and_cleans_up(self):
        """枚举任务必须是 daemon 线程：能回传结果、结束后从保活集合移除。"""
        import time
        from unittest.mock import patch
        from PyQt5.QtWidgets import QApplication
        from ui.tabs.tab_settings_da import _ServerListWorker, _RUNNING_WORKERS

        self._build_tab()
        results = []
        worker = _ServerListWorker("localhost")
        worker.finished_list.connect(lambda servers, err: results.append((servers, err)))
        with patch("opcda.client.list_servers", return_value=["Matrikon.OPC.Simulation.1"]):
            worker.start()
        app = QApplication.instance()
        for _ in range(300):
            app.processEvents()
            if results and not worker.isRunning():
                break
            time.sleep(0.01)
        app.processEvents()
        self.assertTrue(results, "daemon 枚举线程没有回传结果")
        self.assertEqual(results[0][0], ["Matrikon.OPC.Simulation.1"])
        self.assertFalse(worker.isRunning())
        self.assertNotIn(worker, _RUNNING_WORKERS)

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


@unittest.skipUnless(QT_AVAILABLE, "PyQt5/PySide6 不可用")
class TestDaFilterHelpers(unittest.TestCase):
    """ProgID 与类型筛选的纯逻辑回归（不依赖 COM / 真实服务器）。"""

    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def test_progid_pattern_accepts_spaces_and_clsid(self):
        from ui.tabs.tab_settings_da import _PROGID_PATTERN
        accepted = [
            "Matrikon.OPC.Simulation.1",
            "Kepware.KEPServerEX.V6",
            "My Server.Group.1",
            "Rockwell Automation.Tag.1",
            "{12345678-1234-1234-1234-1234567890AB}",
        ]
        for name in accepted:
            self.assertIsNotNone(_PROGID_PATTERN.match(name), "应接受 ProgID/CLSID: {}".format(name))
        rejected = ["", "NoDotHere", "{not-a-clsid}", "Bad/Chars.1"]
        for name in rejected:
            self.assertIsNone(_PROGID_PATTERN.match(name), "应拒绝非法 ProgID: {}".format(name))

    def test_type_matches_real_int_semantics(self):
        from utils.filter_helper import type_matches
        # DA 类型名 Float/Double 必须能被 Real 选中
        self.assertTrue(type_matches("Float", "Real"))
        self.assertTrue(type_matches("Double", "Real"))
        # Int 选中 Int16，但绝不能误匹配 UInt16
        self.assertTrue(type_matches("Int16", "Int"))
        self.assertFalse(type_matches("UInt16", "Int"))
        self.assertTrue(type_matches("UInt16", "UInt"))
        self.assertTrue(type_matches("Boolean", "Boolean"))
        self.assertFalse(type_matches("Float", "Boolean"))
        self.assertTrue(type_matches("Anything", "全部类型"))

    def test_group_search_proxy_uses_type_semantics(self):
        """分组页代理模型必须按 TYPE_MAP 语义筛选类型，而不是子串匹配。"""
        from ui.tabs.tab_group import SearchProxyModel

        class _FakeModel:
            def __init__(self, rows):
                self.rows = rows

            def index(self, row, col, parent=None):
                return (row, col)

            def data(self, idx):
                return self.rows[idx[0]][idx[1]]

        rows = [
            ["Float 点", "1", "ns=2;s=FloatTag", "Float"],
            ["Double 点", "2", "ns=2;s=DoubleTag", "Double"],
            ["Int16 点", "3", "ns=2;s=IntTag", "Int16"],
            ["UInt16 点", "4", "ns=2;s=UIntTag", "UInt16"],
        ]
        fake = _FakeModel(rows)
        proxy = SearchProxyModel()
        # 直接替换 sourceModel() 返回值，绕开 Qt 源模型，单测行过滤逻辑本身
        proxy.sourceModel = lambda: fake

        proxy.set_filter("", "Real")
        self.assertEqual(
            [i for i in range(len(rows)) if proxy.filterAcceptsRow(i, None)], [0, 1])
        proxy.set_filter("", "Int")
        self.assertEqual(
            [i for i in range(len(rows)) if proxy.filterAcceptsRow(i, None)], [2])
        proxy.set_filter("", "全部类型")
        self.assertEqual(
            [i for i in range(len(rows)) if proxy.filterAcceptsRow(i, None)], [0, 1, 2, 3])


if __name__ == "__main__":
    unittest.main()
