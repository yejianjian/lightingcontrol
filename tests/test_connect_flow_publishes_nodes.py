"""连接流程回归测试：点位必须真的出现在数据总线上。

背景：重构连接流程时，`bootstrap()` 之后的"把节点灌进 DataManager"这一步被漏掉了，
表现为**连上了、系统状态显示已连接，但总点位/当前开启全是 0**，所有列表空着。
这是纯功能回归，只有把 UI 与引擎拼起来测才能发现。
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

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

from opc.client_engine import OpcClientEngine


def _node(node_id, name, value, vtype="Boolean"):
    return {
        "name": name,
        "node_id": node_id,
        "type": vtype,
        "value": value,
        "timestamp": "2026-01-01 00:00:00",
        "node_obj": object(),   # 底层 asyncua 引用，不应进入数据总线
    }


@unittest.skipUnless(QT_AVAILABLE, "PyQt5/PySide6 不可用")
class TestNodesReachDataBus(unittest.TestCase):
    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="lc_bus_")
        self._patch = patch("utils.persistence._get_base_dir", return_value=self._tmp.name)
        self._patch.start()

        from core.data_manager import DataManager
        from utils.persistence import PersistenceManager
        from ui.tabs.tab_settings import TabSettings

        self.pm = PersistenceManager()
        self.dm = DataManager(self.pm)
        self.engine = OpcClientEngine(host="127.0.0.1", port=4840)
        self.tab = TabSettings(self.engine, self.dm)

    def tearDown(self):
        try:
            self.tab.hide()
            self.tab.deleteLater()
            self.app.processEvents()
        finally:
            self._patch.stop()
            self._tmp.cleanup()

    def test_engine_nodes_are_published_to_the_data_bus(self):
        self.engine.nodes = {
            "ns=2;s=Light_1": _node("ns=2;s=Light_1", "灯1", True),
            "ns=2;s=Light_2": _node("ns=2;s=Light_2", "灯2", False),
            "ns=2;s=Power_1": _node("ns=2;s=Power_1", "功率1", 220.5, "Double"),
        }

        published = self.tab._publish_nodes_to_bus()

        self.assertEqual(published, 3)
        self.assertEqual(len(self.dm.nodes), 3, "点位没有进入数据总线，仪表盘会是 0")
        # 底层 asyncua 引用绝不能进入数据总线（否则 GC 无法回收）
        for node in self.dm.nodes.values():
            self.assertNotIn("node_obj", node)
        # 开关量计数：True=1 个开启，False=1 个关闭，220.5 属于模拟量不得计入
        self.assertEqual(self.dm.on_count, 1)
        self.assertEqual(self.dm.off_count, 1)
        self.assertTrue(self.dm.structure_changed)

    def test_empty_node_tree_warns_and_returns_zero(self):
        self.engine.nodes = {}
        with self.assertLogs("LightingControl", level="WARNING") as captured:
            published = self.tab._publish_nodes_to_bus()
        self.assertEqual(published, 0)
        self.assertEqual(self.dm.nodes, {})
        self.assertTrue(any("empty" in line for line in captured.output),
                        f"空点位表应当给出可诊断的告警: {captured.output}")

    def test_reconnect_republishes_nodes_with_fresh_values(self):
        """重连后（点位表复用 + 值已刷新）必须重新发布，UI 才能自愈。"""
        self.engine.nodes = {"ns=2;s=Light_1": _node("ns=2;s=Light_1", "灯1", False)}
        self.tab._publish_nodes_to_bus()
        self.assertEqual(self.dm.off_count, 1)

        # 模拟断线期间现场把灯打开了，重连后 bootstrap 重新读到了 True
        self.engine.nodes["ns=2;s=Light_1"]["value"] = True
        self.tab._publish_nodes_to_bus()

        self.assertEqual(self.dm.nodes["ns=2;s=Light_1"]["value"], True)
        self.assertEqual(self.dm.on_count, 1)
        self.assertEqual(self.dm.off_count, 0)

    def test_reconnect_policy_widgets_do_not_save_on_construction(self):
        """构造设置页时的 setValue 不得被当成用户修改（否则启动日志被刷屏）。"""
        saved = self.pm.data_store
        before = (saved.get("reconnect_base_delay"), saved.get("reconnect_max_delay"),
                  saved.get("session_timeout_ms"))
        # 立刻重建一个设置页，再次触发所有 setValue
        from ui.tabs.tab_settings import TabSettings
        tab2 = TabSettings(self.engine, self.dm)
        try:
            after = (saved.get("reconnect_base_delay"), saved.get("reconnect_max_delay"),
                     saved.get("session_timeout_ms"))
            self.assertEqual(before, after,
                             "构造设置页不应改写并落盘重连策略")
        finally:
            tab2.hide()
            tab2.deleteLater()


if __name__ == "__main__":
    unittest.main()
