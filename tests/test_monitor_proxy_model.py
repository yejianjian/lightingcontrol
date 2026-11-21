import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtCore import QModelIndex, Qt
    from PyQt5.QtGui import QColor
except ImportError:
    from PySide6.QtWidgets import QApplication
    from PySide6.QtCore import QModelIndex, Qt
    from PySide6.QtGui import QColor

from core.data_manager import DataManager
from utils.persistence import PersistenceManager
from ui.tabs.tab_monitor import MonitorTableModel, MonitorSearchProxyModel
from ui.components.action_delegate import _get_node_from_index, _is_node_writable

class TestMonitorProxyModel(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication(sys.argv)

    def test_proxy_model_and_delegate(self):
        test_file = "tmp_proxy_test.json"
        if os.path.exists(test_file):
            os.remove(test_file)

        pm = PersistenceManager(test_file)
        dm = DataManager(pm)

        # 添加模拟数据点位
        dm.update_node("ns=2;s=Light_Hall", {"name": "Light_Hall", "alias": "大厅主灯", "type": "Boolean", "value": True})
        dm.update_node("ns=2;s=Light_Corridor", {"name": "Light_Corridor", "alias": "走廊筒灯", "type": "Boolean", "value": False})
        dm.update_node("ns=2;s=Meter_Power", {"name": "Meter_Power", "alias": "电表功率", "type": "Real", "value": 220.5})
        dm.update_node("ns=2;s=Device_Count", {"name": "Device_Count", "alias": "设备计数", "type": "Int", "value": 15})
        dm.update_node("ns=2;s=Device_UInt", {"name": "Device_UInt", "alias": "状态寄存器", "type": "UInt", "value": 3})

        table_model = MonitorTableModel(dm)
        table_model.refresh_data()
        self.assertEqual(table_model.rowCount(), 5)

        proxy_model = MonitorSearchProxyModel()
        proxy_model.setSourceModel(table_model)
        self.assertEqual(proxy_model.rowCount(), 5)

        # 1. 测试关键词搜索
        proxy_model.set_filter("大厅", "全部数据类型")
        self.assertEqual(proxy_model.rowCount(), 1)
        node_idx = proxy_model.index(0, 0)
        node_data = _get_node_from_index(node_idx)
        self.assertEqual(node_data["alias"], "大厅主灯")

        # 2. 测试 Node ID 模糊搜索 (忽略大小写)
        proxy_model.set_filter("corridor", "全部数据类型")
        self.assertEqual(proxy_model.rowCount(), 1)
        node_data = _get_node_from_index(proxy_model.index(0, 0))
        self.assertEqual(node_data["node_id"], "ns=2;s=Light_Corridor")

        # 3. 测试类型过滤
        proxy_model.set_filter("", "Boolean")
        self.assertEqual(proxy_model.rowCount(), 2)

        proxy_model.set_filter("", "Real")
        self.assertEqual(proxy_model.rowCount(), 1)

        proxy_model.set_filter("", "Int")
        self.assertEqual(proxy_model.rowCount(), 1)  # Device_Count (Int)

        proxy_model.set_filter("", "UInt")
        self.assertEqual(proxy_model.rowCount(), 1)  # Device_UInt (UInt)

        # 4. 测试增量更新 (不调用 beginResetModel 的局部刷新)
        dm.update_node("ns=2;s=Light_Hall", {"value": False})
        table_model.update_nodes(["ns=2;s=Light_Hall"])

        # 验证代理模型依然稳定映射
        proxy_model.set_filter("", "全部数据类型")
        self.assertEqual(proxy_model.rowCount(), 5)

        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

    def test_status_column_shows_bad_quality(self):
        """质量非 Good 时状态列必须显示（通信异常），并保持颜色/布尔格式兼容。"""
        import tempfile
        with tempfile.TemporaryDirectory(prefix="lc_monitor_q_") as tmp:
            pm = PersistenceManager(os.path.join(tmp, "cfg.json"))
            dm = DataManager(pm)
            dm.update_node("ns=2;s=Q_Bad", {"name": "Q_Bad", "alias": "坏质量",
                                            "type": "Real", "value": 12.5, "quality": 0x0018})
            dm.update_node("ns=2;s=Q_Uncertain", {"name": "Q_Uncertain", "alias": "不确定",
                                                  "type": "Real", "value": 3.0, "quality": 0x0040})
            dm.update_node("ns=2;s=Q_None", {"name": "Q_None", "alias": "无值坏质量",
                                             "type": "Real", "value": None, "quality": 0x0000})
            dm.update_node("ns=2;s=Q_Good", {"name": "Q_Good", "alias": "好质量",
                                             "type": "Real", "value": 220.5, "quality": 0x00C0})
            dm.update_node("ns=2;s=Q_Bool", {"name": "Q_Bool", "alias": "布尔坏质量",
                                             "type": "Boolean", "value": True, "quality": 0x0000})

            model = MonitorTableModel(dm)
            model.refresh_data()
            self.assertEqual(model.columnCount(), 6)  # 列数不变
            row_map = {n.get('node_id'): i for i, n in enumerate(model._data_cache)}

            def cell(node_id):
                idx = model.index(row_map[node_id], 3)
                return (model.data(idx, Qt.DisplayRole),
                        model.data(idx, Qt.ForegroundRole))

            text, color = cell("ns=2;s=Q_Bad")
            self.assertEqual(text, "12.5（通信异常）")
            self.assertEqual(color.name(), QColor("red").name())

            text, _ = cell("ns=2;s=Q_None")
            self.assertEqual(text, "通信异常")

            text, color = cell("ns=2;s=Q_Uncertain")
            self.assertEqual(text, "3.0（通信异常）")
            self.assertEqual(color.name(), QColor("orange").name())

            text, _ = cell("ns=2;s=Q_Good")
            self.assertEqual(text, "220.5")

            text, _ = cell("ns=2;s=Q_Bool")
            self.assertEqual(text, "开启（通信异常）")

    def test_readonly_nodes_not_clickable(self):
        """DA 的 writable=False 必须判为只读；UA 缺字段按可写处理（兼容）。"""
        self.assertFalse(_is_node_writable({"type": "Boolean", "writable": False}))
        self.assertTrue(_is_node_writable({"type": "Boolean", "writable": True}))
        self.assertTrue(_is_node_writable({"type": "Boolean"}))  # UA 版无 writable 字段
        self.assertFalse(_is_node_writable({"writable": False, "access": 3}))
        self.assertFalse(_is_node_writable({"access": 0x01}))
        self.assertFalse(_is_node_writable({"access": "ReadOnly"}))
        self.assertTrue(_is_node_writable({"access": 0x03}))

if __name__ == "__main__":
    unittest.main()
