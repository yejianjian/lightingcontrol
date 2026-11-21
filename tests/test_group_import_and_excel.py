# -*- coding: utf-8 -*-
"""分组导入 / Excel 别名列识别的回归测试。

覆盖四个已修复缺陷中的三个（导入合并落盘、父子环、别名列误判）以及组内成员表格
模型的增量刷新。测试全部使用 tempfile 下的独立配置文件，绝不触碰
D:\\lightingcontrol\\data\\lighting_config.json。
"""

import os

# Qt 在无人值守环境下必须走离屏后端，且要在创建 QApplication 之前设置
os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import shutil
import sys
import tempfile
import unittest
import unittest.mock as mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

from core.data_manager import DataManager
from utils.persistence import PersistenceManager
from utils.excel_parser import import_aliases_from_excel

try:
    from PyQt5.QtWidgets import QApplication, QMessageBox
    from PyQt5.QtCore import Qt
    _QT_ERROR = None
except ImportError:
    try:
        from PySide6.QtWidgets import QApplication, QMessageBox
        from PySide6.QtCore import Qt
        _QT_ERROR = None
    except ImportError as exc:
        _QT_ERROR = exc

try:
    from ui.tabs.tab_group import (
        GroupMemberTableModel,
        TabGroup,
        import_groups_from_excel,
        merge_groups_from_dataframe,
    )
    _TAB_GROUP_ERROR = None
except ImportError as exc:
    _TAB_GROUP_ERROR = exc


GROUP_COLUMNS = ["分组名称", "父分组名称", "节点标识"]


class _TempDataMixin(object):
    """公共夹具：每个用例一个临时目录 + 独立配置文件。"""

    def setUp(self):
        super().setUp()
        self.tmp_dir = tempfile.mkdtemp(prefix="lc_group_test_")
        self.data_file = os.path.join(self.tmp_dir, "lighting_config.json")
        self.addCleanup(shutil.rmtree, self.tmp_dir, ignore_errors=True)

    def _new_pm(self):
        return PersistenceManager(data_file=self.data_file)

    def _make_dm(self, node_ids):
        """构造 DataManager，并按 node_id 预置点位（模拟已发现的节点表）。"""
        pm = self._new_pm()
        dm = DataManager(pm)
        for nid in node_ids:
            dm.update_node(nid, {"name": nid.split("=")[-1], "type": "Boolean", "value": True})
        return pm, dm

    def _write_excel(self, rows, columns, name="import.xlsx"):
        path = os.path.join(self.tmp_dir, name)
        pd.DataFrame(rows, columns=columns).to_excel(path, index=False)
        return path

    def _assert_no_parent_cycle(self, groups):
        """从每个分组向上走父链，任何一次重复访问都说明存在环。"""
        by_id = {g["id"]: g for g in groups}
        for group in groups:
            seen = set()
            cursor = group.get("parent_id")
            while cursor:
                self.assertNotIn(cursor, seen,
                                 f"分组 '{group['name']}' 的父链存在循环引用")
                seen.add(cursor)
                parent = by_id.get(cursor)
                self.assertIsNotNone(parent,
                                     f"分组 '{group['name']}' 的父分组 {cursor} 不存在")
                cursor = parent.get("parent_id")


@unittest.skipIf(_TAB_GROUP_ERROR is not None, "ui.tabs.tab_group 无法导入: %s" % (_TAB_GROUP_ERROR,))
class TestGroupImportMerge(_TempDataMixin, unittest.TestCase):
    """缺陷1：合并进「已存在分组」的内容必须真正落盘。"""

    def test_merge_into_existing_group_is_persisted(self):
        pm, dm = self._make_dm(["ns=2;s=Light_1", "ns=2;s=Light_2", "ns=2;s=Light_3"])
        group_id = pm.add_group("一层照明")
        pm.update_group_members(group_id, ["ns=2;s=Light_1"])

        path = self._write_excel([
            {"分组名称": "一层照明", "父分组名称": "", "节点标识": "Light_2"},
            {"分组名称": "一层照明", "父分组名称": "", "节点标识": "Light_3"},
        ], GROUP_COLUMNS)

        count = import_groups_from_excel(path, pm, dm)
        self.assertEqual(count, 1)

        # 内存中的活对象已按并集合并（原有成员 Light_1 不能被覆盖掉）
        self.assertEqual(set(pm.get_group_by_id(group_id)["nodes"]),
                         {"ns=2;s=Light_1", "ns=2;s=Light_2", "ns=2;s=Light_3"})

        # 关键断言：重新从磁盘实例化后合并结果依然存在（修复前这里只剩 Light_1）
        pm2 = PersistenceManager(data_file=self.data_file)
        matched = [g for g in pm2.get_groups() if g["name"] == "一层照明"]
        self.assertEqual(len(matched), 1)
        self.assertEqual(set(matched[0]["nodes"]),
                         {"ns=2;s=Light_1", "ns=2;s=Light_2", "ns=2;s=Light_3"})

    def test_new_group_and_parent_relation_are_persisted(self):
        """新建分组路径不能被修复破坏，父分组关系同样要落盘。"""
        pm, dm = self._make_dm(["ns=2;s=Light_9"])
        parent_id = pm.add_group("一层照明")

        path = self._write_excel([
            {"分组名称": "二层照明", "父分组名称": "一层照明", "节点标识": "Light_9"},
        ], GROUP_COLUMNS)

        count = import_groups_from_excel(path, pm, dm)
        self.assertEqual(count, 1)

        pm2 = PersistenceManager(data_file=self.data_file)
        by_name = {g["name"]: g for g in pm2.get_groups()}
        self.assertIn("二层照明", by_name)
        self.assertEqual(by_name["二层照明"]["parent_id"], parent_id)
        self.assertEqual(by_name["二层照明"]["nodes"], ["ns=2;s=Light_9"])

    def test_parent_cycle_from_excel_is_rejected(self):
        pm, dm = self._make_dm([])
        a_id = pm.add_group("组A")
        b_id = pm.add_group("组B")

        path = self._write_excel([
            {"分组名称": "组A", "父分组名称": "组B", "节点标识": ""},
            {"分组名称": "组B", "父分组名称": "组A", "节点标识": ""},
        ], GROUP_COLUMNS)

        merge_groups_from_dataframe(pm, dm, pd.read_excel(path))
        pm.save()

        groups = PersistenceManager(data_file=self.data_file).get_groups()
        self._assert_no_parent_cycle(groups)

        by_id = {g["id"]: g for g in groups}
        # 先处理的一条被接受，后一条会导致环，被拒绝
        self.assertEqual(by_id[a_id]["parent_id"], b_id)
        self.assertIsNone(by_id[b_id]["parent_id"])

    def test_self_parent_from_excel_is_rejected(self):
        pm, dm = self._make_dm([])
        a_id = pm.add_group("组A")

        df = pd.DataFrame([{"分组名称": "组A", "父分组名称": "组A", "节点标识": ""}],
                          columns=GROUP_COLUMNS)
        merge_groups_from_dataframe(pm, dm, df)
        pm.save()

        groups = PersistenceManager(data_file=self.data_file).get_groups()
        self._assert_no_parent_cycle(groups)
        self.assertIsNone({g["id"]: g for g in groups}[a_id]["parent_id"])

    def test_cycle_through_newly_created_group_is_rejected(self):
        """环检测必须能看见「本次导入刚创建、尚未 save()」的分组。"""
        pm, dm = self._make_dm([])
        d_id = pm.add_group("组D")

        df = pd.DataFrame([
            {"分组名称": "新组C", "父分组名称": "组D", "节点标识": ""},
            {"分组名称": "组D", "父分组名称": "新组C", "节点标识": ""},
        ], columns=GROUP_COLUMNS)

        merge_groups_from_dataframe(pm, dm, df)
        pm.save()

        groups = PersistenceManager(data_file=self.data_file).get_groups()
        self._assert_no_parent_cycle(groups)

        by_name = {g["name"]: g for g in groups}
        # 新建组挂到已有组下面是合法层级，允许
        self.assertEqual(by_name["新组C"]["parent_id"], d_id)
        # 反向赋值会形成 新组C -> 组D -> 新组C，必须被拒绝
        self.assertIsNone(by_name["组D"]["parent_id"])

    def test_single_save_after_batch_merge(self):
        """整批合并只写盘一次（原子持久化）。"""
        pm, dm = self._make_dm(["ns=2;s=Light_1", "ns=2;s=Light_2"])
        pm.add_group("一层照明")

        path = self._write_excel([
            {"分组名称": "一层照明", "父分组名称": "", "节点标识": "Light_1"},
            {"分组名称": "二层照明", "父分组名称": "一层照明", "节点标识": "Light_2"},
        ], GROUP_COLUMNS)

        with mock.patch.object(pm, "save", wraps=pm.save) as spy:
            import_groups_from_excel(path, pm, dm)
        self.assertEqual(spy.call_count, 1)


class TestExcelAliasColumnDetection(_TempDataMixin, unittest.TestCase):
    """缺陷2：[Node ID, 数据类型] 这类表不得把类型当成别名写库。"""

    def test_missing_alias_column_does_not_overwrite_aliases(self):
        pm = self._new_pm()
        dm = DataManager(pm)
        dm.update_node("ns=2;s=Tag_1", {"name": "Tag_1", "type": "Boolean", "value": True})
        dm.set_alias("ns=2;s=Tag_1", "用户原别名")

        path = self._write_excel([
            {"Node ID": "Tag_1", "数据类型": "Boolean"},
            {"Node ID": "Tag_2", "数据类型": "Real"},
        ], ["Node ID", "数据类型"])

        count, msg = import_aliases_from_excel(path, dm)

        # 必须明确失败：调用方以 count > 0 判定成功
        self.assertEqual(count, 0)
        self.assertTrue(msg)
        self.assertIn("未导入任何数据", msg)

        # 内存别名未被覆盖
        self.assertEqual(dm.pm.get_all_aliases(), {"ns=2;s=Tag_1": "用户原别名"})
        self.assertEqual(dm.get_alias_by_node_id("ns=2;s=Tag_1"), "用户原别名")

        # 磁盘上同样未被覆盖
        pm2 = PersistenceManager(data_file=self.data_file)
        self.assertEqual(pm2.get_all_aliases(), {"ns=2;s=Tag_1": "用户原别名"})
        self.assertNotIn("Boolean", list(pm2.get_all_aliases().values()))
        self.assertNotIn("Real", list(pm2.get_all_aliases().values()))

    def test_node_id_column_preferred_over_node_name_column(self):
        """「节点名称」不得抢走 Node ID 列的位置。"""
        pm = self._new_pm()
        dm = DataManager(pm)
        dm.update_node("ns=2;s=Tag_1", {"name": "Tag_1", "type": "Boolean", "value": True})

        path = self._write_excel([
            {"节点名称": "大厅主灯", "节点标识": "Tag_1"},
        ], ["节点名称", "节点标识"])

        count, msg = import_aliases_from_excel(path, dm)
        self.assertEqual(count, 1, msg)
        self.assertEqual(dm.pm.get_all_aliases()["ns=2;s=Tag_1"], "大厅主灯")

    def test_happy_path_still_works(self):
        pm = self._new_pm()
        dm = DataManager(pm)
        dm.update_node("ns=2;s=Tag_1", {"name": "Tag_1", "type": "Boolean", "value": True})

        path = self._write_excel([
            {"Node ID": "Tag_1", "备注名/别名": "走廊筒灯"},
        ], ["Node ID", "备注名/别名"])

        count, msg = import_aliases_from_excel(path, dm)
        self.assertEqual(count, 1, msg)
        self.assertEqual(dm.pm.get_all_aliases()["ns=2;s=Tag_1"], "走廊筒灯")


@unittest.skipIf(_TAB_GROUP_ERROR is not None, "ui.tabs.tab_group 无法导入: %s" % (_TAB_GROUP_ERROR,))
class TestGroupMemberTableModelRefresh(_TempDataMixin, unittest.TestCase):
    """缺陷3：update_nodes 必须同步刷新本地快照缓存。"""

    @classmethod
    def setUpClass(cls):
        if _QT_ERROR is not None:
            raise unittest.SkipTest("Qt 不可用: %s" % (_QT_ERROR,))
        cls.app = QApplication.instance() or QApplication([])

    def test_update_nodes_refreshes_cached_status(self):
        pm, dm = self._make_dm(["ns=2;s=Light_1"])
        model = GroupMemberTableModel(dm)
        model.set_data(dm.get_node_list())

        self.assertEqual(model.data(model.index(0, 1), Qt.DisplayRole), "开启")
        self.assertTrue(model._data_cache[0]["value"])

        # 数据总线更新后，增量通知必须把最新值合并进缓存
        dm.update_node("ns=2;s=Light_1", {"value": False})
        model.update_nodes(["ns=2;s=Light_1"])

        self.assertFalse(model._data_cache[0]["value"])
        self.assertEqual(model.data(model.index(0, 1), Qt.DisplayRole), "关闭")

    def test_update_nodes_ignores_unknown_rows(self):
        pm, dm = self._make_dm(["ns=2;s=Light_1"])
        model = GroupMemberTableModel(dm)
        model.set_data(dm.get_node_list())
        model.update_nodes(["ns=2;s=Not_In_This_Group"])
        self.assertTrue(model._data_cache[0]["value"])


@unittest.skipIf(_TAB_GROUP_ERROR is not None, "ui.tabs.tab_group 无法导入: %s" % (_TAB_GROUP_ERROR,))
class TestTabGroupExcelHandlers(_TempDataMixin, unittest.IsolatedAsyncioTestCase):
    """缺陷4：分组的 Excel 导入/导出走线程池，且导入结果落盘。"""

    @classmethod
    def setUpClass(cls):
        if _QT_ERROR is not None:
            raise unittest.SkipTest("Qt 不可用: %s" % (_QT_ERROR,))
        cls.app = QApplication.instance() or QApplication([])

    async def test_export_then_import_round_trip_persists(self):
        pm, dm = self._make_dm(["ns=2;s=Light_1", "ns=2;s=Light_2"])
        group_id = pm.add_group("一层照明")
        pm.update_group_members(group_id, ["ns=2;s=Light_1"])

        tab = TabGroup(dm, object())
        export_path = os.path.join(self.tmp_dir, "groups_export.xlsx")
        import_path = self._write_excel([
            {"分组名称": "一层照明", "父分组名称": "", "节点标识": "Light_2"},
        ], GROUP_COLUMNS, name="groups_import.xlsx")

        with mock.patch.object(QMessageBox, "information", return_value=QMessageBox.Ok), \
                mock.patch.object(QMessageBox, "critical", return_value=QMessageBox.Ok):
            await tab._do_async_export_groups(export_path, dm.pm.get_groups())
            self.assertTrue(os.path.exists(export_path))
            exported = pd.read_excel(export_path)
            self.assertIn("分组名称", exported.columns)
            self.assertEqual(list(exported["节点标识"]), ["Light_1"])

            await tab._do_async_import_groups(import_path)

        pm2 = PersistenceManager(data_file=self.data_file)
        matched = [g for g in pm2.get_groups() if g["name"] == "一层照明"]
        self.assertEqual(len(matched), 1)
        self.assertEqual(set(matched[0]["nodes"]), {"ns=2;s=Light_1", "ns=2;s=Light_2"})


if __name__ == "__main__":
    unittest.main()
