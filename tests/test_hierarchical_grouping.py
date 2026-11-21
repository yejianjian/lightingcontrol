import sys
import os
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.persistence import PersistenceManager

class TestHierarchicalGrouping(unittest.TestCase):
    def test_hierarchy_and_cascade_delete(self):
        test_file = "tmp_hierarchy_test.json"
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

        pm = PersistenceManager(test_file)

        # 1. 创建三级结构: 区域 > 楼层 > 房间
        area_id = pm.add_group("Office Area")
        floor_id = pm.add_group("1st Floor", parent_id=area_id)
        room_id = pm.add_group("Room 101", parent_id=floor_id)

        # 2. 分配节点
        pm.update_group_members(area_id, ["node_area_1"])
        pm.update_group_members(floor_id, ["node_floor_1", "node_floor_2"])
        pm.update_group_members(room_id, ["node_room_1"])

        # 3. 验证递归获取
        # 测试 Room 级 (底层)
        room_nodes = pm.get_group_nodes_recursive(room_id)
        self.assertEqual(room_nodes, {"node_room_1"})

        # 测试 Floor 级 (中层)
        floor_nodes = pm.get_group_nodes_recursive(floor_id)
        self.assertEqual(floor_nodes, {"node_floor_1", "node_floor_2", "node_room_1"})

        # 测试 Area 级 (顶层)
        area_nodes = pm.get_group_nodes_recursive(area_id)
        self.assertEqual(area_nodes, {"node_area_1", "node_floor_1", "node_floor_2", "node_room_1"})

        # 4. 验证删除及其连带子分组
        pm.delete_group(floor_id)
        remaining_groups = pm.get_groups()
        remaining_ids = [g["id"] for g in remaining_groups]
        self.assertIn(area_id, remaining_ids)
        self.assertNotIn(floor_id, remaining_ids)
        self.assertNotIn(room_id, remaining_ids)  # 子分组被连带删除

        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

if __name__ == "__main__":
    unittest.main()
