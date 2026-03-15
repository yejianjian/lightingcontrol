import sys
import os
import uuid

# 模拟环境
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.persistence import PersistenceManager
from utils.logger import global_logger

def test_hierarchy():
    test_file = "tmp_test_data.json"
    if os.path.exists(test_file): os.remove(test_file)
    
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
    print(f"Room nodes: {room_nodes}")
    assert room_nodes == {"node_room_1"}
    
    # 测试 Floor 级 (中层)
    floor_nodes = pm.get_group_nodes_recursive(floor_id)
    print(f"Floor nodes (recursive): {floor_nodes}")
    assert floor_nodes == {"node_floor_1", "node_floor_2", "node_room_1"}
    
    # 测试 Area 级 (顶层)
    area_nodes = pm.get_group_nodes_recursive(area_id)
    print(f"Area nodes (recursive): {area_nodes}")
    assert area_nodes == {"node_area_1", "node_floor_1", "node_floor_2", "node_room_1"}
    
    print("\n[SUCCESS] Hierarchical node collection verified!")
    
    # 4. 验证删除及其连带子分组
    pm.delete_group(floor_id)
    remaining_groups = pm.get_groups()
    remaining_ids = [g["id"] for g in remaining_groups]
    assert area_id in remaining_ids
    assert floor_id not in remaining_ids
    assert room_id not in remaining_ids # 子分组应被一并删除
    print("[SUCCESS] Cascade deletion verified!")

    os.remove(test_file)

if __name__ == "__main__":
    test_hierarchy()
