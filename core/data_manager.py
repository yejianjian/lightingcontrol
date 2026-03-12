from typing import Dict, Any
from utils.persistence import PersistenceManager

class DataManager:
    """
    中央数据总线，管理OPC节点的状态和本地配置文件映射
    """
    def __init__(self, persistence_manager: PersistenceManager):
        self.nodes: Dict[str, Any] = {} # node_id -> {name, value, type, timestamp, alias}
        
        self.pm = persistence_manager
        self.aliases = self.pm.get_all_aliases()
        self.groups = self.pm.get_groups()
        
        # 脏标记缓冲队列（用于防抖防卡死）
        self.dirty_nodes = set()
        self.structure_changed = False 

    def update_node(self, node_id: str, new_data: dict):
        if node_id not in self.nodes:
            self.nodes[node_id] = new_data
            self.nodes[node_id]['alias'] = self.aliases.get(node_id, new_data.get('name', ''))
            self.structure_changed = True
        else:
            self.nodes[node_id].update(new_data)
            
        self.dirty_nodes.add(node_id)

    def set_alias(self, node_id: str, alias: str):
        self.aliases[node_id] = alias
        self.pm.set_alias(node_id, alias)
        if node_id in self.nodes:
            self.nodes[node_id]['alias'] = alias
            self.dirty_nodes.add(node_id)

    def get_node_list(self):
        return list(self.nodes.values())
        

