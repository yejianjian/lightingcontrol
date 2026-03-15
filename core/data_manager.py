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
        
        # 脏标记缓冲队列（用于防抖防卡死）
        self.dirty_nodes = set()
        self.structure_changed = False 

    def update_node(self, node_id: str, new_data: dict):
        if node_id not in self.nodes:
            # 仅接受来自节点发现的完整数据（含 name 字段）创建新条目，
            # 跳过订阅推送中未知节点的数据（可能为服务端延迟推送或新增节点）
            if 'name' not in new_data:
                from utils.logger import global_logger
                global_logger.debug(f"Skipping subscription data for unknown node: {node_id}")
                return
            # 补齐默认字段，确保后续访问不会 KeyError
            new_data.setdefault('type', 'Unknown')
            new_data.setdefault('timestamp', '')
            new_data.setdefault('value', None)
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
        
    def get_alias_by_node_id(self, node_id: str) -> str:
        """根据 Node ID 获取别名，如果无别名则返回原始 ID"""
        if node_id in self.nodes:
            return self.nodes[node_id].get('alias', node_id)
        return self.aliases.get(node_id, node_id)

