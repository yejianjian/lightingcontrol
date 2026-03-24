from typing import Dict, Any, Set
import asyncio
import threading
from utils.persistence import PersistenceManager
from utils.logger import global_logger

class DataManager:
    """
    中央数据总线，管理OPC节点的状态和本地配置文件映射
    """
    def __init__(self, persistence_manager: PersistenceManager):
        self.nodes: Dict[str, Any] = {} # node_id -> {name, value, type, timestamp, alias}

        self.pm = persistence_manager
        self.aliases = self.pm.get_all_aliases()

        self._dirty_nodes: Set[str] = set()
        self._dirty_lock = threading.Lock()
        self.structure_changed = False

        self.on_count = 0
        self.off_count = 0

    def update_node(self, node_id: str, new_data: dict):
        # 过滤掉 asyncua 底层 Node 对象引用，避免存入数据总线导致 GC 无法回收
        filtered_data = {k: v for k, v in new_data.items() if k != 'node_obj'}

        # 锁的范围尽量小：仅保护 self.nodes 字典的读写和计数器更新
        # 避免长时间持锁阻塞 asyncua 订阅分发线程（该线程同步调用本方法）
        with self._dirty_lock:
            if node_id not in self.nodes:
                # 仅接受来自节点发现的完整数据（含 name 字段）创建新条目，
                # 跳过订阅推送中未知节点的数据（可能为服务端延迟推送或新增节点）
                if 'name' not in filtered_data:
                    return
                # 补齐默认字段，确保后续访问不会 KeyError
                filtered_data.setdefault('type', 'Unknown')
                filtered_data.setdefault('timestamp', '')
                filtered_data.setdefault('value', None)
                self.nodes[node_id] = filtered_data
                self.nodes[node_id]['alias'] = self.aliases.get(node_id, filtered_data.get('name', ''))
                self.structure_changed = True
                
                # 首次记录值以初始化计数器
                new_val = filtered_data.get('value')
                if new_val is True:
                    self.on_count += 1
                elif new_val is False:
                    self.off_count += 1
            else:
                old_val = self.nodes[node_id].get('value')
                self.nodes[node_id].update(filtered_data)
                new_val = self.nodes[node_id].get('value')
                
                # 使用 O(1) 状态机更迭计数器
                if old_val is not new_val:
                    if old_val is True:
                        self.on_count -= 1
                    elif old_val is False:
                        self.off_count -= 1
                        
                    if new_val is True:
                        self.on_count += 1
                    elif new_val is False:
                        self.off_count += 1

            self._dirty_nodes.add(node_id)

    def get_dirty_nodes_and_clear(self) -> Set[str]:
        """原子地获取并清空脏节点集合，供 Qt 主线程调用"""
        with self._dirty_lock:
            dirty = self._dirty_nodes
            self._dirty_nodes = set()
            return dirty

    def clear_nodes(self):
        """清空所有节点数据，用于断线时释放过期引用"""
        with self._dirty_lock:
            self.nodes.clear()
            self._dirty_nodes.clear()
            self.on_count = 0
            self.off_count = 0
            self.structure_changed = True

    def set_alias(self, node_id: str, alias: str):
        self.aliases[node_id] = alias
        self.pm.set_alias(node_id, alias)
        with self._dirty_lock:
            if node_id in self.nodes:
                self.nodes[node_id]['alias'] = alias
            self._dirty_nodes.add(node_id)

    def mark_dirty(self, node_id: str):
        """标记节点为脏，用于外部模块触发 UI 刷新"""
        with self._dirty_lock:
            self._dirty_nodes.add(node_id)

    def get_node_list(self):
        return list(self.nodes.values())

    def get_alias_by_node_id(self, node_id: str) -> str:
        """根据 Node ID 获取别名，如果无别名则返回原始 ID"""
        if node_id in self.nodes:
            return self.nodes[node_id].get('alias', node_id)
        return self.aliases.get(node_id, node_id)

