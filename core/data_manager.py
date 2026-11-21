from typing import Dict, Any, Set, Optional, List
import asyncio
import threading
from utils.persistence import PersistenceManager
from utils.logger import global_logger


def _normalize_bool(val: Any) -> Optional[bool]:
    """
    统一将各种 OPC UA 表现形式转换为纯粹的 bool 或 None。
    支持 bool, int (1/0), str ('true'/'false'/'1'/'0') 等格式。

    注意：本函数用于**展示与判定开关语义**，因此对无法确定开关含义的值返回 None。
    调用方如果要统计"开启/关闭"数量，请使用 :func:`_normalize_switch`。
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        return bool(val)
    if isinstance(val, str):
        val_lower = val.strip().lower()
        if val_lower in ("true", "1", "on", "yes", "开启"):
            return True
        if val_lower in ("false", "0", "off", "no", "关闭"):
            return False
    return None


def _normalize_switch(val: Any) -> Optional[bool]:
    """把值归一化为"开关状态"，无法确定时返回 None。

    仪表盘的"当前开启/当前关闭"必须只统计真正的开关量。现场点表里同时存在大量
    模拟量（电压、功率、温度、计数器），如果沿用 ``bool(val)`` 的宽松规则，
    220.5 或任何非零整数都会被算成"开启"，仪表盘数字与表格内容完全对不上，
    而且 ``recalculate_counts`` 用同一规则，断线重连也无法自愈。

    判定规则（保守优先，宁可漏计也不错计）：
    * ``bool`` → 原值；
    * 数值型 → 仅接受精确的 0 / 1；
    * 字符串 → 仅接受明确的开关词汇。
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)):
        try:
            number = float(val)
        except (TypeError, ValueError):
            return None
        if number == 1.0:
            return True
        if number == 0.0:
            return False
        return None
    if isinstance(val, str):
        val_lower = val.strip().lower()
        if val_lower in ("true", "1", "on", "yes", "开启"):
            return True
        if val_lower in ("false", "0", "off", "no", "关闭"):
            return False
    return None


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

        # 别名（alias）是**用户配置**，唯一事实来源是 self.aliases（持久化的
        # data_store["aliases"]）。引擎/服务器推送的点位字典里也带 alias 字段，
        # 但那只是"扫描出来的默认显示名"。曾经因为对已存在点位直接
        # nodes[node_id].update(filtered_data)，每次重连/手动添加点位后重发布
        # （_publish_nodes_to_bus）都会把用户导入的别名覆盖回原始 ItemID，
        # 表现为"手动加了一个点位，所有别名都消失了，需要重新导入别名表"。
        # 因此这里先剥离 incoming alias，再按"已保存别名 > 已有别名 > 默认名"回填。
        incoming_alias = filtered_data.pop('alias', None)

        # 锁的范围尽量小：仅保护 self.nodes 字典的读写和计数器更新
        # 避免长时间持锁阻塞 asyncua 订阅分发线程（该线程同步调用本方法）
        with self._dirty_lock:
            if node_id not in self.nodes:
                # 仅接受来自节点发现的完整数据（含 name 字段）创建新条目，
                # 跳过订阅推送中未知节点的数据（可能为服务端延迟推送或新增节点）
                if 'name' not in filtered_data:
                    return
                # 补齐默认字段，确保后续访问不会 KeyError
                filtered_data.setdefault('node_id', node_id)
                filtered_data.setdefault('type', 'Unknown')
                filtered_data.setdefault('timestamp', '')
                filtered_data.setdefault('value', None)
                self.nodes[node_id] = filtered_data
                self.nodes[node_id]['alias'] = self._alias_for_new_node(
                    node_id, incoming_alias, filtered_data.get('name'))
                self.structure_changed = True
                
                # 首次记录值以初始化计数器（仅统计真正的开关量）
                b_val = _normalize_switch(filtered_data.get('value'))
                if b_val is True:
                    self.on_count += 1
                elif b_val is False:
                    self.off_count += 1
            else:
                old_val = self.nodes[node_id].get('value')
                old_b = _normalize_switch(old_val)

                self.nodes[node_id].update(filtered_data)
                # 已存在点位：绝不把用户别名覆盖成服务器默认名
                self.nodes[node_id]['alias'] = self._alias_for_existing_node(
                    node_id, incoming_alias)

                new_val = self.nodes[node_id].get('value')
                new_b = _normalize_switch(new_val)
                
                # 使用归一化后的开关状态维护计数器，防止模拟量被计入开关统计
                if old_b != new_b:
                    if old_b is True:
                        self.on_count = max(0, self.on_count - 1)
                    elif old_b is False:
                        self.off_count = max(0, self.off_count - 1)
                        
                    if new_b is True:
                        self.on_count += 1
                    elif new_b is False:
                        self.off_count += 1

            self._dirty_nodes.add(node_id)

    def _alias_for_new_node(self, node_id, incoming_alias, default_name):
        """新点位别名解析顺序：已保存别名 > 数据源携带的别名 > 显示名 > node_id。"""
        return (self.aliases.get(node_id)
                or incoming_alias
                or default_name
                or node_id)

    def _alias_for_existing_node(self, node_id, incoming_alias):
        """已存在点位的别名：已保存别名 > 当前别名 > 数据源别名 > 显示名。

        注意：调用本方法时 nodes[node_id] 已经被 meta 数据更新过，
        因此这里读到的是"当前别名"（可能来自上一轮解析结果）。
        """
        node = self.nodes.get(node_id) or {}
        return (self.aliases.get(node_id)
                or node.get('alias')
                or incoming_alias
                or node.get('name')
                or node_id)

    def remove_node(self, node_id: str) -> bool:
        """从数据总线移除一个点位（手动添加点位被删除时使用）。

        与 update_node 使用同一套开关判定维护计数器；返回是否确实移除了点位。
        """
        with self._dirty_lock:
            node = self.nodes.pop(node_id, None)
            if node is None:
                return False
            b_val = _normalize_switch(node.get('value'))
            if b_val is True:
                self.on_count = max(0, self.on_count - 1)
            elif b_val is False:
                self.off_count = max(0, self.off_count - 1)
            self._dirty_nodes.discard(node_id)
            self.structure_changed = True
        return True

    def recalculate_counts(self):
        """重新全量计算开启/关闭数量，供断线重连或批量导入后自愈校准。

        与 ``update_node`` 使用同一套开关判定，保证增量与全量口径一致。
        """
        with self._dirty_lock:
            on_c = 0
            off_c = 0
            for n in self.nodes.values():
                b = _normalize_switch(n.get('value'))
                if b is True:
                    on_c += 1
                elif b is False:
                    off_c += 1
            self.on_count = on_c
            self.off_count = off_c

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
        with self._dirty_lock:
            self.aliases[node_id] = alias
            if node_id in self.nodes:
                self.nodes[node_id]['alias'] = alias
            self._dirty_nodes.add(node_id)
        # 持久化放到锁外避免死锁
        self.pm.set_alias(node_id, alias)

    def batch_apply_aliases(self, alias_map: dict) -> int:
        """线程安全地批量应用别名（Excel 导入用）。

        Excel 导入在后台线程执行；旧实现直接 dm.nodes[nid]['alias'] = alias，
        会与 Qt/asyncio 线程的订阅更新并发改写同一个字典，且没有加锁。这里统一
        在锁内更新内存与脏集合，再一次性持久化（避免 N 次磁盘写）。
        """
        if not alias_map:
            return 0
        with self._dirty_lock:
            self.aliases.update(alias_map)
            for node_id, alias in alias_map.items():
                if node_id in self.nodes:
                    self.nodes[node_id]['alias'] = alias
                self._dirty_nodes.add(node_id)
        self.pm.batch_set_aliases(alias_map)
        return len(alias_map)

    def mark_dirty(self, node_id: str):
        """标记节点为脏，用于外部模块触发 UI 刷新"""
        with self._dirty_lock:
            self._dirty_nodes.add(node_id)

    def get_node_list(self) -> List[Dict[str, Any]]:
        """加锁获取安全的字典浅拷贝快照，彻底杜绝并发迭代时的 RuntimeError"""
        with self._dirty_lock:
            return [dict(n) for n in self.nodes.values()]

    def get_alias_by_node_id(self, node_id: str) -> str:
        """根据 Node ID 获取别名，如果无别名则返回原始 ID（线程安全）"""
        with self._dirty_lock:
            if node_id in self.nodes:
                return self.nodes[node_id].get('alias', node_id)
            return self.aliases.get(node_id, node_id)


