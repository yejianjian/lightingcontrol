import json
import os
import sys
import uuid
from contextlib import contextmanager
from utils.logger import global_logger

def _get_base_dir():
    """获取应用基础目录：打包后为可执行文件所在目录，开发时为项目根目录"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

class PersistenceManager:
    """
    负责本地持久化点位的别名、数据分组和调度方案
    """
    def __init__(self, data_file="data/lighting_config.json"):
        # 将相对路径转为基于应用目录的绝对路径，避免工作目录不一致问题
        if not os.path.isabs(data_file):
            data_file = os.path.join(_get_base_dir(), data_file)
        self.data_file = data_file
        self._batch_count = 0  # 批量模式嵌套计数
        self._load_failed = False  # 标记配置文件是否全部加载失败
        self._groups_index = {}     # id -> group_obj 索引
        self._schedules_index = {}  # id -> schedule_obj 索引
        self.data_store = {
            "aliases": {},  # node_id: "自定义名称"
            "groups": [],   # 升级为列表结构: [{"id": "uuid", "name": "...", "parent_id": None, "nodes": [...]}, ...]
            "schedules": [] # 结构化的定时任务清单
        }
        self.load()

    def load(self):
        """将本地磁盘的数据反序列化到内存，加载失败时尝试从备份恢复"""
        loaded = False
        for path in [self.data_file, self.data_file + ".bak"]:
            if os.path.exists(path):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    # 结构校验：确保核心 key 存在
                    for key in ("aliases", "groups", "schedules"):
                        if key not in data:
                            data[key] = self.data_store[key]
                    self.data_store.update(data)
                    
                    # --- 数据迁移逻辑 (由平级字典迁移至树形列表) ---
                    if isinstance(self.data_store["groups"], dict):
                        global_logger.info("Migrating legacy flat group dictionary to hierarchical list...")
                        old_groups = self.data_store["groups"]
                        new_groups = []
                        name_to_id_map = {}
                        
                        # 转换分组
                        for g_name, nodes in old_groups.items():
                            g_id = str(uuid.uuid4())
                            name_to_id_map[g_name] = g_id
                            new_groups.append({
                                "id": g_id,
                                "name": g_name,
                                "parent_id": None,
                                "nodes": nodes
                            })
                        self.data_store["groups"] = new_groups
                        
                        # 修正调度计划关联（由名称关联改为 ID 关联）
                        for sched in self.data_store["schedules"]:
                            old_g_name = sched.get("group")
                            if old_g_name in name_to_id_map:
                                sched["group_id"] = name_to_id_map[old_g_name]
                                # 兼容性：保留 group 字段用于显示，新增 group_id 用于逻辑
                        
                        self.save()
                        global_logger.info("Data migration completed successfully.")
                    # ---------------------------------------------
                    
                    loaded = True
                    if path != self.data_file:
                        global_logger.warning(f"Primary config corrupted, restored from backup: {path}")
                    else:
                        global_logger.info(f"Successfully loaded configuration from {path}")
                    self._rebuild_index()
                    break
                except Exception as e:
                    global_logger.error(f"Failed to load configuration from {path}: {e}")
        if not loaded:
            # 检查是文件不存在还是文件损坏
            primary_exists = os.path.exists(self.data_file)
            backup_exists = os.path.exists(self.data_file + ".bak")
            if primary_exists or backup_exists:
                # 文件存在但加载失败（损坏）
                self._load_failed = True
                global_logger.error(f"Configuration files corrupted. Both primary and backup failed to load.")
            else:
                global_logger.info(f"{self.data_file} not found. A new one will be created upon saving.")

    @contextmanager
    def batch_mode(self):
        """批量操作上下文：期间所有 save() 调用被抑制，退出时统一保存一次"""
        self._batch_count += 1
        try:
            yield
        finally:
            self._batch_count -= 1
            if self._batch_count == 0:
                self.save()

    def save(self):
        """原子写入：先写临时文件再替换，防止断电导致文件损坏"""
        if self._batch_count > 0:
            return  # 批量模式下跳过，等 batch_mode 退出时统一保存
        os.makedirs(os.path.dirname(self.data_file) or '.', exist_ok=True)
        tmp_file = self.data_file + ".tmp"
        try:
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(self.data_store, f, ensure_ascii=False, indent=2)
            # 成功写入后，创建备份并替换原文件
            if os.path.exists(self.data_file):
                bak_file = self.data_file + ".bak"
                try:
                    # 使用 os.replace 替代 os.rename，前者在 Windows 上也能原子覆盖
                    os.replace(self.data_file, bak_file)
                except OSError:
                    pass  # 备份失败不阻塞主流程
            os.replace(tmp_file, self.data_file)
            # 保存成功后清除损坏标记
            self._load_failed = False
            global_logger.debug(f"Configuration saved to {self.data_file}")
            self._rebuild_index()
        except Exception as e:
            global_logger.error(f"Failed to save configuration: {e}")
            # 清理可能残留的临时文件
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

    def was_load_corrupted(self) -> bool:
        """检查配置文件是否在加载时损坏（主文件和备份都损坏）"""
        return self._load_failed

    # --- 别名 (Alias/Remark) API ---
    def get_all_aliases(self) -> dict:
        return self.data_store.get("aliases", {})
        
    def set_alias(self, node_id: str, alias: str):
        self.data_store["aliases"][node_id] = alias
        self.save()
        
    def batch_set_aliases(self, alias_dict: dict):
        """批量导入用"""
        self.data_store["aliases"].update(alias_dict)
        self.save()

    # --- Group API (多级分组管理) ---
    def get_groups(self) -> list:
        """返回分组列表对象"""
        return self.data_store.get("groups", [])

    def add_group(self, group_name: str, parent_id: str = None) -> str:
        """添加分组，返回新分组的 ID"""
        new_id = str(uuid.uuid4())
        new_group = {
            "id": new_id,
            "name": group_name,
            "parent_id": parent_id,
            "nodes": []
        }
        self.data_store["groups"].append(new_group)
        self.save()
        return new_id
        
    def rename_group(self, group_id: str, new_name: str) -> bool:
        for g in self.data_store["groups"]:
            if g["id"] == group_id:
                g["name"] = new_name
                self.save()
                return True
        return False
        
    def delete_group(self, group_id: str):
        """删除分组及其所有子分组"""
        to_delete = {group_id}
        
        # 递归寻找子孙
        def find_descendants(pid):
            for g in self.data_store["groups"]:
                if g["parent_id"] == pid:
                    to_delete.add(g["id"])
                    find_descendants(g["id"])
                    
        find_descendants(group_id)
        
        # 批量过滤
        self.data_store["groups"] = [g for g in self.data_store["groups"] if g["id"] not in to_delete]
        # 同时清理关联的调度计划
        self.data_store["schedules"] = [s for s in self.data_store["schedules"] if s.get("group_id") not in to_delete]
        
        self.save()
        return True
        
    def update_group_members(self, group_id: str, node_ids: list):
        for g in self.data_store["groups"]:
            if g["id"] == group_id:
                g["nodes"] = node_ids
                self.save()
                return True
        return False

    def get_group_nodes_recursive(self, group_id: str) -> set:
        """递归获取当前组及其所有子孙组关联的 OPC 节点 ID 集合"""
        all_node_ids = set()
        visited = set()
        
        def collect(gid):
            if gid in visited:
                return
            visited.add(gid)
            for g in self.data_store["groups"]:
                if g["id"] == gid:
                    all_node_ids.update(g.get("nodes", []))
                if g["parent_id"] == gid:
                    collect(g["id"])
                    
        collect(group_id)
        return all_node_ids

    def get_group_by_id(self, group_id: str):
        return self._groups_index.get(group_id)

    def _rebuild_index(self):
        """重建 groups 和 schedules 的 O(1) 查找索引"""
        self._groups_index = {g["id"]: g for g in self.data_store.get("groups", [])}
        self._schedules_index = {s.get("id"): s for s in self.data_store.get("schedules", []) if s.get("id")}

    # --- Schedule API (调度器预留) ---
    def get_schedules(self):
        return self.data_store.get("schedules", [])

    def get_schedule_by_id(self, sched_id):
        return self._schedules_index.get(sched_id)

    def add_schedule(self, schedule_dict):
        """传入字典包含: id, group_id, time(HH:MM), action(bool), enabled"""
        if "schedules" not in self.data_store:
            self.data_store["schedules"] = []
        self.data_store["schedules"].append(schedule_dict)
        self.save()
        
    def delete_schedule(self, sched_id):
        schedules = self.data_store.get("schedules", [])
        self.data_store["schedules"] = [s for s in schedules if s.get("id") != sched_id]
        self.save()

    def batch_add_schedules(self, schedule_list):
        """批量添加多个调度计划，一次性持久化，避免 N 次磁盘 IO"""
        if "schedules" not in self.data_store:
            self.data_store["schedules"] = []
        self.data_store["schedules"].extend(schedule_list)
        self.save()
        
    def update_schedule(self, sched_id, updated_fields: dict):
        schedules = self.data_store.get("schedules", [])
        for s in schedules:
            if s.get("id") == sched_id:
                s.update(updated_fields)
                break
        self.save()
