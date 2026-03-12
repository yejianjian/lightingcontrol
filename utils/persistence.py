import json
import os
from utils.logger import global_logger

class PersistenceManager:
    """
    负责本地持久化点位的别名、数据分组和调度方案
    """
    def __init__(self, data_file="data/lighting_config.json"):
        self.data_file = data_file
        self.data_store = {
            "aliases": {},  # node_id: "自定义名称"
            "groups": {},   # group_name: ["node_id1", "node_id2"]
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
                    loaded = True
                    if path != self.data_file:
                        global_logger.warning(f"Primary config corrupted, restored from backup: {path}")
                    else:
                        global_logger.info(f"Successfully loaded configuration from {path}")
                    break
                except Exception as e:
                    global_logger.error(f"Failed to load configuration from {path}: {e}")
        if not loaded:
            global_logger.info(f"{self.data_file} not found. A new one will be created upon saving.")

    def save(self):
        """原子写入：先写临时文件再替换，防止断电导致文件损坏"""
        os.makedirs(os.path.dirname(self.data_file) or '.', exist_ok=True)
        tmp_file = self.data_file + ".tmp"
        try:
            with open(tmp_file, 'w', encoding='utf-8') as f:
                json.dump(self.data_store, f, ensure_ascii=False, indent=2)
            # 成功写入后，创建备份并替换原文件
            if os.path.exists(self.data_file):
                bak_file = self.data_file + ".bak"
                try:
                    if os.path.exists(bak_file):
                        os.remove(bak_file)
                    os.rename(self.data_file, bak_file)
                except OSError:
                    pass  # 备份失败不阻塞主流程
            os.rename(tmp_file, self.data_file)
            global_logger.debug(f"Configuration saved to {self.data_file}")
        except Exception as e:
            global_logger.error(f"Failed to save configuration: {e}")
            # 清理可能残留的临时文件
            if os.path.exists(tmp_file):
                try:
                    os.remove(tmp_file)
                except OSError:
                    pass

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

    # --- Group API (分组管理) ---
    def get_groups(self) -> dict:
        return self.data_store.get("groups", {})

    def add_group(self, group_name: str) -> bool:
        if group_name in self.data_store["groups"]:
            return False
        self.data_store["groups"][group_name] = []
        self.save()
        return True
        
    def rename_group(self, old_name: str, new_name: str) -> bool:
        if old_name not in self.data_store["groups"] or new_name in self.data_store["groups"]:
            return False
        nodes = self.data_store["groups"].pop(old_name)
        self.data_store["groups"][new_name] = nodes
        self.save()
        return True
        
    def delete_group(self, group_name: str):
        if group_name in self.data_store["groups"]:
            del self.data_store["groups"][group_name]
            self.save()
            return True
        return False
        
    def update_group_members(self, group_name: str, node_ids: list):
        if group_name in self.data_store["groups"]:
            self.data_store["groups"][group_name] = node_ids
            self.save()

    # --- Schedule API (调度器预留) ---
    def get_schedules(self):
        return self.data_store.get("schedules", [])
        
    def add_schedule(self, schedule_dict):
        """传入字典包含: id, group, time(HH:MM), action(bool), enabled"""
        if "schedules" not in self.data_store:
            self.data_store["schedules"] = []
        self.data_store["schedules"].append(schedule_dict)
        self.save()
        
    def delete_schedule(self, sched_id):
        schedules = self.data_store.get("schedules", [])
        self.data_store["schedules"] = [s for s in schedules if s.get("id") != sched_id]
        self.save()
        
    def update_schedule(self, sched_id, updated_fields: dict):
        schedules = self.data_store.get("schedules", [])
        for s in schedules:
            if s.get("id") == sched_id:
                s.update(updated_fields)
                break
        self.save()
