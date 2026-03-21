import pandas as pd
import re
from utils.logger import global_logger
from core.data_manager import DataManager

def import_aliases_from_excel(file_path: str, dm: DataManager) -> tuple:
    """
    从 Excel 批量读取点位别名。
    要求 Excel至少包含两列：'Node ID' 和 '备注名/别名'。如果列名不是固定的，可以尝试索引。
    返回: (成功导入的数量, 错误或成功信息)
    """
    try:
        df = pd.read_excel(file_path)

        # 宽容处理列名
        node_col = None
        alias_col = None

        for col in df.columns:
            col_lower = str(col).lower()
            if 'node' in col_lower or 'id' in col_lower or '\u8282\u70b9' in col_lower:
                if not node_col: node_col = col
            if '\u522b\u540d' in col_lower or '\u5907\u6ce8' in col_lower or 'alias' in col_lower or 'name' in col_lower:
                if not alias_col and col != node_col: alias_col = col

        if not node_col or not alias_col:
             # 如果猜不出，尝试强制使用第1列为NodeID，第2列为别名
             if len(df.columns) >= 2:
                 node_col = df.columns[0]
                 alias_col = df.columns[1]
             else:
                 return 0, "Excel\u6587\u4ef6\u683c\u5f0f\u4e0d\u7b26\uff0c\u81f3\u5c11\u9700\u8981\u5305\u542b Node ID \u548c\u522b\u540d\u4e24\u5217\u3002"

        # 过滤空行
        df = df.dropna(subset=[node_col, alias_col])
        dict_mapping = dict(zip(df[node_col].astype(str), df[alias_col].astype(str)))

        # 准备"标识符 -> 全量ID"的映射表
        all_nodes = dm.get_node_list()
        short_id_to_full = {}
        for node in all_nodes:
            full_id = node.get('node_id', '')
            match = re.search(r';[isgb]=(.+)', full_id)
            if match:
                short_id_to_full[match.group(1)] = full_id
            # 同时也保留全量映射以支持直接匹配
            short_id_to_full[full_id] = full_id

        imported_count = 0
        valid_aliases = {}
        for nid, alias in dict_mapping.items():
            # 使用 pandas 的 notna 检查，而非字符串比较
            if nid and pd.notna(alias) and str(alias).lower() != 'nan':
                # 先尝试短 ID 匹配，再尝试原始匹配
                # 兼容 Excel 将数字 ID 读取为 '1001.0' 的情况
                # 使用 rstrip 正确处理，如 "1.0" -> "1", "1.10" -> "1.1"
                nid_clean = nid
                if '.' in nid:
                    parts = nid.split('.')
                    if parts[1] == '0':
                        nid_clean = parts[0]

                target_full_id = short_id_to_full.get(nid_clean) or short_id_to_full.get(nid)
                if target_full_id:
                    valid_aliases[target_full_id] = alias
                    imported_count += 1

        # 批量写入，一次性持久化，避免 N 次磁盘 IO
        if valid_aliases:
            dm.pm.batch_set_aliases(valid_aliases)
            # 同步更新内存中的 alias 缓存
            dm.aliases.update(valid_aliases)
            for nid, alias in valid_aliases.items():
                if nid in dm.nodes:
                    dm.nodes[nid]['alias'] = alias
                    dm.mark_dirty(nid)  # 使用 DataManager 方法标记脏节点

        global_logger.info(f"Successfully imported {imported_count} aliases from {file_path}")
        return imported_count, f"\u6210\u529f\u5bfc\u5165 {imported_count} \u4e2a\u70b9\u4f4d\u522b\u540d\uff01"

    except Exception as e:
        global_logger.error(f"Failed to parse excel {file_path}: {e}")
        return 0, f"\u8bfb\u53d6 Excel \u6587\u4ef6\u5931\u8d25: {str(e)}"
