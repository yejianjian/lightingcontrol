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
            if 'node' in col_lower or 'id' in col_lower or '节点' in col_lower:
                if not node_col: node_col = col
            if '别名' in col_lower or '备注' in col_lower or 'alias' in col_lower or 'name' in col_lower:
                if not alias_col and col != node_col: alias_col = col

        if not node_col or not alias_col:
             # 如果猜不出，尝试强制使用第1列为NodeID，第2列为别名
             if len(df.columns) >= 2:
                 node_col = df.columns[0]
                 alias_col = df.columns[1]
             else:
                 return 0, "Excel文件格式不符，至少需要包含 Node ID 和别名两列。"

        # 过滤空行
        df = df.dropna(subset=[node_col, alias_col])
        dict_mapping = dict(zip(df[node_col].astype(str), df[alias_col].astype(str)))
        
        # 准备“标识符 -> 全量ID”的映射表
        all_nodes = dm.get_node_list()
        short_id_to_full = {}
        for node in all_nodes:
            full_id = node.get('node_id', '')
            match = re.search(r'[isgb]=(.+)', full_id)
            if match:
                short_id_to_full[match.group(1)] = full_id
            # 同时也保留全量映射以支持直接匹配
            short_id_to_full[full_id] = full_id

        imported_count = 0
        valid_aliases = {}
        for nid, alias in dict_mapping.items():
            if nid and alias and alias.lower() != 'nan':
                # 先尝试短 ID 匹配，再尝试原始匹配
                # 兼容 Excel 将数字 ID 读取为 '1001.0' 的情况
                nid_clean = nid.split('.')[0] if '.' in nid and nid.split('.')[1] == '0' else nid
                
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
                    dm.dirty_nodes.add(nid)
                
        global_logger.info(f"Successfully imported {imported_count} aliases from {file_path}")
        return imported_count, f"成功导入 {imported_count} 个点位别名！"

    except Exception as e:
        global_logger.error(f"Failed to parse excel {file_path}: {e}")
        return 0, f"读取 Excel 文件失败: {str(e)}"
