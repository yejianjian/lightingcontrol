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

        if len(df.columns) == 0:
            return 0, "Excel文件中没有任何列，至少需要包含 Node ID 和别名两列。"

        # [修复] 列名识别由「先到先得的宽松关键字匹配」改为「按置信度打分取最优」：
        # 1) 原先 `'节点' in col` 这类宽松规则会先命中「节点名称」列，把真正的 Node ID 列挤掉；
        # 2) 原先只要有一列没猜出来，就会把 node_col/alias_col 一起重置为第 1/2 列，
        #    于是 [Node ID, 数据类型] 这种表会把「Boolean/Real」当成每一行的别名写库，
        #    既覆盖了用户已保存的别名，还向上层返回了「导入成功」。
        def _norm_col(col) -> str:
            return str(col).strip().lower().replace(' ', '').replace('_', '').replace('-', '')

        node_exact = {"nodeid", "节点id", "节点编号", "节点标识", "节点地址", "标识", "id"}
        alias_exact = {"别名", "备注名", "备注名/别名", "备注", "节点名称",
                       "alias", "name", "remark", "comment"}

        def _node_score(col) -> int:
            """Node ID 列置信度：精确列名 > 含 node > 含 id > 宽松的含「节点」。"""
            norm = _norm_col(col)
            col_lower = str(col).lower()
            if norm in node_exact:
                return 3
            if 'node' in col_lower:
                return 2
            if 'id' in col_lower:
                return 1
            if '节点' in col_lower:
                # 宽松兜底：如「节点名称」，只有在没有更优候选时才可能被选中
                return 1
            return 0

        def _alias_score(col) -> int:
            """别名列置信度：精确列名 > 含 别名/备注/name > 含「名称/描述」。"""
            norm = _norm_col(col)
            col_lower = str(col).lower()
            if norm in alias_exact:
                return 3
            if '别名' in col_lower or '备注' in col_lower or 'alias' in col_lower or 'name' in col_lower:
                return 2
            if '名称' in col_lower or '描述' in col_lower:
                return 1
            return 0

        # 先选别名列（取置信度最高者），避免 Node ID 一侧把「节点名称」这类列抢走
        best_alias_score = 0
        for col in df.columns:
            score = _alias_score(col)
            if score > best_alias_score:
                best_alias_score = score
                alias_col = col

        # 再从未被占用的列中选 Node ID 列
        best_node_score = 0
        for col in df.columns:
            if col == alias_col:
                continue
            score = _node_score(col)
            if score > best_node_score:
                best_node_score = score
                node_col = col

        if not node_col and not alias_col:
            # 两列都完全无法识别时，保留原有的位置兜底：第 1 列 Node ID、第 2 列别名
            if len(df.columns) >= 2:
                node_col = df.columns[0]
                alias_col = df.columns[1]
            else:
                return 0, "Excel文件格式不符，至少需要包含 Node ID 和别名两列。"

        # [修复] 位置兜底只允许用于「尚未识别出来」的列，绝不覆盖已经识别成功的 Node ID 列
        if not node_col:
            for col in df.columns:
                if col != alias_col:
                    node_col = col
                    break

        # [修复] 别名列确实找不到时直接失败，禁止再靠位置猜测（否则会把「数据类型」列当别名写库）。
        # 调用方（ui/tabs/tab_monitor.py）以 count > 0 判定成功，因此这里返回 0 + 明确提示。
        if not alias_col:
            return 0, ("未识别到别名列：Excel 需包含「别名 / 备注 / 名称 / Alias / Name」等列名。"
                       "为避免覆盖已保存的别名，本次未导入任何数据，请检查表头后重试。")

        # Node ID 列同样必须存在，否则无法与系统内点位一一对应
        if not node_col or node_col == alias_col:
            return 0, ("未识别到 Node ID 列：Excel 需包含「Node ID / 节点标识」等列名。"
                       "本次未导入任何数据，请检查表头后重试。")

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

        # 批量写入，一次性持久化，避免 N 次磁盘 IO。
        # 本函数运行在后台线程（run_in_executor），必须走 DataManager 的加锁批量接口，
        # 不能直接改 dm.nodes / dm.aliases，否则会与订阅回调并发写同一字典。
        if valid_aliases:
            dm.batch_apply_aliases(valid_aliases)

        global_logger.info(f"Successfully imported {imported_count} aliases from {file_path}")
        return imported_count, f"成功导入 {imported_count} 个点位别名！"

    except Exception as e:
        global_logger.error(f"Failed to parse excel {file_path}: {e}")
        return 0, f"读取 Excel 文件失败: {str(e)}"
