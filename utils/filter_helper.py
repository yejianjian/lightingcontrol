"""
公共类型筛选与关键字过滤工具
将 tab_monitor 和 tab_group 中的重复逻辑抽取到此处
"""

# 数据类型映射表：将 UI 显示名映射为 asyncua 底层类型关键字
TYPE_MAP = {
    "boolean": ["bool", "boolean"],
    "uint": ["uint", "byte"],
    "int": ["int", "sbyte"],
    "real": ["float", "double", "real"],
    "string": ["string", "str", "localizedtext"]
}


def filter_nodes(all_nodes, keyword="", type_filter="全部数据类型"):
    """
    根据关键字和数据类型筛选节点列表。

    Args:
        all_nodes: 所有节点字典列表
        keyword: 搜索关键字（匹配 alias 或 node_id）
        type_filter: 类型筛选条件（UI 下拉框文本）

    Returns:
        过滤后的节点列表
    """
    tf_lower = type_filter.lower()
    # "全部数据类型" 和 "全部类型" 都视为不过滤
    is_filter_all = "全部" in type_filter

    allowed = TYPE_MAP.get(tf_lower, [tf_lower])

    # 特殊处理 Int/UInt 区分：Int 只匹配带 int 但不带 uint 的类型
    is_int_filter = tf_lower == 'int'

    filtered = []
    for n in all_nodes:
        # 类型筛检
        if not is_filter_all:
            n_type = str(n.get('type', '')).lower()
            matched = False
            for t in allowed:
                if t in n_type:
                    # 规避 Int 被 UInt 误匹配：Int 过滤器排除 uint 类型
                    if is_int_filter and 'uint' in n_type:
                        continue
                    matched = True
                    break
            if not matched:
                continue

        # 关键词筛检
        if keyword:
            kw = keyword.lower()
            alias = str(n.get('alias', n.get('name', ''))).lower()
            node_id = str(n.get('node_id', '')).lower()
            if kw not in alias and kw not in node_id:
                continue

        filtered.append(n)

    return filtered
