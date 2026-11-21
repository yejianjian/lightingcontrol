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


def type_matches(type_value, filter_text):
    """判断点位的数据类型是否命中 UI 类型筛选。

    供分组页的代理模型（SearchProxyModel）等无法直接拿到节点字典的场景复用，
    语义与 filter_nodes 完全一致：

    - Real 命中 Float / Double / Real（DA 的类型名是 Float/Double）；
    - Int 命中 Int16 等有符号整数，但**排除** UInt16 等无符号类型，
      避免 Int 子串误匹配 UInt；
    - "全部类型"/"全部数据类型" 或空筛选恒为 True。

    Args:
        type_value: 点位的底层数据类型文本（大小写不敏感），如 Float/Double/Int16。
        filter_text: UI 下拉框文本，如 Boolean/UInt/Int/Real/String/全部类型。

    Returns:
        True 表示该点位应显示；False 表示被类型筛选排除。
    """
    if not filter_text or "全部" in str(filter_text):
        return True

    tf_lower = str(filter_text).strip().lower()
    allowed = TYPE_MAP.get(tf_lower, [tf_lower])
    # 特殊处理 Int/UInt 区分：Int 只匹配带 int 但不带 uint 的类型
    is_int_filter = tf_lower == "int"

    type_lower = str(type_value or "").lower()
    if not type_lower:
        return False

    for t in allowed:
        if t in type_lower:
            # 规避 Int 被 UInt 误匹配：Int 过滤器排除 uint 类型
            if is_int_filter and "uint" in type_lower:
                continue
            return True
    return False


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
    # "全部数据类型" 和 "全部类型" 都视为不过滤
    is_filter_all = "全部" in type_filter

    filtered = []
    for n in all_nodes:
        # 类型筛检（复用 type_matches，保证分组页/监控页的类型语义一致）
        if not is_filter_all and not type_matches(n.get('type', ''), type_filter):
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
