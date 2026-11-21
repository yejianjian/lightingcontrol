"""OPC DA（COM/DCOM）通信引擎。

本模块是灯光控制系统与 OPC DA 服务器之间的唯一通信接缝。上层 UI、DataManager、
GroupScheduler 只依赖 OpcDaClientEngine 的少数几个方法，其签名与行为刻意与
UA 版 opc/client_engine.py::OpcClientEngine 对齐，因此监控/分组/调度/日志页面
可以原样复用，只有设置页需要换成 DA 版（DCOM 主机 + ProgID）。

设计要点（与 UA 版对齐）：

1. 单一状态机：所有状态变更都经 _set_state 收敛，connected 只是只读投影，
   避免多处竞态改写布尔标志位。状态字符串与 UA 版完全一致：
   disconnected / connecting / loading / connected / reconnecting / disconnecting。
2. 引擎自治重连：断线后由引擎自己的 supervisor 协程按指数退避 + 随机抖动重试，
   UI 不再自己 sleep 重试；_auto_reconnect 是唯一的"自动重连总闸"。
3. 世代号（generation）：每个底层 OpcDaClient 实例关联一个单调递增的世代号，
   旧世代的订阅回调、心跳任务、轮询任务一律被忽略，杜绝"僵尸连接"污染新连接。
4. 重连复用点位表：重连时不重新 Browse，直接拿缓存的 ItemID 重新 AddItems、
   重新订阅、重新读初值，然后触发 on_nodes_recovered。Browse 一个上千点的
   地址空间可能要几十秒，断线抖动时反复 Browse 是最常见的"重连卡死"来源。
5. 健康判据基于链路而非数据变化：灯光点位可能数小时不变，因此断线检测只依赖
   周期 get_server_status()（不超过 10s）、订阅/读写异常与 is_connected()，
   绝不把"没有数据推送"当作断线。
6. 高兼容性：老 DA1 服务器可能不支持 IOPCAsyncIO2。start_subscription 失败时
   自动降级为 SyncRead 轮询（每 update_rate_ms 读一次全表），保证功能不丢。
7. 无 Qt 也可测试：引擎不 import 任何 Qt 模块；底层客户端通过 client_factory
   注入，测试用假 client 即可覆盖状态机/重连/批量写/点位发布。数据变化回调由
   client.pump_events() 在心跳循环里泵出（Qt 下由 Qt 事件循环泵）。

线程模型：底层 opcda.client 是同步 COM 封装，必须在创建它的线程（GUI 主线程/
STA）上调用。因此本引擎不做 run_in_executor，所有 client 调用都发生在事件循环
所在线程；耗时操作（Browse/AddItems/Read）按批（300 个点位）之间 await
asyncio.sleep(0) 让出事件循环，避免界面长时间无响应。
"""

import asyncio
import random
import time

from utils.logger import global_logger
from . import com


# --------------------------------------------------------------------------------------
# 常量与超时预算
# --------------------------------------------------------------------------------------

DEFAULT_PROG_ID = "Matrikon.OPC.Simulation.1"

# 底层 COM 建链超时（秒）。CreateObject + QI IOPCServer + SetLocaleID。
CONNECT_TIMEOUT = 20.0
# 等待后台任务退出的时间预算（秒）
TASK_CANCEL_GRACE = 1.0

# 更新周期（毫秒）：订阅/轮询的采样间隔，可由设置页注入。
DEFAULT_UPDATE_RATE_MS = 500
MIN_UPDATE_RATE_MS = 100
MAX_UPDATE_RATE_MS = 60000

# 地址空间浏览深度
DEFAULT_BROWSE_MAX_DEPTH = 3
MIN_BROWSE_MAX_DEPTH = 1
MAX_BROWSE_MAX_DEPTH = 12

# AddItems / SyncRead 的分批大小：每批之间让出一轮事件循环。
ADD_BATCH_SIZE = 300
READ_BATCH_SIZE = 500
POLL_READ_BATCH_SIZE = 500

# 心跳查询 get_server_status 的最大/最小周期（秒）。契约要求"不超过 10s"。
HEARTBEAT_MAX_INTERVAL = 10.0
HEARTBEAT_MIN_INTERVAL = 1.0
# 会话保活窗口（毫秒），仅用于推导心跳周期，与 UA 版的同名属性保持兼容。
DEFAULT_SESSION_TIMEOUT_MS = 60_000

# 自动重连退避参数（秒）
DEFAULT_RECONNECT_BASE_DELAY = 10.0
DEFAULT_RECONNECT_MAX_DELAY = 60.0
# 抖动比例：避免多台集控终端同一时刻惊群式重连
RECONNECT_JITTER = 0.2
# 退避地板的绝对值（秒）：允许测试把 base/max 调得很小而不至于 0 轮空转
MIN_RECONNECT_DELAY = 0.01

# OPC DA 服务器状态码（OPCSERVERSTATE）
# OPC 服务器状态码：统一引用 com.py 的规范常量。
# 旧实现本地定义与 OPC 规范错位（把 3 当 NODATA、5 当 NOCONFIG），且只识别
# FAILED，通信故障（COMM_FAULT）等异常状态完全无告警。
_OPC_STATE_RUNNING = com.OPC_STATUS_RUNNING
_OPC_STATE_FAILED = com.OPC_STATUS_FAILED
_OPC_STATE_NOCONFIG = com.OPC_STATUS_NOCONFIG
_OPC_STATE_SUSPENDED = com.OPC_STATUS_SUSPENDED
_OPC_STATE_TEST = com.OPC_STATUS_TEST
_OPC_STATE_COMM_FAULT = com.OPC_STATUS_COMM_FAULT

# OPC DA 品质：高两位是质量主状态，Good = 0b11（0xC0），Bad = 0b00。
_QUALITY_GOOD_MASK = 0xC0
_QUALITY_GOOD_VALUE = 0xC0

# VARENUM（VT_*）到契约约定的类型名。
# 契约要求的类型名：Boolean/Int16/Int32/UInt16/UInt32/Int64/UInt64/Float/Double/String/Unknown。
_VT_TYPE_NAMES = {
    2: "Int16",
    3: "Int32",
    4: "Float",
    5: "Double",
    6: "Currency",
    7: "Date",
    8: "String",
    9: "Object",
    10: "Error",
    11: "Boolean",
    12: "Variant",
    13: "Unknown",
    16: "Int8",
    17: "UInt8",
    18: "UInt16",
    19: "UInt32",
    20: "Int64",
    21: "UInt64",
    22: "Int",
    23: "UInt",
    24: "Void",
    25: "HResult",
    26: "Pointer",
}

# 模块级缓存的 vtype 转类型名函数（来自 opcda.client，失败则用本地表）
_VTYPE_CONVERTER = None


def _vtype_name(vt):
    """把 VARENUM 转成契约约定的类型名。

    优先使用 opcda.client.vtype_to_type_name（与底层实现保持一致），
    导入失败时退回本地 _VT_TYPE_NAMES（单测/无 COM 环境同样可用）。
    """
    global _VTYPE_CONVERTER
    try:
        code = int(vt or 0)
    except (TypeError, ValueError):
        code = 0
    if _VTYPE_CONVERTER is None:
        # 优先用 client 再导出的转换函数；T1 也可能只放在 opcda.com 里。
        try:
            from opcda.client import vtype_to_type_name as _conv
            _VTYPE_CONVERTER = _conv
        except Exception:
            try:
                from opcda.com import vtype_to_type_name as _conv
                _VTYPE_CONVERTER = _conv
            except Exception:
                _VTYPE_CONVERTER = False
    if callable(_VTYPE_CONVERTER):
        try:
            return str(_VTYPE_CONVERTER(code))
        except Exception:
            pass
    return _VT_TYPE_NAMES.get(code, "Unknown")


def _python_type_name(value):
    """Python 值转与 UA 版兼容的类型名（仅用于初值兜底）。"""
    if isinstance(value, bool):
        return "Boolean"
    if isinstance(value, int):
        return "Int32"
    if isinstance(value, float):
        return "Float"
    if isinstance(value, str):
        return "String"
    if value is None:
        return "Unknown"
    return type(value).__name__


def _quality_is_good(quality):
    """OPC 品质是否 Good。

    OPC DA 品质字的高两位是质量主状态：Good=0b11（0xC0）、Uncertain=0b01（0x40）、
    Bad=0b00（0x00），低 6 位是子状态。因此 Good 的判定是 (q & 0xC0) == 0xC0，
    不能写成 == 0（那会把 Bad 当成 Good）。
    """
    try:
        return (int(quality) & _QUALITY_GOOD_MASK) == _QUALITY_GOOD_VALUE
    except (TypeError, ValueError):
        return True


def _hresult_ok(hresult):
    """HRESULT 成功判定（S_OK 与 S_FALSE 等正数码都算成功）。"""
    if hresult is None:
        return False
    try:
        return com.succeeded(int(hresult))
    except (TypeError, ValueError):
        return False


def _is_write_value_error(error):
    """判断写失败是否只是"值类型无法转换"，而非链路故障。

    底层客户端对数组/无法安全转换的点位会抛 OpcDaError('unsupported data type for
    write...')。这类错误与连接健康无关，不能计入连续写失败（否则往数组点写几次就会
    被误判为断线并触发整链重连）。
    """
    text = str(error).lower()
    markers = ("unsupported data type", "unsupported conversion", "cannot convert",
               "conversion", "无法安全转换", "类型转换")
    return any(marker in text for marker in markers)


def _default_client_factory(prog_id, host):
    """默认客户端工厂：延迟导入 opcda.client。

    延迟导入有两个好处：无 COM 环境（自动化测试）可以完全不碰 comtypes；
    opcda.client 由并行的 T1 交付，引擎模块本身不必等到它存在才能 import。
    """
    from opcda.client import OpcDaClient

    return OpcDaClient(prog_id, host=host)


class ConnectionSuperseded(Exception):
    """本次连接尝试已被更新的世代取代（用户断开或重新发起连接）。

    内部控制流异常：表示"放弃当前这一轮"，不是网络故障，因此不上报断线、不触发重连。
    """


class OpcDaClientEngine:
    """OPC DA 客户端引擎（含自治重连与订阅/轮询降级）。

    对调用方可见的接口（与 UA 版 OpcClientEngine 对齐）::

        engine.configure(prog_id=..., host=..., update_rate_ms=..., browse_max_depth=...)
        engine.on_state_change = fn(state: str, detail: dict)
        engine.on_nodes_recovered = fn()
        engine.on_connection_lost = fn()
        engine.set_subscription_callback(fn)     # fn(node_id, value, timestamp)
        await engine.connect()
        await engine.bootstrap()                 # 浏览 + AddItems + 读初值 + 订阅
        await engine.reconnect_forever()         # 打开自治重连总闸
        await engine.disconnect()                # 幂等、有界
        await engine.write_node_value(node_id, value)
        await engine.write_values_batch(node_ids, value, display_names=None)
        engine.nodes                             # node_id -> {node_id,name,type,value,timestamp,...}
        engine.connected                         # 状态机投影
    """

    # 状态常量（字符串必须与 UA 版完全一致，UI 的 _apply_state 直接按字符串分支）
    STATE_DISCONNECTED = "disconnected"
    STATE_CONNECTING = "connecting"
    STATE_LOADING = "loading"
    STATE_CONNECTED = "connected"
    STATE_RECONNECTING = "reconnecting"
    STATE_DISCONNECTING = "disconnecting"

    def __init__(self, host="localhost", prog_id=DEFAULT_PROG_ID,
                 update_rate_ms=DEFAULT_UPDATE_RATE_MS,
                 browse_max_depth=DEFAULT_BROWSE_MAX_DEPTH,
                 client_factory=None):
        self.host = host or "localhost"
        self.prog_id = prog_id or DEFAULT_PROG_ID
        self.port = 0  # OPC DA 没有端口概念，保留字段仅为与 UA 版接口兼容
        self.update_rate_ms = self._clamp_update_rate(update_rate_ms)
        self.browse_max_depth = self._clamp_browse_depth(browse_max_depth)
        self.url = self._build_url()

        # 底层客户端与点位表
        self.client = None
        self.nodes = {}
        self.subscription = None          # "async" / "poll" / None（诊断用）
        # 自动扫描之外、由用户手工添加的 ItemID（持久化在 da_manual_nodes）。
        # 每次连接/重连时并入 nodes，与自动扫描点位一起 AddItems/订阅。
        self.manual_items = []             # [{"item_id": str, "name": str}, ...]
        self._browse_node_ids = set()      # 最近一次自动扫描命中的 ItemID（用于区分手工点）

        # 客户端工厂（测试注入假 client；默认延迟导入 opcda.client）
        self._client_factory = client_factory

        self._state = self.STATE_DISCONNECTED
        self._generation = 0
        # Python 3.8 的 asyncio.Lock() 在没有事件循环的线程里构造会抛 RuntimeError，
        # 而 UI 会在事件循环启动前就构造引擎，所以惰性创建。
        self._client_lock = None
        self._disconnect_lock = None

        # 订阅/回调
        self._sub_callback = None
        # 回调接收 quality 的方式："positional"（4+ 位置参数/*args）、
        # "keyword"（3 参数 + **kwargs）或 "none"（旧三参回调）。
        self._sub_cb_quality_mode = "none"
        self._subscribed_items = []
        self._subscription_mode = None
        # 客户端句柄与 ItemID 的映射（订阅回调只给 hClient，需要此表还原点位）
        self._item_handles = {}
        self._handle_to_item = {}
        # AddItems 状态：记录为哪个世代/客户端添加过，避免重复 AddItems
        self._items_added_gen = -1
        self._items_added_client = None
        # 非 Good 品质的去重日志集合，避免坏点刷屏
        self._bad_quality_items = set()

        # 后台任务
        self._monitor_task = None
        self._poll_task = None
        self._reconnect_task = None
        self._pending_reconnect_forever = None
        # 当前是否处于用户/流程发起的 connect() 内；以及当前任务是否为重连 supervisor。
        # 这两个标志用于避免"手动连接刚取消 supervisor，supervisor 的 finally 又立刻
        # 把自己拉起来"的重复 supervisor 竞态（它可能在 bootstrap 期间抢到 client_lock
        # 把刚建立的连接拆掉）。
        self._connecting = False
        self._in_supervisor = False
        # 是否正在执行 bootstrap()（浏览/AddItems/读初值/订阅）。supervisor 看到
        # 装配进行中要主动让路，避免它 teardown 掉正在装配的 client。
        self._loading = False
        self._monitor_task_created_at = 0.0
        self._monitor_started_at = 0.0
        self._poll_task_created_at = 0.0
        self._poll_started_at = 0.0
        # ensure_liveness() 补拉任务的强引用：asyncio 事件循环对 Task 只保留弱引用，
        # 不保存引用时任务可能在跑到第一行之前就被 GC 回收（Python 3.8 文档明确要求）。
        self._liveness_tasks = set()

        # 时间戳（诊断/心跳）
        self._connected_at = 0.0
        self._last_data_at = 0.0
        self._last_heartbeat_at = 0.0
        # 最近一次已告警的异常服务器状态（避免每 10s 重复刷同一条 WARNING）
        self._last_abnormal_server_state = None
        # 心跳周期覆盖值（秒）。默认 None 表示由 session_timeout_ms 推导；测试可调小。
        self.heartbeat_interval_s = None
        # 是否由引擎主动泵 Windows 消息。
        # True：无 Qt 的自动化测试/命令行自检需要引擎泵消息，订阅回调才会执行；
        # False：Qt（qasync）主线程里 Qt 事件循环本身就会泵 COM 消息，引擎再泵一次会
        # 递归进入 Qt 事件循环，引发 asyncio "Cannot enter into task ..." 僵尸任务，
        # 因此 DaMainWindow 会把本开关关掉。
        self.pump_events_enabled = True

        # 写失败累计：连续失败达到阈值触发受控重连
        self._consecutive_write_failures = 0
        self._max_write_failures = 3

        # 重连策略（可由设置页覆盖，键名与 UA 版一致）
        self.session_timeout_ms = DEFAULT_SESSION_TIMEOUT_MS
        self.reconnect_base_delay = DEFAULT_RECONNECT_BASE_DELAY
        self.reconnect_max_delay = DEFAULT_RECONNECT_MAX_DELAY
        self._auto_reconnect = False
        self.reconnect_attempt = 0
        self.total_reconnects = 0

        # 回调钩子（UI 直接赋值）
        self.on_connection_lost = None
        self.on_state_change = None
        self.on_nodes_recovered = None

    # ----------------------------------------------------------------------------------
    # 基础属性与配置
    # ----------------------------------------------------------------------------------

    @property
    def state(self):
        return self._state

    @property
    def connected(self):
        return self._state == self.STATE_CONNECTED

    @connected.setter
    def connected(self, value):
        """兼容旧代码：engine.connected = False 等价于把状态机打到断开态。"""
        if value:
            self._state = self.STATE_CONNECTED
        elif self._state == self.STATE_CONNECTED:
            self._state = self.STATE_DISCONNECTED

    @property
    def display_endpoint(self):
        """给 UI 显示的一行端点描述。"""
        return self.url

    @staticmethod
    def _clamp_update_rate(value):
        try:
            rate = int(value)
        except (TypeError, ValueError):
            rate = DEFAULT_UPDATE_RATE_MS
        return max(MIN_UPDATE_RATE_MS, min(MAX_UPDATE_RATE_MS, rate))

    @staticmethod
    def _clamp_browse_depth(value):
        try:
            depth = int(value)
        except (TypeError, ValueError):
            depth = DEFAULT_BROWSE_MAX_DEPTH
        return max(MIN_BROWSE_MAX_DEPTH, min(MAX_BROWSE_MAX_DEPTH, depth))

    def _build_url(self):
        return "opcda://{}/{}".format(self.host, self.prog_id)

    def configure(self, prog_id=None, host=None, update_rate_ms=None,
                  browse_max_depth=None, manual_items=None, **_ignored):
        """更新连接参数。

        DA 版只关心 DCOM 主机 / ProgID / 更新周期 / 浏览深度；port、username
        等 UA 参数会被 **_ignored 静默吞掉，因此 UA 版调用方也能复用本引擎。

        目标服务器（host 或 ProgID）改变时，缓存的点位表与句柄映射失效。
        """
        endpoint_changed = False
        if host is not None:
            new_host = str(host).strip()
            if new_host and new_host != self.host:
                self.host = new_host
                endpoint_changed = True
        if prog_id is not None:
            new_prog = str(prog_id).strip()
            if new_prog and new_prog != self.prog_id:
                self.prog_id = new_prog
                endpoint_changed = True
        if update_rate_ms is not None:
            requested_rate = self._clamp_update_rate(update_rate_ms)
            if requested_rate != self.update_rate_ms:
                self.update_rate_ms = requested_rate
                # 已连接时原地 SetState 修改组更新周期（不重建组、不中断订阅）。
                # 旧实现只改本地字段，必须断开重连才生效。
                client = self.client
                apply_rate = getattr(client, "set_update_rate_ms", None)
                if self.connected and callable(apply_rate):
                    try:
                        applied = apply_rate(requested_rate)
                        if applied:
                            self.update_rate_ms = self._clamp_update_rate(applied)
                        global_logger.info(
                            "OPC DA update rate applied: requested={}ms, effective={}ms".format(
                                requested_rate, self.update_rate_ms))
                    except Exception as e:
                        global_logger.warning(
                            "应用新的更新周期失败（保持 {}ms）：{}".format(
                                self.update_rate_ms, e))
        if browse_max_depth is not None:
            self.browse_max_depth = self._clamp_browse_depth(browse_max_depth)
        if manual_items is not None:
            self.set_manual_items(manual_items)

        self.url = self._build_url()
        if endpoint_changed:
            global_logger.info(
                "OPC DA endpoint changed to {}; cached node table invalidated.".format(self.url)
            )
            self.nodes = {}
            self._subscribed_items = []
            self._item_handles.clear()
            self._handle_to_item.clear()
            self._items_added_gen = -1
            self._items_added_client = None
        return True

    @staticmethod
    def _callback_quality_mode(callback):
        """判断回调接收 quality 的方式：positional / keyword / none。

        - 4 个及以上位置参数，或含 *args：按第 4 个位置参数传 quality；
        - 恰好 3 个位置参数 + **kwargs：用关键字参数 quality= 传（避免 TypeError）；
        - 其余（含旧三参回调、内建不可签名对象）：不传 quality。
        """
        if not callable(callback):
            return "none"
        try:
            import inspect
            signature = inspect.signature(callback)
        except (TypeError, ValueError):  # C 函数/内建可调用对象：保守按三参处理
            return "none"
        positional = 0
        has_var_kwargs = False
        for param in signature.parameters.values():
            if param.kind in (param.POSITIONAL_ONLY, param.POSITIONAL_OR_KEYWORD):
                positional += 1
            elif param.kind == param.VAR_POSITIONAL:
                return "positional"
            elif param.kind == param.VAR_KEYWORD:
                has_var_kwargs = True
        if positional >= 4:
            return "positional"
        if has_var_kwargs:
            return "keyword"
        return "none"

    def set_subscription_callback(self, callback):
        """登记数据推送回调（callback(node_id, value, timestamp[, quality])）。

        重连后引擎会自动用它重建订阅；这里只保存引用。

        质量码 quality 是监控页显示「通信异常」/恢复正常的唯一实时来源：若回调
        能接收第 4 个参数就一并推送；旧的三参回调保持原有调用语义。
        """
        self._sub_callback = callback
        self._sub_cb_quality_mode = self._callback_quality_mode(callback)

    # ----------------------------------------------------------------------------------
    # 手动添加点位（自动扫描之外的补充）
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _normalize_manual_items(items):
        """把配置里的手动点位规范化为 [{'item_id','name'}]。

        兼容三种历史/手工写法：字符串 ItemID、{"item_id","name"}、{"id","name"}。
        空值/重复 ItemID 会被丢弃，保证 AddItems 不会重复。
        """
        normalized = []
        seen = set()
        for entry in items or []:
            if isinstance(entry, str):
                item_id = entry.strip()
                name = item_id
            elif isinstance(entry, dict):
                item_id = str(entry.get("item_id") or entry.get("id") or "").strip()
                name = str(entry.get("name") or item_id).strip() or item_id
            else:
                continue
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            normalized.append({"item_id": item_id, "name": name})
        return normalized

    def set_manual_items(self, items):
        """更新手动添加点位列表（只影响配置，不主动联网；下次 bootstrap 自动并入）。"""
        self.manual_items = self._normalize_manual_items(items)
        return [dict(item) for item in self.manual_items]

    @staticmethod
    def _manual_node_placeholder(item_id, name):
        """尚未完成 AddItems 的手动点位占位节点，类型/值由后续 AddItems+SyncRead 填充。"""
        display = str(name or item_id)
        return {
            "node_id": item_id,
            "name": display,
            "alias": display,
            "type": "Unknown",
            "value": None,
            "timestamp": "",
            "path": "",
            "item_id": item_id,
            "canonical_vt": 0,
            "access": 0,
            "writable": False,
            "manual": True,
        }

    def _merge_manual_items_into_nodes(self):
        """把手动点位并入当前点位表（已在表内的只更新显示名）。"""
        # 配置中已经删除的手动点位，必须从 nodes 里清掉；否则断线期间删除的点位
        # 会在下一次 bootstrap 的 AddItems 中"复活"。
        manual_ids = {spec["item_id"] for spec in self.manual_items}
        for item_id, node in list(self.nodes.items()):
            if node.get("manual") and item_id not in manual_ids:
                self.nodes.pop(item_id, None)
                handle = self._item_handles.pop(item_id, None)
                if handle is not None:
                    self._handle_to_item.pop(handle, None)
        added = 0
        for spec in self.manual_items:
            item_id = spec["item_id"]
            name = spec.get("name") or item_id
            node = self.nodes.get(item_id)
            if node is None:
                node = self._manual_node_placeholder(item_id, name)
                self.nodes[item_id] = node
                added += 1
            else:
                if spec.get("name"):
                    node["name"] = spec["name"]
                    node["alias"] = spec["name"]
                node["manual"] = True
        if added:
            global_logger.info(
                "Manual OPC DA point(s) merged into node table: {} added, {} total manual.".format(
                    added, len(self.manual_items)))
        return added

    async def add_manual_node(self, item_id, name=None):
        """在已连接状态下立即挂载一个手工点位，并读取一次初值。

        - 会同步写入 self.manual_items，保证断线重连/下次启动继续挂载；
        - 新点位通过客户端 AddItems 加入现有 group：异步订阅会自动开始推送，
          轮询降级模式下也会在下一轮 SyncRead 中被包含；
        - 返回点位字典（与 engine.nodes[item_id] 一致）；失败抛异常，由 UI 提示。
        """
        if not self.connected and self._state != self.STATE_LOADING:
            raise ConnectionError("add_manual_node() requires an established connection")
        item_id = str(item_id or "").strip()
        if not item_id:
            raise ValueError("ItemID 不能为空")
        client = self.client
        if client is None:
            raise ConnectionError("add_manual_node() requires a connected client")

        my_gen = self._generation
        try:
            results = client.add_items([item_id], update_rate_ms=self.update_rate_ms) or []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            raise ConnectionError(
                "手动添加点位失败（{}）: {}".format(item_id, e)) from e
        if not results:
            errors = getattr(client, "last_add_errors", None)
            raise ConnectionError(
                "手动添加点位失败（{}）: 服务器拒绝或 ItemID 不存在{}".format(
                    item_id, "（{}）".format(errors[:3]) if errors else ""))

        item = results[0]
        node = self._node_from_da_item(item) or self._manual_node_placeholder(item_id, name)
        if name:
            node["name"] = str(name)
            node["alias"] = str(name)
        node["manual"] = True
        old_node = self.nodes.get(item_id)
        if old_node is not None and old_node.get("value") is not None and node.get("value") is None:
            node["value"] = old_node.get("value")
            node["timestamp"] = old_node.get("timestamp", "")
        self.nodes[item_id] = node

        try:
            handle = int(getattr(item, "client_handle", 0) or 0)
        except (TypeError, ValueError):
            handle = 0
        if handle:
            self._item_handles[item_id] = handle
            self._handle_to_item[handle] = item_id

        # 初值读取：失败不影响挂载（订阅/心跳会继续更新）
        try:
            values = client.read_items([item_id], source=1) or []
            if values:
                self._apply_values(values, my_gen, dispatch=False)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            global_logger.warning(
                "手动点位 {} 已挂载，但读取初值失败：{}".format(item_id, e))

        if not any(spec.get("item_id") == item_id for spec in self.manual_items):
            self.manual_items.append({"item_id": item_id, "name": str(name or node.get("name") or item_id)})

        # 首次连接时若自动扫描为 0 且没有手动点位，bootstrap() 会跳过订阅/轮询
        # （不挂回调、不启 poll）。之后手动新增点位必须补启动，否则该点位后续的
        # 值永远不会自动刷新（界面停在初值）。
        if self._subscription_mode is None and self._sub_callback is not None:
            try:
                await self.start_subscription(self._sub_callback)
            except asyncio.CancelledError:
                raise
            except ConnectionSuperseded:
                raise
            except Exception as e:
                global_logger.warning(
                    "手动点位 {} 已挂载，但补启动订阅/轮询失败：{}".format(item_id, e))

        global_logger.info(
            "[MANUAL] Added OPC DA item {} (name={}, type={}, value={!r}).".format(
                item_id, node.get("name"), node.get("type"), node.get("value")))
        await asyncio.sleep(0)
        return dict(node)

    async def remove_manual_node(self, item_id):
        """从手动列表和当前点位表移除一个点位；已连接时同时从服务器组中 RemoveItems。"""
        item_id = str(item_id or "").strip()
        if not item_id:
            return False
        client = self.client
        if client is not None and (self.connected or self._state == self.STATE_LOADING):
            remove = getattr(client, "remove_items", None)
            if callable(remove):
                try:
                    remove([item_id])
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    global_logger.warning(
                        "从 OPC DA group 移除 {} 失败（仅从界面/配置移除）：{}".format(item_id, e))
        node = self.nodes.pop(item_id, None)
        handle = self._item_handles.pop(item_id, None)
        if handle is not None:
            self._handle_to_item.pop(handle, None)
        if item_id in self._subscribed_items:
            try:
                self._subscribed_items.remove(item_id)
            except ValueError:
                pass
        before = len(self.manual_items)
        self.manual_items = [spec for spec in self.manual_items
                             if spec.get("item_id") != item_id]
        if node is not None or len(self.manual_items) != before:
            global_logger.info("[MANUAL] Removed OPC DA item {}.".format(item_id))
        await asyncio.sleep(0)
        return node is not None or len(self.manual_items) != before

    def _set_state(self, new_state, reason="", **detail):
        if self._state == new_state:
            return
        old = self._state
        self._state = new_state
        payload = {"previous": old, "reason": reason}
        payload.update(detail)
        global_logger.info("[STATE] {} -> {} ({})".format(old, new_state, reason or "n/a"))
        cb = self.on_state_change
        if callable(cb):
            try:
                cb(new_state, payload)
            except Exception as e:
                global_logger.error("on_state_change callback error: {}".format(e), exc_info=True)

    def _status_detail(self):
        return {
            "attempt": self.reconnect_attempt,
            "total_reconnects": self.total_reconnects,
            "host": self.host,
            "prog_id": self.prog_id,
        }

    def _ensure_locks(self):
        """惰性创建并校验 asyncio 锁（Python 3.8 / 跨事件循环安全）。"""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if self._client_lock is None:
            self._client_lock = asyncio.Lock()
        if self._disconnect_lock is None:
            self._disconnect_lock = asyncio.Lock()
        for name in ("_client_lock", "_disconnect_lock"):
            lock = getattr(self, name)
            bound = getattr(lock, "_loop", None)
            if running is not None and bound is not None and bound is not running:
                global_logger.warning(
                    "{} was bound to a different event loop; recreating it.".format(name)
                )
                setattr(self, name, asyncio.Lock())

    def _link_usable(self):
        """链路是否处于连接可用/装填中状态。

        与 UA 版同理：心跳/轮询任务在 LOADING 阶段就已启动，判据不能只用 connected。
        """
        return self._state in (self.STATE_LOADING, self.STATE_CONNECTED)

    def _heartbeat_interval(self):
        if self.heartbeat_interval_s is not None:
            try:
                return max(0.05, float(self.heartbeat_interval_s))
            except (TypeError, ValueError):
                pass
        try:
            derived = float(self.session_timeout_ms) / 2000.0
        except (TypeError, ValueError):
            derived = HEARTBEAT_MAX_INTERVAL
        return max(HEARTBEAT_MIN_INTERVAL, min(HEARTBEAT_MAX_INTERVAL, derived))

    # ----------------------------------------------------------------------------------
    # 客户端创建 / 清理
    # ----------------------------------------------------------------------------------

    def _create_client(self):
        factory = self._client_factory or _default_client_factory
        return factory(self.prog_id, self.host)

    def _pump_client_events(self):
        """泵一次 Windows 消息，让 comtypes 的订阅回调有机会执行。

        Qt 主线程里 Qt 自己的事件循环会泵消息，此时必须由上层把
        pump_events_enabled 置 False（DaMainWindow 已处理），否则递归泵 Qt 消息
        会造成事件循环重入；无 Qt 的自动化测试/自检里则必须由引擎心跳/轮询负责泵。
        """
        if not self.pump_events_enabled:
            return
        client = self.client
        if client is None:
            return
        pump = getattr(client, "pump_events", None)
        if callable(pump):
            try:
                pump()
            except Exception as e:
                global_logger.debug("pump_events failed (non-fatal): {}".format(e))

    async def _cleanup_client(self):
        """断开并释放当前客户端引用（幂等、同步、绝不挂起）。"""
        client = self.client
        if client is None:
            self._items_added_gen = -1
            self._items_added_client = None
            return
        try:
            set_cb = getattr(client, "set_data_change_callback", None)
            if callable(set_cb):
                set_cb(None)
        except Exception as e:
            global_logger.debug("reset data change callback failed (non-fatal): {}".format(e))
        try:
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()
        except Exception as e:
            global_logger.debug("client.disconnect failed (non-fatal): {}".format(e))
        self.client = None
        self._items_added_gen = -1
        self._items_added_client = None
        self._item_handles.clear()
        self._handle_to_item.clear()

    # ----------------------------------------------------------------------------------
    # 连接
    # ----------------------------------------------------------------------------------

    async def connect(self):
        """建立到 OPC DA 服务器的 COM 连接。

        并发调用被串行化；比新一次调用更旧的调用会抛 ConnectionSuperseded。
        """
        self._ensure_locks()
        self._connecting = True
        connected_ok = False
        try:
            async with self._client_lock:
                my_gen = self._generation + 1
                self._generation = my_gen
                # 只取消重连 supervisor，不改总闸（总闸只由用户意图改写）
                await self._stop_reconnect_supervisor()
                await self._cleanup_client()
                await self._connect_once(my_gen)
                connected_ok = True
                return True
        finally:
            self._connecting = False
            # 只有本次建链**实际失败**才重新拉起重连 supervisor。
            # 修复前用 "not self.connected" 判断：connect() 成功返回时状态是
            # STATE_LOADING（connected=False），于是每次正常连接都会多创建一个
            # supervisor（0.5s 后自行退出并打 WARNING），且启动时与自动连接竞态。
            if (not connected_ok and not self.connected and self._auto_reconnect
                    and not self._in_supervisor
                    and (self._reconnect_task is None or self._reconnect_task.done())):
                self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())

    async def _connect_once(self, my_gen):
        if my_gen != self._generation:
            raise ConnectionSuperseded(
                "connection attempt superseded (gen {} != {})".format(my_gen, self._generation)
            )

        self._set_state(self.STATE_CONNECTING, "connect requested", **self._status_detail())

        try:
            client = self._create_client()
        except Exception as e:
            raise ConnectionError(
                "创建 OPC DA 客户端失败（ProgID={}, host={}）: {}".format(
                    self.prog_id, self.host, e)
            ) from e

        try:
            client.connect(timeout=CONNECT_TIMEOUT)
        except asyncio.CancelledError:
            try:
                client.disconnect()
            except Exception:
                pass
            raise
        except Exception as e:
            try:
                client.disconnect()
            except Exception:
                pass
            raise ConnectionError(
                "连接 OPC DA 服务器失败（{} @ {}）: {}".format(
                    self.prog_id, self.host, e)
            ) from e

        if my_gen != self._generation:
            try:
                client.disconnect()
            except Exception:
                pass
            raise ConnectionSuperseded(
                "connection attempt superseded during connect (gen {})".format(my_gen)
            )

        self.client = client
        self._consecutive_write_failures = 0
        self._bad_quality_items.clear()
        self._items_added_gen = -1
        self._items_added_client = None
        self._item_handles.clear()
        self._handle_to_item.clear()
        self._connected_at = time.monotonic()
        self._last_data_at = self._connected_at

        try:
            status = client.get_server_status() or {}
            global_logger.info(
                "Connected OPC DA server {} @ {} | state={} vendor={} version={} groups={}".format(
                    self.prog_id, self.host,
                    status.get("state_text") or status.get("state"),
                    status.get("vendor"), status.get("version"), status.get("group_count"),
                )
            )
        except Exception as e:
            global_logger.warning("Connected but get_server_status() failed: {}".format(e))

        # 先推进到 loading 再启动心跳：心跳协程的存活判据是 _link_usable()，
        # 必须能看到 LOADING 状态，否则第一次调度就退出。
        self._set_state(self.STATE_LOADING, "transport established", **self._status_detail())
        self._start_monitor(my_gen)
        # 交给事件循环一轮，让心跳协程真正跑到第一行（qasync 重入保护，与 UA 版一致）
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    # ----------------------------------------------------------------------------------
    # 装配：浏览 / AddItems / 读初值 / 订阅
    # ----------------------------------------------------------------------------------

    async def bootstrap(self):
        """连接后的装配阶段：确保点位表、AddItems、读初值、建立订阅。

        重连时复用 self.nodes（不重新 Browse），只重新 AddItems + 订阅 + 读初值。
        """
        if not self.connected and self._state != self.STATE_LOADING:
            raise ConnectionError("bootstrap() requires an established connection")

        self._loading = True
        # 在 try 之外先捕获世代：装配失败时据此收尾状态机（见下方 except）。
        my_gen = self._generation
        try:
            # 捕获本次装配的客户端：装配过程中若心跳/读写检测到断线（或用户断开），
            # 绝不能在最后一步把状态错误地推回 connected。
            client = self.client
            self._set_state(self.STATE_LOADING, "loading nodes", **self._status_detail())

            if not self.nodes:
                try:
                    await self.get_all_nodes()
                except ConnectionSuperseded:
                    raise
                except Exception as e:
                    # 自动扫描失败时，只要配置了手动点位就继续装配，不能因为浏览失败
                    # 让整条链路不可用（FLAT/超大地址空间/权限受限时很常见）。
                    if not self.manual_items:
                        raise
                    global_logger.warning(
                        "OPC DA 自动扫描失败（{}）；继续使用 {} 个手动添加点位。".format(
                            e, len(self.manual_items)))
                    self.nodes = {}
            # 自动扫描之外的手动点位：首次连接、重连、断线期间新增都要并入。
            self._merge_manual_items_into_nodes()
            # DA 必须先把点位 AddItems 到 group 才能订阅/高效读；重连后句柄需要重建。
            await self._ensure_items_added()
            await self._refresh_node_values()

            if my_gen != self._generation or self.client is not client:
                raise ConnectionSuperseded("bootstrap superseded by a newer connection")
            if self._state not in (self.STATE_LOADING, self.STATE_CONNECTED):
                raise ConnectionSuperseded("bootstrap aborted: link was lost during assembly")
            if client is None:
                raise ConnectionSuperseded("bootstrap aborted: client was released")

            if self._sub_callback is not None:
                await self.start_subscription(self._sub_callback)

            if my_gen != self._generation or self.client is not client:
                raise ConnectionSuperseded("bootstrap superseded before completion")
            if self._state not in (self.STATE_LOADING, self.STATE_CONNECTED):
                raise ConnectionSuperseded(
                    "bootstrap aborted: link was lost during subscription")

            self._set_state(self.STATE_CONNECTED, "bootstrap complete",
                            **self._status_detail())
            # 装配完成后再次确认心跳/轮询任务仍在运行：防止长耗时 AddItems/Read
            # 期间任务静默退出（含僵尸 pending 与轮询降级任务），与 UA 版对齐。
            await self._ensure_link_tasks(my_gen)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # 装配失败（browse/AddItems/读初值/订阅）必须收尾状态机。
            # 修复前 bootstrap 抛错后状态停在 STATE_LOADING：connect 已成功、
            # GetStatus 仍正常，心跳不会判断线，界面永远显示"正在加载节点/订阅"，
            # 也不会触发重连。这里按断线路径收尾：
            #   自动重连开启 -> reconnecting（supervisor 接手重试）；
            #   自动重连关闭 -> disconnected 并释放已建立的 COM 会话。
            try:
                if my_gen == self._generation and self._state in (
                        self.STATE_LOADING, self.STATE_CONNECTED):
                    self._handle_connection_lost(
                        "bootstrap failed: {}".format(exc), my_gen)
                    if not self._auto_reconnect:
                        try:
                            await self._cleanup_client()
                        except Exception as cleanup_exc:
                            global_logger.debug(
                                "bootstrap failure cleanup error: {}".format(cleanup_exc))
                        self._set_state(self.STATE_DISCONNECTED,
                                        "bootstrap failed (auto-reconnect disabled)")
            except Exception as state_exc:
                global_logger.error(
                    "bootstrap failure cleanup failed: {}".format(state_exc), exc_info=True)
            raise
        finally:
            self._loading = False
        cb = self.on_nodes_recovered
        if callable(cb):
            try:
                cb()
            except Exception as e:
                global_logger.error(
                    "on_nodes_recovered callback error: {}".format(e), exc_info=True)

    async def get_all_nodes(self):
        """浏览 OPC DA 地址空间并缓存点位表，返回纯数据列表（不含 COM 引用）。"""
        if not self.connected and self._state != self.STATE_LOADING:
            return []
        client = self.client
        if client is None:
            raise ConnectionError("get_all_nodes() requires a connected client")

        my_gen = self._generation
        try:
            raw_items = client.browse(max_depth=self.browse_max_depth) or []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if my_gen != self._generation:
                raise ConnectionSuperseded("browse superseded")
            raise ConnectionError("浏览 OPC DA 地址空间失败: {}".format(e)) from e

        if my_gen != self._generation:
            raise ConnectionSuperseded("node browse superseded by a newer connection")
        if self._state not in (self.STATE_LOADING, self.STATE_CONNECTED):
            raise ConnectionSuperseded("node browse aborted: link was lost during browse")

        discovered = {}
        for raw in raw_items:
            node = self._node_from_da_item(raw)
            if node is not None:
                discovered[node["node_id"]] = node

        self.nodes = discovered
        self._browse_node_ids = set(discovered.keys())
        # 即使单独调用 get_all_nodes()，也不能把手动点位从表里冲掉
        self._merge_manual_items_into_nodes()
        self._subscribed_items = []
        self._item_handles.clear()
        self._handle_to_item.clear()
        self._items_added_gen = -1
        self._items_added_client = None
        global_logger.info(
            "OPC DA browse complete: {} items (max_depth={}, host={}, manual={}).".format(
                len(discovered), self.browse_max_depth, self.host, len(self.manual_items))
        )
        await asyncio.sleep(0)
        return [dict(n) for n in discovered.values()]

    def _node_from_da_item(self, raw):
        """把 opcda.client.DaItem 转成引擎内部点位结构。"""
        item_id = getattr(raw, "item_id", None)
        if not item_id:
            return None
        item_id = str(item_id)
        name = getattr(raw, "name", None) or item_id
        path = getattr(raw, "path", None) or ""
        try:
            canonical_vt = int(getattr(raw, "canonical_vt", 0) or 0)
        except (TypeError, ValueError):
            canonical_vt = 0
        try:
            access = int(getattr(raw, "access", 0) or 0)
        except (TypeError, ValueError):
            access = 0
        return {
            "node_id": item_id,
            "name": str(name),
            "alias": str(name),
            "type": _vtype_name(canonical_vt),
            "value": None,
            "timestamp": "",
            "path": path,
            "item_id": item_id,
            "canonical_vt": canonical_vt,
            "access": access,
            "writable": bool(access & 0x02),
        }

    async def _ensure_items_added(self):
        """为当前客户端 AddItems（每 300 个一批），并维护 hClient 到 ItemID 的映射。

        重连后必须重新 AddItems（服务器句柄随 group 失效），但 ItemID 不变，
        所以这里直接用缓存的点位表，不重新 Browse。
        """
        if not self.nodes:
            return 0
        client = self.client
        if client is None:
            raise ConnectionError("_ensure_items_added() requires a connected client")

        my_gen = self._generation
        if self._items_added_gen == my_gen and self._items_added_client is client:
            return len(self.nodes)

        item_ids = list(self.nodes.keys())
        added = 0
        partial_failures = []
        for start in range(0, len(item_ids), ADD_BATCH_SIZE):
            if my_gen != self._generation:
                raise ConnectionSuperseded("AddItems superseded")
            chunk = item_ids[start:start + ADD_BATCH_SIZE]
            try:
                results = client.add_items(chunk, update_rate_ms=self.update_rate_ms) or []
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if my_gen != self._generation:
                    raise ConnectionSuperseded("AddItems superseded")
                if self._is_partial_additems_error(e):
                    partial_failures.extend(chunk)
                    global_logger.warning(
                        "AddItems partially failed for {} items: {}".format(len(chunk), e)
                    )
                    continue
                raise ConnectionError(
                    "AddItems 失败（{} 个点位）: {}".format(len(chunk), e)
                ) from e

            for result in results:
                item_id = getattr(result, "item_id", None)
                node = self.nodes.get(item_id) if item_id else None
                if node is None:
                    continue
                try:
                    canonical_vt = int(getattr(result, "canonical_vt", 0) or 0)
                except (TypeError, ValueError):
                    canonical_vt = 0
                try:
                    access = int(getattr(result, "access", 0) or 0)
                except (TypeError, ValueError):
                    access = 0
                try:
                    handle = int(getattr(result, "client_handle", 0) or 0)
                except (TypeError, ValueError):
                    handle = 0
                if canonical_vt:
                    node["canonical_vt"] = canonical_vt
                    node["type"] = _vtype_name(canonical_vt)
                node["access"] = access
                node["writable"] = bool(access & 0x02)
                node["client_handle"] = handle
                if handle:
                    self._item_handles[item_id] = handle
                    self._handle_to_item[handle] = item_id
                added += 1
            await asyncio.sleep(0)

        self._items_added_gen = my_gen
        self._items_added_client = client
        self._subscribed_items = list(self.nodes.keys())
        if partial_failures:
            global_logger.warning(
                "AddItems: {}/{} items accepted; {} failed.".format(
                    added, len(item_ids), len(partial_failures))
            )
        else:
            global_logger.info(
                "AddItems complete: {}/{} items (update_rate={}ms, generation={}).".format(
                    added, len(item_ids), self.update_rate_ms, my_gen)
            )
        return added

    def _is_partial_additems_error(self, error):
        """区分"部分点位无效"与"链路已死"（按 HRESULT 严格判定）。

        契约允许 add_items 对失败的 item 抛带错误列表的 OpcDaError。这类错误
        不应触发整条连接重连（否则一个坏点名就能让 24/7 系统反复重建链路）；
        而 RPC/未连接/CreateObject 之类错误必须上抛，由重连 supervisor 处理。

        修复前用关键字匹配（包含中文『点位』）判断部分失败，而底层 AddItems
        链路崩溃时的异常文本是『批量添加点位失败（N 个）』也含『点位』，结果把
        整批链路故障当成坏点忽略，最终在已断开的 COM 对象上把状态推进到
        connected（假连接）。现在只认逐项错误列表，且要求**全部**是点位级业务错误。
        """
        errors = getattr(error, "errors", None)
        if not errors:
            return False
        for entry in errors:
            try:
                code = entry[1]
            except (TypeError, IndexError):
                code = None
            if not com.is_item_level_error(code):
                return False
        return True

    async def _refresh_node_values(self):
        """SyncRead(CACHE) 一次全量初值（重连后同时补齐断线期间的变化）。"""
        if not self.nodes:
            return 0
        client = self.client
        if client is None:
            return 0
        my_gen = self._generation
        item_ids = list(self.nodes.keys())
        refreshed_values = []
        failures = 0
        last_error = None
        for start in range(0, len(item_ids), READ_BATCH_SIZE):
            if my_gen != self._generation:
                raise ConnectionSuperseded("read initial values superseded")
            chunk = item_ids[start:start + READ_BATCH_SIZE]
            try:
                values = client.read_items(chunk, source=1) or []
            except asyncio.CancelledError:
                raise
            except Exception as e:
                failures += 1
                last_error = e
                code = getattr(e, "code", None)
                if code is None or com.is_link_level_error(code):
                    # 链路级读取失败不能吞掉：否则 bootstrap 会在已断开的 COM 对象
                    # 上继续建立订阅并把状态推进到 connected。
                    raise ConnectionError(
                        "SyncRead 初始化读取失败（链路故障，{} 个点位）: {}".format(len(chunk), e)
                    ) from e
                global_logger.warning(
                    "SyncRead initial values failed for a batch of {}: {}".format(len(chunk), e)
                )
            else:
                refreshed_values.extend(values)
            await asyncio.sleep(0)

        refreshed = self._apply_values(refreshed_values, my_gen, dispatch=False)
        if failures:
            global_logger.warning(
                "Initial value refresh: {}/{} values read, {} batch(es) failed "
                "(last error: {}). Heartbeat will continue to probe the link.".format(
                    refreshed, len(item_ids), failures, last_error)
            )
        else:
            global_logger.info(
                "Initial value refresh complete: {}/{} values.".format(
                    refreshed, len(item_ids))
            )
        return refreshed

    # ----------------------------------------------------------------------------------
    # 订阅 / 轮询
    # ----------------------------------------------------------------------------------

    async def start_subscription(self, callback):
        """建立数据变化订阅；服务器不支持 IOPCAsyncIO2 时自动降级为 SyncRead 轮询。"""
        if callback is not None:
            self._sub_callback = callback
            self._sub_cb_quality_mode = self._callback_quality_mode(callback)
        if not self.nodes:
            global_logger.warning("start_subscription skipped: node list is empty.")
            return
        if not self.connected and self._state != self.STATE_LOADING:
            raise ConnectionError("start_subscription() requires an established connection")

        client = self.client
        if client is None:
            raise ConnectionError("start_subscription() requires a connected client")

        my_gen = self._generation
        await self._ensure_items_added()
        if my_gen != self._generation:
            raise ConnectionSuperseded("subscription superseded")

        # 先停掉可能存在的轮询任务，避免两种更新源同时推送
        self._cancel_poll_task()

        try:
            client.set_data_change_callback(
                lambda values, gen=my_gen: self._on_da_data_change(values, gen)
            )
            client.start_subscription(update_rate_ms=self.update_rate_ms)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            if my_gen != self._generation:
                raise ConnectionSuperseded("subscription superseded during start")
            code = getattr(e, "code", None)
            async_unsupported = com.is_async_unsupported_error(code)
            if not async_unsupported and code is None:
                # 没有 HRESULT 时只按明确的"不支持异步订阅"文本兜底；其余（OSError/
                # RPC 异常）一律按链路故障上抛，绝不能靠降级轮询掩盖断线。
                text = str(e).lower()
                async_unsupported = ("async unsupported" in text
                                     or "iopcasyncio2" in text
                                     or "连接点" in text)
            if not async_unsupported:
                raise
            global_logger.warning(
                "OPC DA 异步订阅不可用（{}）；降级为 SyncRead 轮询，周期 {}ms。".format(
                    e, self.update_rate_ms)
            )
            try:
                client.set_data_change_callback(None)
            except Exception:
                pass
            self._subscription_mode = "poll"
            self.subscription = "poll"
            self._start_poll_task(my_gen)
            await asyncio.sleep(0)
            return

        self._subscription_mode = "async"
        self.subscription = "async"
        global_logger.info(
            "Subscription established for {} items (update_rate={}ms, generation={}).".format(
                len(self.nodes), self.update_rate_ms, my_gen)
        )
        await asyncio.sleep(0)

    def _start_poll_task(self, my_gen):
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()
        self._poll_started_at = 0.0
        self._poll_task_created_at = time.monotonic()
        self._poll_task = asyncio.create_task(self._poll_loop(my_gen))

    def _cancel_poll_task(self):
        task = self._poll_task
        self._poll_task = None
        if task is not None and not task.done():
            task.cancel()

    async def _poll_loop(self, my_gen):
        """SyncRead 轮询循环（IOPCAsyncIO2 不可用时的降级路径）。"""
        self._poll_started_at = time.monotonic()
        try:
            while self._poll_started_at and my_gen == self._generation \
                    and self.client is not None and self._link_usable():
                # 每轮重算周期：设置页热更新 update_rate_ms 后轮询立即生效。
                interval = max(0.05, self.update_rate_ms / 1000.0)
                await asyncio.sleep(interval)
                if my_gen != self._generation or self.client is None or not self._link_usable():
                    break
                # 自愈：每轮轮询都确认心跳/轮询任务仍在运行（本任务即轮询任务，
                # _ensure_link_tasks 不会重启自己，只会补拉已退出的心跳任务）。
                await self._ensure_link_tasks(my_gen)
                client = self.client
                item_ids = list(self.nodes.keys())
                values = []
                try:
                    for start in range(0, len(item_ids), POLL_READ_BATCH_SIZE):
                        if my_gen != self._generation or not self._link_usable():
                            return
                        chunk = item_ids[start:start + POLL_READ_BATCH_SIZE]
                        values.extend(client.read_items(chunk, source=1) or [])
                        await asyncio.sleep(0)
                    self._pump_client_events()
                    self._apply_values(values, my_gen, dispatch=True)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    global_logger.error("SyncRead polling failed: {}".format(e))
                    self._handle_connection_lost("poll read failure: {}".format(e), my_gen)
                    break
        except asyncio.CancelledError:
            pass
        finally:
            global_logger.debug("Poll loop exited (generation {}).".format(my_gen))

    def _on_da_data_change(self, values, my_gen):
        """底层订阅回调入口（在 COM 消息泵所在线程触发）。

        回调只包含发生变化的子集，且以 hClient 标识点位，因此通过
        _handle_to_item 还原 ItemID；旧世代回调直接丢弃。
        """
        if my_gen != self._generation:
            global_logger.debug(
                "Ignoring data callback from stale generation {} (current {}).".format(
                    my_gen, self._generation)
            )
            return
        try:
            self._apply_values(values, my_gen, dispatch=True)
        except Exception as e:
            global_logger.error("Data change callback error: {}".format(e), exc_info=True)

    def _resolve_item_id(self, da_value):
        """把回调里的 DaValue 还原成 ItemID（兼容只给 hClient 的实现）。"""
        item_id = getattr(da_value, "item_id", None)
        if item_id:
            item_id = str(item_id)
            if item_id in self.nodes:
                return item_id
        for attr in ("client_handle", "handle", "h_client"):
            handle = getattr(da_value, attr, None)
            if handle is None:
                continue
            try:
                handle = int(handle)
            except (TypeError, ValueError):
                continue
            node_id = self._handle_to_item.get(handle)
            if node_id is not None:
                return node_id
        return None

    def _apply_values(self, values, my_gen, dispatch=True):
        """把 DaValue 列表写入缓存点位表；可选地推送给 UI 回调。

        返回成功更新的点位数。quality 非 Good 的点位会记录一次告警，但保留可用的值
        （若本帧没有值则沿用缓存值），避免坏点把界面刷成空。
        """
        if my_gen != self._generation or not values:
            return 0
        updated = 0
        dispatched = []
        now_ts = time.strftime("%Y-%m-%d %H:%M:%S")
        for da_value in values:
            if my_gen != self._generation:
                break
            node_id = self._resolve_item_id(da_value)
            if node_id is None:
                continue
            node = self.nodes.get(node_id)
            if node is None:
                continue

            quality = getattr(da_value, "quality", 0)
            error = getattr(da_value, "error", 0)
            value = getattr(da_value, "value", None)
            timestamp = getattr(da_value, "timestamp", None) or now_ts

            if error and not com.succeeded(error):
                # 单项读取失败：保留缓存值，不推送，避免界面被清空
                # （S_FALSE / OPC_S_CLAMP 等正数码是成功提示，不能当失败）
                global_logger.debug(
                    "Read error for {} (HRESULT=0x{:08X}); cached value kept.".format(
                        node_id, int(error) & 0xFFFFFFFF if isinstance(error, int) else 0)
                )
                continue

            if not _quality_is_good(quality):
                if node_id not in self._bad_quality_items:
                    self._bad_quality_items.add(node_id)
                    global_logger.warning(
                        "Bad OPC quality for {} (quality=0x{:04X}); "
                        "keeping the last good value.".format(
                            node_id, int(quality) & 0xFFFF if isinstance(quality, int) else 0)
                    )
                if value is None:
                    value = node.get("value")
            else:
                self._bad_quality_items.discard(node_id)

            node["value"] = value
            node["timestamp"] = timestamp
            node["quality"] = quality
            if node.get("type") in (None, "", "Unknown") and value is not None:
                node["type"] = _python_type_name(value)
            updated += 1
            if dispatch:
                dispatched.append((node_id, value, timestamp, quality))

        if dispatch and dispatched:
            self._last_data_at = time.monotonic()
            callback = self._sub_callback
            quality_mode = self._sub_cb_quality_mode
            if callable(callback):
                for node_id, value, timestamp, quality in dispatched:
                    try:
                        if quality_mode == "positional":
                            callback(node_id, value, timestamp, quality)
                        elif quality_mode == "keyword":
                            callback(node_id, value, timestamp, quality=quality)
                        else:
                            callback(node_id, value, timestamp)
                    except Exception as e:
                        global_logger.error(
                            "Subscription callback error for {}: {}".format(node_id, e),
                            exc_info=True,
                        )
        return updated

    # ----------------------------------------------------------------------------------
    # 健康看门狗
    # ----------------------------------------------------------------------------------

    # 协程创建后超过该秒数仍未执行到第一行，就认定它被 qasync/Qt 的
    # processEvents() 重入卡成了"僵尸任务"（pending 但永远不会被调度）。
    ZOMBIE_TASK_GRACE = 2.0

    def _is_zombie_task(self, task, started_at, created_at, now):
        """判断任务是否"创建后一步都没跑就永久 pending"的僵尸任务。"""
        if task is None or task.done() or task.cancelled():
            return False
        if started_at > 0 or created_at <= 0:
            return False
        return (now - created_at) >= self.ZOMBIE_TASK_GRACE

    @staticmethod
    def _task_exit_detail(task):
        """读取已完成任务的退出原因；CancelledError 不能用 .exception() 取。"""
        try:
            if task.cancelled():
                return "cancelled"
            exc = task.exception()
        except Exception:
            return "unknown"
        return "returned normally" if exc is None else repr(exc)

    async def _ensure_link_tasks(self, my_gen):
        """确保心跳看门狗与轮询任务都活着（含"僵尸任务"自愈）。

        24/7 运行中有两类致命但静默的后台任务故障（与 UA 版
        opc/client_engine.py::_ensure_link_tasks 完全一致）：

        1. 协程正常 return（不抛异常）：健康判据看不到，链路"看起来正常"；
        2. 协程一步都没执行就永久 pending：qasync 里 Qt 的 processEvents() 重入
           会丢掉刚 create_task 的任务的第一次 __step，之后它永远不会再被调度。
           这种任务 done() 为 False，只按 done() 检查发现不了，必须比较
           "创建时间"和"协程真正开始执行的时间"（_monitor_started_at /
           _poll_started_at）。现场表现：连接后既没有 [HEARTBEAT] 也没有轮询，
           链路健康检测永久失效。

        本函数是 async 的：补拉任务后主动让出两轮事件循环，保证新任务立刻跑到
        第一行（登记 started 时间戳并进入 sleep），避免再次被 UI 重入卡死。
        """
        if my_gen != self._generation or self.client is None or not self._link_usable():
            return

        now = time.monotonic()
        restarted = False

        if self._monitor_task is None or self._monitor_task.done():
            if self._monitor_task is not None:
                global_logger.warning(
                    "[HEARTBEAT] OPC DA 心跳任务未在运行（{}）；重新拉起。".format(
                        self._task_exit_detail(self._monitor_task)))
            self._monitor_started_at = 0.0
            self._monitor_task_created_at = now
            self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))
            restarted = True
        elif self._is_zombie_task(
                self._monitor_task, self._monitor_started_at,
                self._monitor_task_created_at, now):
            global_logger.error(
                "[HEARTBEAT] OPC DA 心跳任务创建后 {:.1f}s 仍未开始执行"
                "（qasync 重入僵尸任务）；替换它。".format(
                    now - self._monitor_task_created_at))
            self._monitor_task.cancel()
            self._monitor_started_at = 0.0
            self._monitor_task_created_at = now
            self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))
            restarted = True

        # 只有降级到 SyncRead 轮询时才需要保全 _poll_task。
        if self._subscription_mode == "poll" or self.subscription == "poll":
            poll_task = self._poll_task
            poll_dead = poll_task is None or poll_task.done()
            # _poll_started_at 为 0 说明协程从未跑到第一行（始终 pending），同样视为失效。
            poll_broken = (not poll_dead) and (
                self._poll_started_at <= 0.0
                or self._is_zombie_task(
                    poll_task, self._poll_started_at, self._poll_task_created_at, now))
            if poll_dead or poll_broken:
                if poll_dead and poll_task is not None:
                    global_logger.warning(
                        "[POLL] OPC DA 轮询任务未在运行（{}）；重新拉起。".format(
                            self._task_exit_detail(poll_task)))
                elif poll_broken:
                    global_logger.error(
                        "[POLL] OPC DA 轮询任务创建后 {:.1f}s 仍未开始执行"
                        "（qasync 重入僵尸任务）；替换它。".format(
                            now - self._poll_task_created_at))
                self._start_poll_task(my_gen)
                restarted = True

        if restarted:
            # 让新任务在本次 await 中跑到第一行；执行两轮更稳妥。
            await asyncio.sleep(0)
            await asyncio.sleep(0)

    def ensure_liveness(self):
        """同步自愈入口：供 Qt 刷新定时器等非异步上下文兜底调用。

        仅当状态为 LOADING/CONNECTED 且 client 存在时才可能重启；已断开/断开中
        绝不重建任务（否则会把用户主动断开后的僵尸任务重新拉起来）。
        心跳/轮询任务死亡或处于僵尸 pending 时 create_task(self._ensure_link_tasks)，
        并把任务强引用保存到 _liveness_tasks。返回是否触发了重启。
        """
        if self._state not in (self.STATE_LOADING, self.STATE_CONNECTED):
            return False
        if self.client is None:
            return False
        # 已有一个补拉任务在跑就不重复创建，避免 UI 定时器高频调用造成任务风暴。
        if any(task is not None and not task.done() for task in self._liveness_tasks):
            return False

        now = time.monotonic()
        needs_restart = self._monitor_task is None or self._monitor_task.done()
        if not needs_restart and self._is_zombie_task(
                self._monitor_task, self._monitor_started_at,
                self._monitor_task_created_at, now):
            needs_restart = True
        if not needs_restart and (
                self._subscription_mode == "poll" or self.subscription == "poll"):
            poll_task = self._poll_task
            poll_dead = poll_task is None or poll_task.done()
            poll_broken = (not poll_dead) and (
                self._poll_started_at <= 0.0
                or self._is_zombie_task(
                    poll_task, self._poll_started_at, self._poll_task_created_at, now))
            needs_restart = poll_dead or poll_broken
        if not needs_restart:
            return False

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return False
        try:
            task = loop.create_task(self._ensure_link_tasks(self._generation))
        except Exception as e:
            global_logger.debug(
                "ensure_liveness: create_task failed (non-fatal): {}".format(e))
            return False
        self._liveness_tasks.add(task)
        task.add_done_callback(self._liveness_tasks.discard)
        global_logger.warning(
            "[HEARTBEAT] OPC DA 心跳/轮询任务缺失，已安排自愈（generation {}）。".format(
                self._generation))
        return True

    def _start_monitor(self, my_gen):
        if self._monitor_task is not None and not self._monitor_task.done():
            self._monitor_task.cancel()
        self._monitor_started_at = 0.0
        self._monitor_task_created_at = time.monotonic()
        self._monitor_task = asyncio.create_task(self._monitor_connection(my_gen))

    async def _monitor_connection(self, my_gen):
        """健康看门狗：周期 get_server_status() + pump_events()。

        判据全部基于链路活性（客户端自报连接状态、服务器状态码、COM 调用是否成功），
        绝不把"没有数据变化"当作断线。
        """
        self._monitor_started_at = time.monotonic()
        interval = self._heartbeat_interval()
        ticks = 0
        try:
            while my_gen == self._generation and self.client is not None \
                    and self._link_usable():
                await asyncio.sleep(interval)
                if my_gen != self._generation or self.client is None or not self._link_usable():
                    break
                try:
                    # 自愈：每 tick 确认心跳/轮询任务仍在运行（含僵尸 pending），
                    # 防止看门狗自己退出后无人再重新拉起。
                    await self._ensure_link_tasks(my_gen)
                    client = self.client
                    # 泵消息：Qt 下是冗余的，无 Qt 测试里是订阅回调的驱动源
                    self._pump_client_events()

                    is_connected = getattr(client, "is_connected", None)
                    if callable(is_connected) and not is_connected():
                        raise RuntimeError("OPC DA client reports disconnected")

                    status = client.get_server_status() or {}
                    state_code = status.get("state")
                    try:
                        state_code = int(state_code)
                    except (TypeError, ValueError):
                        state_code = _OPC_STATE_RUNNING
                    # FAILED / COMM_FAULT 表示服务器已不可用，按断线处理；
                    # NOCONFIG / SUSPENDED / TEST 仍保留链路但必须告警（旧实现
                    # 只判断 FAILED，通信故障等异常状态完全没有提示）。
                    if state_code in (_OPC_STATE_FAILED, _OPC_STATE_COMM_FAULT):
                        raise RuntimeError(
                            "OPC server state is {} ({})".format(
                                "FAILED" if state_code == _OPC_STATE_FAILED else "COMM_FAULT",
                                status.get("state_text") or state_code)
                        )
                    if state_code in (_OPC_STATE_NOCONFIG, _OPC_STATE_SUSPENDED,
                                      _OPC_STATE_TEST):
                        abnormal = "{} ({})".format(
                            status.get("state_text") or state_code, state_code)
                        if self._last_abnormal_server_state != abnormal:
                            self._last_abnormal_server_state = abnormal
                            global_logger.warning(
                                "[HEARTBEAT] OPC server state is abnormal: {}; "
                                "link kept but control/write may be affected.".format(abnormal))
                    else:
                        self._last_abnormal_server_state = None
                    self._last_heartbeat_at = time.monotonic()
                    ticks += 1
                    if ticks % 6 == 0:
                        since_push = time.monotonic() - (
                            self._last_data_at or self._connected_at or time.monotonic())
                        # 周期成功心跳保持每 6 tick 一条，但降到 DEBUG：
                        # 现场日志的 INFO 分类只保留状态变更/故障/自愈等有信息量的记录。
                        global_logger.debug(
                            "[HEARTBEAT] OPC DA link OK | server_state={} | vendor={} | "
                            "uptime={:.0f}s | last push={:.0f}s ago | subscription={}".format(
                                status.get("state_text") or state_code,
                                status.get("vendor"),
                                time.monotonic() - (self._connected_at or time.monotonic()),
                                since_push,
                                self._subscription_mode or "none",
                            )
                        )
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    global_logger.error(
                        "OPC DA connection monitor detected failure: {}".format(e))
                    self._handle_connection_lost("heartbeat failure: {}".format(e), my_gen)
                    break
        except asyncio.CancelledError:
            pass
        finally:
            current = asyncio.current_task()
            # 被 _ensure_link_tasks 主动替换掉的旧任务不算"意外退出"（避免自愈时刷 WARNING）。
            replaced = (self._monitor_task is not None
                        and self._monitor_task is not current
                        and not self._monitor_task.done())
            user_disconnect = self._state in (
                self.STATE_DISCONNECTING, self.STATE_DISCONNECTED)
            if my_gen == self._generation and self._link_usable() \
                    and not user_disconnect and not replaced:
                # 不静默退出：链路仍然可用却没人做健康检测，说明任务自己 return 了，
                # 必须 WARNING 并立即安排自愈（UA 版同样依赖 _ensure_link_tasks 兜底）。
                global_logger.warning(
                    "[HEARTBEAT] OPC DA 心跳任务意外退出，已安排自愈（generation {}）。".format(
                        my_gen))
                # 正在退出的任务在 finally 里 done() 仍为 False，ensure_liveness 会把它
                # 误判成"还活着"而跳过自愈；先摘掉对自身的引用，补拉任务才能看到它已退出。
                if self._monitor_task is current:
                    self._monitor_task = None
                    self._monitor_started_at = 0.0
                self.ensure_liveness()
            else:
                global_logger.debug(
                    "OPC DA connection monitor task exited (generation {}).".format(my_gen))

    # ----------------------------------------------------------------------------------
    # 断线处理与自动重连
    # ----------------------------------------------------------------------------------

    def _handle_connection_lost(self, reason, my_gen):
        """幂等的断线入口。心跳、轮询、写失败三条检测路径都汇入这里。"""
        if my_gen != self._generation:
            return
        if self._state in (self.STATE_DISCONNECTING, self.STATE_DISCONNECTED):
            return
        was_connected = self._state in (self.STATE_CONNECTED, self.STATE_LOADING)
        if not self._in_supervisor:
            # 手动/心跳检测到的断线从第 1 次重新计数；若异常发生在 supervisor
            # 自己的重连尝试中（connect 成功但 bootstrap 失败），必须保留当前
            # 退避计数，交由外层 supervisor 继续递增，否则会一直按 base_delay
            # 高频重试。
            self.reconnect_attempt = 0
        self._subscription_mode = None
        self.subscription = None
        if self._auto_reconnect:
            self._set_state(self.STATE_RECONNECTING, reason, **self._status_detail())
        else:
            self._set_state(self.STATE_DISCONNECTED, reason, **self._status_detail())

        if was_connected:
            cb = self.on_connection_lost
            if callable(cb):
                try:
                    cb()
                except Exception as e:
                    global_logger.error(
                        "on_connection_lost callback error: {}".format(e), exc_info=True)

        # supervisor 可能在上一次"还连着"的时候就自然退出，断线时必须确保它重新站起来。
        # 但绝不能在 supervisor 自身仍在执行时再拉起第二个：否则两个 supervisor
        # 并发重连、共享 _in_supervisor 标志互相覆盖，退避也会被重置。
        if (self._auto_reconnect and not self._in_supervisor
                and (self._reconnect_task is None or self._reconnect_task.done())):
            self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())

    async def reconnect_forever(self):
        """打开自治重连总闸，并确保 supervisor 已经在跑（幂等）。"""
        self._auto_reconnect = True
        if self._reconnect_task is not None and not self._reconnect_task.done():
            return self._reconnect_task
        if self.connected:
            return None
        if self._state not in (self.STATE_RECONNECTING, self.STATE_LOADING):
            self._set_state(self.STATE_RECONNECTING, "reconnect supervisor requested",
                            **self._status_detail())
        self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())
        return self._reconnect_task

    def stop_reconnect(self):
        """关闭自治重连总闸（同步，便于在关闭流程中调用）。"""
        self._auto_reconnect = False
        pending = getattr(self, "_pending_reconnect_forever", None)
        if pending is not None and not pending.done():
            pending.cancel()
        self._pending_reconnect_forever = None
        task = self._reconnect_task
        if task is not None and not task.done():
            task.cancel()

    async def _stop_reconnect_supervisor(self):
        """取消当前 supervisor 任务，但不改动 _auto_reconnect 总闸。

        总闸只由用户意图（stop_reconnect / disconnect）改写；connect() 会调用本方法，
        而 supervisor 自己也会调用 connect()，所以当 supervisor 就是当前任务时必须
        跳过自我取消，否则重连会在第一步就被 CancelledError 打断。
        """
        pending = getattr(self, "_pending_reconnect_forever", None)
        if pending is not None and not pending.done():
            pending.cancel()
        self._pending_reconnect_forever = None

        task = self._reconnect_task
        if task is None or task.done():
            self._reconnect_task = None
            return
        try:
            current = asyncio.current_task()
        except RuntimeError:
            current = None
        if task is current:
            # supervisor 自己在调用 connect()：绝不能先清掉 self._reconnect_task。
            # 旧实现先置 None 再判断 current_task，导致 supervisor 丢掉自身引用；
            # 随后 connect 成功但 bootstrap 失败时，_handle_connection_lost 会误判
            # "没有 supervisor 在跑"而再拉起一个：两个 supervisor 并发，且各自
            # 从 Attempt 1 重计，指数退避被永久重置。
            return
        self._reconnect_task = None
        task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=TASK_CANCEL_GRACE)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        except Exception:
            pass

    def _next_reconnect_delay(self, attempt):
        """指数退避 + 抖动（attempt 从 1 开始）。"""
        try:
            base = max(MIN_RECONNECT_DELAY, float(self.reconnect_base_delay))
            maximum = max(base, float(self.reconnect_max_delay))
        except (TypeError, ValueError):
            base, maximum = DEFAULT_RECONNECT_BASE_DELAY, DEFAULT_RECONNECT_MAX_DELAY
        delay = base * (2 ** max(0, attempt - 1))
        delay = min(delay, maximum)
        if RECONNECT_JITTER > 0:
            delay *= 1.0 + random.uniform(-RECONNECT_JITTER, RECONNECT_JITTER)
        return max(MIN_RECONNECT_DELAY, delay)

    async def _reconnect_supervisor(self):
        """自治重连循环：指数退避 + 抖动，直到成功或用户要求停止。"""
        global_logger.warning("OPC DA auto-reconnect supervisor started.")
        self._in_supervisor = True
        try:
            while self._auto_reconnect:
                if self.connected:
                    break
                # 另一条路径（UI 手动连接 / 自动连接）正在建链或装配时，不要抢
                # client_lock 去 teardown，否则会把正在 bootstrap 的 client 拆掉。
                if self._connecting or self._loading:
                    await asyncio.sleep(0.5)
                    continue
                self.reconnect_attempt += 1
                display_attempt = max(1, self.reconnect_attempt)
                delay = self._next_reconnect_delay(display_attempt)
                self._set_state(
                    self.STATE_RECONNECTING,
                    "attempt {} in {:.1f}s".format(display_attempt, delay),
                    attempt=display_attempt,
                    total_reconnects=self.total_reconnects,
                    host=self.host,
                    prog_id=self.prog_id,
                )
                global_logger.info(
                    "[RECONNECT] Attempt {}: retrying in {:.1f}s...".format(
                        display_attempt, delay)
                )
                await asyncio.sleep(delay)
                if not self._auto_reconnect or self.connected:
                    break

                try:
                    await self._teardown_for_retry()
                    await self.connect()
                    await self.bootstrap()
                except asyncio.CancelledError:
                    raise
                except ConnectionSuperseded:
                    break
                except Exception as e:
                    global_logger.error(
                        "[RECONNECT] Attempt {} failed ({}): {!r}".format(
                            display_attempt, type(e).__name__, e)
                    )
                    global_logger.debug("[RECONNECT] failure detail", exc_info=True)
                    if self.client is not None:
                        await self._teardown_for_retry()
                    continue

                if self.connected:
                    self.total_reconnects += 1
                    self.reconnect_attempt = 0
                    global_logger.warning(
                        "[RECONNECT] Reconnected successfully on attempt {} "
                        "(total successful recoveries: {}).".format(
                            display_attempt, self.total_reconnects)
                    )
                    break
                global_logger.warning(
                    "[RECONNECT] Attempt {} finished but the link is not up; retrying.".format(
                        display_attempt)
                )
        except asyncio.CancelledError:
            global_logger.info("OPC DA auto-reconnect supervisor cancelled.")
            raise
        except BaseException as e:
            global_logger.error(
                "OPC DA auto-reconnect supervisor crashed fatally: {}".format(e),
                exc_info=True,
            )
        finally:
            current_task = asyncio.current_task()
            if self._reconnect_task is current_task:
                # 只清理"自己"的引用：若已有新的 supervisor（异常路径）绝不误清。
                self._reconnect_task = None
            self._in_supervisor = False
            global_logger.info(
                "OPC DA auto-reconnect supervisor exited "
                "(connected={}, auto_reconnect={}).".format(
                    self.connected, self._auto_reconnect)
            )
            if not self.connected and not self._auto_reconnect:
                self._set_state(self.STATE_DISCONNECTED, "reconnect supervisor exited",
                                **self._status_detail())
            elif not self.connected and self._auto_reconnect:
                # 正常的手动/自动连接会先取消 supervisor，此时 _connecting=True：
                # 不要立刻重建（connect 自己的 finally 会在失败时负责重新拉起）。
                if self._connecting or self._loading:
                    global_logger.info(
                        "Reconnect supervisor stopped for an in-flight connect/assembly; "
                        "connect() will re-arm it if needed."
                    )
                elif self._reconnect_task is None or self._reconnect_task.done():
                    global_logger.warning(
                        "Reconnect supervisor exited unexpectedly; re-arming it."
                    )
                    self._reconnect_task = asyncio.create_task(self._reconnect_supervisor())

    async def _teardown_for_retry(self):
        """重连前的清理：断开并释放上一个客户端（同步、幂等）。"""
        self._ensure_locks()
        async with self._client_lock:
            self._cancel_poll_task()
            await self._cleanup_client()

    # ----------------------------------------------------------------------------------
    # 断开
    # ----------------------------------------------------------------------------------

    async def disconnect(self):
        """安全彻底断开连接（幂等、有界、绝不挂死）。"""
        self._ensure_locks()
        async with self._disconnect_lock:
            global_logger.info(
                "[disconnect] Starting OPC DA disconnect sequence (state={})...".format(
                    self._state)
            )
            self._auto_reconnect = False
            self._generation += 1  # 立即作废所有旧世代回调
            self._set_state(self.STATE_DISCONNECTING, "disconnect requested")

            self._cancel_owned_tasks()

            client = self.client
            if client is not None:
                try:
                    stop_subscription = getattr(client, "stop_subscription", None)
                    if callable(stop_subscription):
                        stop_subscription()
                except Exception as e:
                    global_logger.debug(
                        "[disconnect] stop_subscription failed (non-fatal): {}".format(e)
                    )

            await self._cleanup_client()
            await self._await_owned_tasks()

            self._subscribed_items = []
            self._subscription_mode = None
            self.subscription = None
            self._bad_quality_items.clear()
            self._connected_at = 0.0
            self._last_heartbeat_at = 0.0
            self.reconnect_attempt = 0
            self._consecutive_write_failures = 0

            self._set_state(self.STATE_DISCONNECTED, "disconnect complete")
            global_logger.info("[disconnect] OPC DA disconnect sequence complete.")

    def _cancel_owned_tasks(self):
        self._cancel_poll_task()
        for name in ("_monitor_task", "_reconnect_task"):
            task = getattr(self, name, None)
            if task is not None and not task.done():
                global_logger.info("[disconnect] Cancelling {}...".format(name))
                task.cancel()

    async def _await_owned_tasks(self):
        pending = [t for t in (self._monitor_task, self._poll_task, self._reconnect_task)
                   if t is not None and not t.done()]
        self._monitor_task = None
        self._poll_task = None
        self._reconnect_task = None
        if not pending:
            return
        try:
            done, still_pending = await asyncio.wait(pending, timeout=TASK_CANCEL_GRACE)
            if still_pending:
                global_logger.warning(
                    "[disconnect] {} background task(s) did not finish within {:.1f}s "
                    "(non-fatal, abandoned).".format(
                        len(still_pending), TASK_CANCEL_GRACE)
                )
            else:
                global_logger.info(
                    "[disconnect] All OPC DA background tasks stopped cleanly.")
        except Exception as e:
            global_logger.debug("[disconnect] Task wait exception suppressed: {}".format(e))

    # ----------------------------------------------------------------------------------
    # 写入
    # ----------------------------------------------------------------------------------

    async def write_node_value(self, node_id, value, display_name=None):
        """向 OPC DA 服务器下发单个点位的写指令。返回 bool。"""
        if not self.connected:
            global_logger.warning("Attempted to write while not connected.")
            return False

        target_display = display_name if display_name else node_id
        my_gen = self._generation
        ts_start = time.monotonic()
        client = self.client
        if client is None:
            self._on_write_failure(my_gen)
            return False

        try:
            results = client.write_items({node_id: value}) or []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            if _is_write_value_error(e):
                # 值类型与点位不匹配（如往数组点写 bool）：只报错，不算连接故障
                global_logger.warning(
                    "Write skipped for {} (value type not supported by the item) | "
                    "elapsed={:.1f}ms | {}".format(target_display, elapsed_ms, e)
                )
                # 自愈：写调用已成功到达服务器（只是值类型不被接受），顺手确认心跳仍在跑。
                await self._ensure_link_tasks(my_gen)
                return False
            global_logger.error(
                "Failed to write to node {} | elapsed={:.1f}ms | {}".format(
                    target_display, elapsed_ms, e)
            )
            self._on_write_failure(my_gen)
            # 自愈：写失败后若链路仍可用（未进入断线流程），补拉已退出的心跳任务。
            await self._ensure_link_tasks(my_gen)
            return False

        if my_gen != self._generation:
            return False

        hresult, error_text = 0, ""
        for entry in results:
            try:
                result_id, result_hr, result_text = entry[0], entry[1], entry[2]
            except (TypeError, IndexError):
                continue
            if result_id is not None and str(result_id) != str(node_id):
                continue
            hresult, error_text = result_hr, result_text
            break

        elapsed_ms = (time.monotonic() - ts_start) * 1000
        if results and _hresult_ok(hresult):
            global_logger.info(
                "[WRITE] {} = {} | {:.1f}ms".format(target_display, value, elapsed_ms)
            )
            self._consecutive_write_failures = 0
            # 自愈：成功写入刚证明链路可用，顺手确认心跳任务仍在运行。
            await self._ensure_link_tasks(my_gen)
            return True

        item_level = com.is_item_level_write_error(hresult)
        log_fn = global_logger.warning if item_level else global_logger.error
        log_fn(
            "OPC DA write rejected{} for {} | HRESULT=0x{:08X} {} | {:.1f}ms".format(
                "（点位级：只读/类型不匹配/坏点）" if item_level else "",
                target_display,
                int(hresult) & 0xFFFFFFFF if isinstance(hresult, int) else 0,
                error_text or "",
                elapsed_ms,
            )
        )
        if item_level:
            # 点位级拒绝不代表链路故障：绝不能累计到连续写失败计数，否则值守人员
            # 误点只读布尔点 3 次就会触发整链断开重连（现场已复现）。
            self._consecutive_write_failures = 0
            await self._ensure_link_tasks(my_gen)
            return False
        self._on_write_failure(my_gen)
        # 自愈：写被拒后若链路仍可用，补拉已退出的心跳任务（断线流程里会提前返回）。
        await self._ensure_link_tasks(my_gen)
        return False

    async def write_values_batch(self, node_ids, value, display_names=None):
        """批量写入多个点位（单次 IOPCSyncIO.Write，减少 RTT）。

        :returns: (成功数, 失败数)
        """
        if not self.connected:
            global_logger.warning("Attempted batch write while not connected.")
            return 0, len(node_ids)

        node_ids = list(node_ids or [])
        if not node_ids:
            return 0, 0

        display_names = display_names or [None] * len(node_ids)
        my_gen = self._generation
        ts_start = time.monotonic()
        client = self.client
        if client is None:
            self._on_write_failure(my_gen)
            return 0, len(node_ids)

        # 同一 ItemID 在请求里出现多次时，Write 只发一次；计数按调用方给的列表统计。
        payload = {}
        for node_id in node_ids:
            payload[node_id] = value

        try:
            results = client.write_items(payload) or []
        except asyncio.CancelledError:
            raise
        except Exception as e:
            elapsed_ms = (time.monotonic() - ts_start) * 1000
            if _is_write_value_error(e):
                # 整批里有无法转换的值：是点位数据类型问题，不触发连接级重连
                global_logger.warning(
                    "Batch write skipped (value type not supported by some items) | "
                    "{} nodes | elapsed={:.1f}ms | {}".format(
                        len(node_ids), elapsed_ms, e)
                )
                return 0, len(node_ids)
            global_logger.error(
                "Batch write failed | {} nodes | elapsed={:.1f}ms | {}".format(
                    len(node_ids), elapsed_ms, e)
            )
            self._on_write_failure(my_gen)
            return 0, len(node_ids)

        if my_gen != self._generation:
            return 0, len(node_ids)

        result_map = {}
        for entry in results:
            try:
                result_id, result_hr, result_text = entry[0], entry[1], entry[2]
            except (TypeError, IndexError):
                continue
            result_map[str(result_id)] = (result_hr, result_text)

        success_count = 0
        failed_ids = []
        for node_id in node_ids:
            entry = result_map.get(str(node_id))
            if entry is not None and _hresult_ok(entry[0]):
                success_count += 1
            else:
                failed_ids.append(node_id)
        fail_count = len(node_ids) - success_count

        elapsed_ms = (time.monotonic() - ts_start) * 1000
        if fail_count == 0:
            global_logger.info(
                "[BATCH_WRITE] All {} nodes = {} | {:.1f}ms | avg={:.2f}ms/node".format(
                    len(node_ids), value, elapsed_ms, elapsed_ms / max(1, len(node_ids)))
            )
            self._consecutive_write_failures = 0
        else:
            global_logger.warning(
                "[BATCH_WRITE] Partial result: {} ok, {} failed out of {} nodes "
                "| {:.1f}ms".format(success_count, fail_count, len(node_ids), elapsed_ms)
            )
            name_map = {}
            for index, node_id in enumerate(node_ids):
                name_map.setdefault(
                    str(node_id),
                    display_names[index] if index < len(display_names) else None,
                )
            for node_id in failed_ids:
                entry = result_map.get(str(node_id))
                global_logger.warning(
                    "  -> Node write rejected: {} (ID: {}) | HRESULT=0x{:08X} {}".format(
                        name_map.get(str(node_id)) or node_id,
                        node_id,
                        int(entry[0]) & 0xFFFFFFFF
                        if entry is not None and isinstance(entry[0], int) else 0,
                        (entry[1] if entry is not None else "") or "",
                    )
                )
            # 区分失败原因：只有链路/传输级失败才累计并触发重连；
            # 全是点位级拒绝（只读/类型/坏点）时不应把整条连接断开重连。
            link_failure = False
            for node_id in failed_ids:
                entry = result_map.get(str(node_id))
                code = entry[0] if entry is not None else None
                if code is None or not com.is_item_level_write_error(code):
                    link_failure = True
                    break
            # 只要该批次里任意 1 个点位写入成功，就说明链路健康，重置连续失败计数
            if success_count > 0:
                self._consecutive_write_failures = 0
            elif link_failure:
                self._on_write_failure(my_gen)
            else:
                global_logger.warning(
                    "[BATCH_WRITE] All {} failure(s) are item-level (read-only/bad type); "
                    "link is healthy, not counting as write failure.".format(fail_count))

        return success_count, fail_count

    def _on_write_failure(self, my_gen=None):
        """写入失败累加；连续失败达到阈值触发受控重连。"""
        if my_gen is not None and my_gen != self._generation:
            return
        self._consecutive_write_failures += 1
        global_logger.warning(
            "Write failure count: {}/{}".format(
                self._consecutive_write_failures, self._max_write_failures)
        )
        if self._consecutive_write_failures >= self._max_write_failures:
            global_logger.error(
                "Consecutive write failures reached threshold ({}). "
                "Connection appears stale. Triggering reconnection...".format(
                    self._max_write_failures)
            )
            self._consecutive_write_failures = 0
            self._handle_connection_lost("consecutive write failures", self._generation)
