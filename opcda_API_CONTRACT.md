# OPC DA 客户端内部接口契约（Lead 定义，T1/T2 共同遵守）

目标：新增"直接连接 OPC DA（COM/DCOM）"的完整客户端，与现有 UA 版功能对齐：
连接/断开、浏览并挂载点位、实时订阅、分组控制、定时任务、日志、自动重连。
运行环境与现有软件一致：Windows Server 2012 / Python 3.8（venv38）+ PyQt5 + qasync。
不得依赖 pywin32 / OPC Core Components；COM 交互统一用 comtypes（已在 venv/venv38 安装）。

## 1. 低层模块 opcda/client.py（T1 负责）

单线程使用（GUI 主线程），所有调用同步；COM 以 STA 初始化。文件：

- `opcda/__init__.py`
- `opcda/com.py`：GUID、ctypes 结构（OPCITEMDEF/OPCITEMRESULT/OPCITEMSTATE/OPCSERVERSTATUS 等）、
  IOPCServer/IOPCItemMgt/IOPCSyncIO/IOPCBrowseServerAddressSpace/IEnumString/
  IOPCCommon/IOPCGroupStateMgt/IOPCAsyncIO2/IConnectionPoint* 等接口定义与辅助函数。
- `opcda/client.py`：公开 API（T2 只依赖这里）：

```python
class OpcDaError(Exception):
    code: int | None

@dataclass
class DaItem:
    item_id: str          # 完整 OPC ItemID（作为 UI 的 node_id 使用）
    name: str             # 叶子名（UI 显示名）
    path: str             # 分支路径，如 "Random"；根级为空串
    canonical_vt: int = 0 # VARENUM（VT_*）；0 表示未知
    access: int = 0       # 1=read 2=write 3=read/write
    server_handle: int = 0
    client_handle: int = 0

@dataclass
class DaValue:
    item_id: str
    value: object         # bool/int/float/str/None
    quality: int          # OPC quality WORD
    timestamp: str | None # 本地时间 "%Y-%m-%d %H:%M:%S"
    error: int = 0        # HRESULT（0 成功）

def list_servers(host: str = "localhost") -> list[str]: ...
    # 优先 OPCEnum/OPC.ServerList（com.list_da_servers：本机 clsctx=None，远程
    # CLSCTX_REMOTE_SERVER+machine=host）；为空/失败退回注册表 Component Categories
    # 扫描（本机 com.list_local_da_servers，远程 com.list_remote_da_servers）；
    # 任何失败返回 []，不抛异常。
    # 本机判定覆盖 localhost/回环/hostname/FQDN/全部网卡 IP，避免把本机 LAN IP
    # 误判成远程而返回空列表。

class OpcDaClient:
    def __init__(self, prog_id: str, host: str = "localhost", locale: int = 0x409): ...
    def connect(self, timeout: float = 15.0) -> None: ...
        # 本机: CreateObject(prog_id, interface=IUnknown)；远程:
        # CreateObject(prog_id, machine=host, clsctx=CLSCTX_REMOTE_SERVER, ...)
        # 失败抛 OpcDaError（带 HRESULT 与中文可读原因）。成功后 QI IOPCServer，
        # 尝试 IOPCCommon.SetLocaleID(locale)（失败忽略）。
    def disconnect(self) -> None: ...
    def is_connected(self) -> bool: ...
    def get_server_status(self) -> dict: ...
        # {"state": int, "state_text": str, "vendor": str, "version": str,
        #  "group_count": int, "start_time": str, "current_time": str}
    def browse(self, max_depth: int = 3, max_items: int = 20000) -> list[DaItem]: ...
        # 层次浏览（ChangeBrowsePosition/BrowseOPCItemIDs/GetItemID），无层次则 FLAT 回退；
        # item_id 必须是完整 ItemID（可直接用于 add_items）。
    def add_items(self, item_ids: Sequence[str], update_rate_ms: int = 500) -> list[DaItem]: ...
        # 内部维护唯一 group（按 update_rate_ms 创建/更新）；返回 canonical_vt/access/
        # server_handle/client_handle；失败的 item 不返回（或抛带错误列表的 OpcDaError）。
    def read_items(self, item_ids: Sequence[str] | None = None, source: int = 1) -> list[DaValue]: ...
        # source: 1=CACHE, 2=DEVICE；item_ids 为空表示 reading() 返回的全部已挂载点位。
        # DaValue.error 只在真正的失败 HRESULT 时非 0；S_FALSE / OPC_S_CLAMP 等正数
        # HRESULT 属成功提示，不得当作读失败丢弃数值。
    def write_items(self, values: Mapping[str, object]) -> list[tuple[str, int, str]]: ...
        # 返回 [(item_id, hresult, error_text)]；按 canonical_vt 构造 VARIANT。
        # 单个点位类型不兼容/数组类型/只读时只记该项错误，继续下发同批其他点位；
        # 仅整批 COM 调用失败（链路问题）才抛 OpcDaError。
        # 错误分类：com.is_item_level_error / is_item_level_write_error 判断点位级
        # 业务错误（OPC_E_*、写结果里的 E_ACCESSDENIED 等），is_link_level_error
        # 判断链路/传输故障（RPC/CO_E/E_FAIL 等）。引擎据此决定是否触发整链重连。
    def set_data_change_callback(self, callback) -> None: ...
        # callback(list[DaValue])，在调用线程的 COM 消息泵中触发。
        # DaValue.quality 必须随每次推送一起回传（引擎再转给 UI），否则监控页
        # 无法实时显示/恢复「通信异常」状态。
    def start_subscription(self, update_rate_ms: int = 500) -> None: ...
    def stop_subscription(self) -> None: ...
    def get_error_string(self, code: int) -> str: ...
    def pump_events(self) -> None: ...  # 无 Qt 环境下由测试/引擎定时调用，泵 Windows 消息

def pump_events() -> None: ...  # 模块级，同上
```

实现要求：
- 订阅优先 IOPCAsyncIO2 + IOPCDataCallback（connection point）；服务器不支持时
  `start_subscription` 抛 `OpcDaError("async unsupported")`，由引擎降级为 SyncRead 轮询。
- 引擎对 UI 的逐点推送回调为 `callback(node_id, value, timestamp[, quality])`：
  回调可接收第 4 个位置参数时按位置推送、恰好 3 个位置参数 + `**kwargs` 时用
  `quality=` 关键字推送实时 OPC 质量码（监控页「通信异常」状态依赖它动态出现/
  恢复）；旧三参回调按签名探测保持兼容。质量码同时写入 DataManager 节点的
  `quality` 字段。
- 所有服务器返回的数组/字符串用 CoTaskMemFree 释放；BSTR 注意不要重复释放。
- VARTYPE→Python 与 Python→VARIANT 的转换集中提供：
  `vtype_to_type_name(vt) -> str`（Boolean/Int16/Int32/UInt16/UInt32/Int64/UInt64/Float/Double/String/Unknown）
  以及写值时按 canonical_vt 转换。
- 错误处理：任何 COM 失败不要泄漏对象；`disconnect` 幂等；重复 connect 先 disconnect。
- 兼容 32/64 位服务器（out-of-proc 经 marshalling）；不要假设服务器支持任何可选接口。

## 2. 引擎适配层 opcda/engine.py（T2 负责）

`OpcDaClientEngine` 必须与 `opc/client_engine.py::OpcClientEngine` 的**上层可用接口**一致，
以便复用现有 UI/调度器/DataManager：

状态常量与字符串：disconnected / connecting / loading / connected / reconnecting / disconnecting。

属性：`state`、`connected`、`nodes`（dict: node_id → {node_id,name,type,value,timestamp,alias,...}）、
`host`、`port`（DA 固定 0，仅为兼容）、`url`（如 opcda://host/ProgID）、
`display_endpoint`、`session_timeout_ms`、`reconnect_base_delay`、`reconnect_max_delay`、
`reconnect_attempt`、`_auto_reconnect`、`_pending_reconnect_forever`、`_reconnect_task`。

方法：
- `configure(prog_id=None, host=None, update_rate_ms=None, browse_max_depth=None, **_ignored)`
- `async connect()` / `async bootstrap()` / `async disconnect()`
- `async get_all_nodes()`（返回纯数据列表）
- `async start_subscription(callback)`、`set_subscription_callback(callback)`
- `async write_node_value(node_id, value)`（bool 成功/失败或抛）
- `async write_values_batch(node_ids, value, display_names=None)` → `(success, fail)`
- `set_manual_items(items)` / `manual_items`：自动扫描之外的手工点位清单
  `[{"item_id","name"}]`；bootstrap 时并入 nodes 并一起 AddItems/订阅
- `async add_manual_node(item_id, name=None)`：已连接时立即 AddItems、读初值、返回节点
- `async remove_manual_node(item_id)`：从手动清单/点位表移除，已连接时 RemoveItems
- `async reconnect_forever()`、`stop_reconnect()`
- 回调：`on_state_change(state, detail)`、`on_connection_lost()`、`on_nodes_recovered()`

行为要求（与 UA 版对齐）：
- 断线检测：周期 `get_server_status()`（不超过 10s）或订阅/读写异常；不得以"没数据变化"判定断线。
- 自动重连：指数退避 + 抖动，重连后**复用已浏览的点位表**（不重新 browse），重新 AddItems +
  订阅 + 重新读初值，然后触发 on_nodes_recovered；世代号机制防旧回调污染新连接。
- bootstrap 失败/超时必须回到重连流程，不能卡死。
- 订阅回调把 DaValue 转成 (node_id, value, timestamp) 调 UI 回调；quality 非 Good 时记日志并保留值。
- 不得阻塞 Qt 事件循环过久：AddItems/Browse 每批（如 300 个）后 `await asyncio.sleep(0)`。
- 在无 Qt 的自动化测试中也要能跑（消息泵用 client.pump_events()，引擎可在心跳循环里调用）。

## 3. UI / 入口 / 打包（T2 负责）

- `ui/tabs/tab_settings_da.py`：`DaTabSettings(TabSettings)`，覆盖：
  - `_build_connection_group(layout)`：DCOM 主机、OPC Server ProgID（可编辑 QComboBox + 刷新按钮，
    调用 client.list_servers）、更新周期 ms、浏览深度；按钮/状态标签与 UA 版一致。
  - `_build_extra_groups(layout)`：手动添加控制点位区（ItemID/显示名/添加/删除/持久化
    `da_manual_nodes`）；修改点已加入 `opcda_API_CONTRACT.md` 与测试。
  - `_apply_form_to_engine(interactive)`：校验并 `engine.configure(host=..., prog_id=..., update_rate_ms=..., browse_max_depth=...)`。
  - `_persist_connection_params()`：存 `data_store["da_connection"]`。
- `ui/da_main_window.py`：`class DaMainWindow(MainWindow)`，设置
  `ENGINE_CLASS = OpcDaClientEngine`、`SETTINGS_CLASS = DaTabSettings`，标题改为 DA 版。
- `main_da.py`：与 main.py 相同，仅使用 DaMainWindow；日志加一行协议信息。
- `LightingControlDA.spec` + `build_da.bat`：单文件打包 py3.8/venv38，输出
  `dist/LightingControlDA_v1.2.4.exe`；hiddenimports 至少 comtypes、comtypes.client、
  comtypes.automation、comtypes.typeinfo、comtypes.connectionpoints；
  数据文件与 UA 版一致（ui/style.qss、data/lighting_config.json、lighting_logo.png）。
- 现有 `ui/main_window.py` 已由 Lead 改造出 `ENGINE_CLASS`/`SETTINGS_CLASS` 插槽，
  `ui/tabs/tab_settings.py` 已抽出 `_build_connection_group()`；不要重复修改这两个文件
  （如需微调先给 Lead 发消息）。

## 4. 测试与验收（Lead 负责汇总）

- `tools/da_selftest.py`（T1）：命令行连 Matrikon `Matrikon.OPC.Simulation.1`，打印
  服务器状态、浏览结果、AddItems/Read/Write/Subscribe 结果；--host/--progid 可配。
- `tests/test_da_*.py`：转换/错误映射单测 + 引擎假客户端单测 + Matrikon 集成测试
  （无服务器/未安装时 skip）。
- 现场验证目标：Matrikon 模拟服务器上浏览、读写、500ms 订阅；停止/重启服务器后引擎自动恢复；
  打包 exe 在 Windows Server 2012 环境启动并连上 DA 服务器。
