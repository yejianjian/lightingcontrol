# Lighting Control System (灯光控制系统) V1.4.0

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.8%2B-brightgreen.svg)
![Framework](https://img.shields.io/badge/framework-PyQt5%20%2F%20PySide6-orange.svg)

这是一个基于 Python 开发的专业灯光控制系统，针对 OPC UA 协议进行了深度优化。系统集成了实时监控、分组控制、定时任务调度以及智能报警功能。

> **OPC DA 直连版（新增）**：同一套界面/分组/调度/日志，另外提供直接通过
> COM/DCOM 连接经典 OPC DA 2.0/3.0 服务器的版本（不经过 DA→UA 网关、不需要
> OPC Core Components / pywin32）：源码入口 [main_da.py](main_da.py)，
> 支持自动扫描地址空间，也支持在设置页**手动单个添加/删除控制点位**（持久化，
> 重连自动挂载；**已导入的别名不会再被手动加点/重连重新发布覆盖**）；
> 打包产物 `dist/LightingControlDA_v1.2.4.exe`（62,045,783 bytes，md5 `c790c3960001f3fc98f3182edd0943e5`；
> 2026-11-21 综合修复（v1.2.2 代码审查 + v1.2.3 实时质量/状态收尾 + v1.2.4 重连并发回归）：
> **重连 supervisor 不再自我丢引用/并发双开，指数退避不再被 bootstrap 失败重置**；
> **quality 回调支持 4 位置参数 / **kwargs（三参旧回调兼容）**；
> **枚举线程保活引用改由 GUI 线程释放**；
> 字符串点位 VARIANT 二次释放、链路故障被当作"坏点"导致假连接、服务器枚举线程生命周期、
> COM 指针释放顺序、多余重连 supervisor、只读点误触发整链重连、批量写一坏全断、
> S_FALSE 误判、心跳状态码、**实时 quality 同步（通信异常动态出现/恢复）**、
> **connect 成功但 bootstrap 失败不再卡 loading**、**枚举改 daemon 线程（关窗不崩）**、
> **热更新周期以服务器实际生效值为准**、**本机 FQDN 精确识别（Windows API，不误判同名远程主机）**、
> 打包瘦身（排除 DA 版无用的 asyncua/cryptography，exe 约 -4.6 MB）等），
> 构建脚本 [build_da.bat](build_da.bat)），使用说明与 DCOM 配置见
> [docs/OPCDA_GUIDE.md](docs/OPCDA_GUIDE.md)。
>
> DA 版链路健康检测已加入**心跳看门狗自愈**（任务静默退出/僵尸 pending 会被自动重启，
> UI 刷新定时器兜底），周期心跳成功日志降为 DEBUG（设置页「链路状态」实时显示
> “心跳 Xs 前”，日志页切到 DEBUG 可查看明细）；「刷新服务器列表」支持
> OPC.ServerList/OPCEnum 枚举 + 远程注册表兜底，远程主机不再只能手工填 ProgID。
> 定时任务调度器同样加入看门狗自愈、错过分钟补跑与计划变更即时重检，不再出现
> "到点不执行"。

## 🌟 核心功能与技术亮点

- **高性能 OPC UA 通信**：
  - 采用 `asyncua` 实现异步全双工通信，支持证书认证与加密。
  - **稳定性增强**：针对 24/7 长时间运行优化了资源清理与异步心跳监控逻辑。
- **全类型 Node ID 支持**：
  - 完美支持 OPC UA 标准的所有标识符类型：数字 (`i=`)、字符串 (`s=`)、GUID (`g=`) 和 不透明型 (`b=`)。
- **智能分组与流控**：
  - 支持对海量点位进行灵活分组；「分组与调度」页为三列布局（分组架构 | 组成员管理 |
    定时控制），组成员管理的「组内控制点 / 可添加控制点」上下并列，窄屏下更易读。
  - **并发控制系统**：内置 `asyncio.Semaphore` 机制，在大规模批量操作时自动平衡网络负载，防止服务器冲击。
- **策略化调度引擎**：
  - 基于 `asyncio` 的后台调度系统，支持按时间点及其组合（如周一至周五）自动触发。
  - **可靠性**：调度守护协程具备自愈看门狗（静默退出/僵尸 pending 自动重启）；
    事件循环被长任务或系统休眠阻塞跨过触发分钟时会自动补跑；计划增删改后立即
    重检当前分钟（同一计划同一分钟去重，绝不重复执行）；断线期间任务自动排队，
    恢复后补发。
- **Excel 深度集成**：
  - 支持别名表与分组逻辑的一键快速导入/导出，具备智能格式兼容处理。

## 🔌 连接与重连稳定性设计

链路保持是整个系统最复杂的部分，全部封装在 `opc/client_engine.py` 的
`OpcClientEngine` 内部；上层 UI 只消费一个状态机，不自己 sleep 重试。

### 单一状态机

```
disconnected → connecting → loading → connected
                   ↑            │           │
                   └── reconnecting ←───────┘
```

`engine.connected` 只是状态机的只读投影（`connected ⟺ state == "connected"`），
杜绝多处并发改写布尔标志导致的界面与实际链路不一致。UI 通过
`engine.on_state_change(state, detail)` 渲染按钮与仪表盘，通过
`engine.on_nodes_recovered()` 在恢复后重算统计。

### 为什么不会再"每 45 分钟断一次"

`asyncua` 内置的安全通道续约任务（`_renew_channel_loop`）在 75% 寿命时续约，
**一旦失败就抛异常退出**，且其 `pre_request_hook` 会把该异常传播到之后所有请求。
本引擎在连接成功后接管续约：

- 提前到 **50% 寿命** 续约；
- 失败重试 3 次（5s / 10s / 20s 退避）；
- 每次重试前复位 `UAClient._open_secure_channel_exchange`。该字段只在收到响应后
  才清空，首次超时会永久残留，使后续每次"重试"都立刻抛
  *"Two Open Secure Channel requests can not happen too close to each other"* ——
  即重试形同虚设；
- 重试仍失败则在通道过期前**主动**触发受控重连，而不是等服务端切断 Socket。

### 健康判据基于链路，而不是"有没有数据推送"

灯光点位状态可能数小时不变，服务器不会推送 `datachange`。因此存活判定只使用：

- 主动读取 `ServerState`（`i=2259`）；
- `asyncua` 自身的服务器看门狗任务是否存活；
- 订阅底层 Publish 任务是否存活；
- 安全通道是否已接近寿命上限仍未续约。

**绝不**把"多久没收到推送"当作断线判据（这是旧版本每 2 分钟误重连的根因）。

### 自动重连

断线后由引擎自己的 supervisor 协程接管（UI 不再参与）：

- 指数退避 + ±20% 随机抖动，默认 `10s → 20s → 40s → 60s`（可在设置页调整）；
- 每一步都有硬超时，任何异常都会回到退避重试，不会卡在节点遍历上永久不再重试；
- 断线期间调度任务自动排队，恢复后补发（队列上限 100，超 10 分钟作废）；
- 重连成功后**复用已有点位表**（不再重新遍历上千个节点），只重读一次实时值并重建订阅；
- 世代号（generation）机制：旧连接的回调/看门狗/续约任务一律被忽略，杜绝"僵尸连接"污染新连接。

### 服务器证书轮换自愈

`Basic256Sha256` 下客户端用服务器证书加密 `OpenSecureChannel` 请求。若本地缓存的
`server_cert_0.der` 与服务器当前证书不一致（重装/还原/重新自签），服务端 RSA 解密
必然失败并**直接丢弃连接**，客户端只看到请求超时，而且每次重连都重蹈覆辙。

本引擎在进程的第一个连接时会向服务器 `GetEndpoints` 核验一次真实证书，然后固定在
内存中复用；发现不一致时把旧缓存改名为 `server_cert_0.der.stale` 供人工比对，并在
日志中记录 ERROR。因此证书轮换最多导致一次重试失败，之后自动恢复。

### 日志不得阻塞事件循环

本应用 Qt 事件循环、asyncio、OPC 订阅回调与所有本地 I/O **共用一个线程**（qasync）。
`asyncua` 会在 INFO 级别打印整个报文（1100 个点位的批量变化是 300KB+ 的单条记录），
足以把心跳读取阻塞到超时并误判断线。因此：

- `utils/logger.py` 对第三方库日志做噪声过滤与长度截断，只接 WARNING 以上；
- 日志页的批量推送合并上屏（150ms 一次，单条上限 1000 字符），不再逐条排版。

### 断开（`disconnect`）保证

幂等、有界、绝不挂死：先停看门狗与重连 → socket 仍存活时优雅关闭 Session/SecureChannel
（3s 预算）→ 无论成败强制关闭底层 Socket/Transport → 用非阻塞 `asyncio.wait(timeout=...)`
等待任务收尾 → 清空 asyncua 请求回调表（断线时它不会主动失败这些 Future）。并发或重复
调用 `disconnect()` 均安全，且在服务器已消失时依然迅速返回。

## 🏗️ 项目架构

```text
lightingcontrol/
├── core/               # 核心逻辑：计划任务、数据持久化管理
├── opc/                # 通信引擎：OPC UA 客户端实现
├── ui/                 # 界面组件：PyQt 窗口与自定义控件
├── data/               # 配置存储
├── utils/              # 通用工具：日志记录、时间转换
└── main.py             # 入口程序
```

## 🚀 快速开始

### 1. 环境准备

确保已安装 Python 3.8 或更高版本。

### 2. 安装依赖

```bash
pip install -r requirements.txt
```
*(注：主要依赖包括 `asyncua`, `PyQt5`, `qasync`, `cryptography` 等)*

### 3. 配置连接

- 本系统首次连接 OPC UA 服务器时会自动生成客户端证书 (`client_cert.der`)。
- 请确保将服务器证书放置在根目录下（或根据提示导入）。

### 4. 运行

```bash
python main.py
```

## 📦 Windows Server 2012 单文件打包

```bat
build_win2012.bat
```

脚本使用 `venv38`（Python 3.8 + PyInstaller 6.19）与
`LightingControlOPC_Win2012.spec` 打出**单文件**可执行程序：

- 产物：`dist\LightingControl_v1.4.0.exe`（约 63 MB）
- 数据文件（`ui/style.qss`、`data/lighting_config.json`、logo、`d3dcompiler_47.dll`）
  运行时解压到 `_MEIPASS`，属于**只读资源**；
- 可写数据（`client_cert.der` / `client_key.pem` / `server_cert_0.der` / `logs/`）
  由 `utils.paths.get_app_path()` 放在 **exe 同级目录**，重启后依然存在。
  这一点很重要：若写到 `_MEIPASS`，程序退出即被删除，会导致每次启动都重新生成
  客户端证书、服务端信任列表失配而握手被拒。

```text
dist\
├── LightingControl_v1.4.0.exe     # 单文件程序
├── client_cert.der                # 首次运行自动生成（请勿随意删除）
├── client_key.pem
├── server_cert_0.der              # 首次连接时从服务器核验并缓存
├── data\lighting_config.json      # 别名/分组/计划配置
└── logs\app.log
```

## 🛠️ 技术选型

- **语言**: Python 3.x
- **GUI 框架**: PyQt5 / PySide6 (支持跨平台)
- **异步框架**: `asyncio` + `qasync` (完美结合 Qt 事件循环与异步 IO)
- **安全**: `cryptography` (X.509 证书生成与解析)

## 🧪 测试

```bash
python -m unittest discover -s tests -t .
```

其中：
- `tests/test_reconnect_integration.py` 会拉起真实的 `asyncua` 加密服务器子进程、
  杀掉它、再重启，用来验证"服务器真的消失了，引擎能不能自己发现并恢复"；单轮检测
  依赖 10 秒的看门狗间隔，整个用例约 35 秒。
- `tests/test_main_window_layout.py` 用 Qt 自身的最小尺寸计算钉住"窗口必须装得进
  1024x768"这条约束，防止再出现"全屏也看不全"的退化。
- OPC DA 版另有：`tests/test_da_client_unit.py` / `tests/test_da_engine.py`（无 COM 环境单测）、
  `tests/test_da_integration_matrikon.py`（本机安装 Matrikon OPC Simulation 时自动执行，
  覆盖连接/浏览/批量读写/订阅）、`tools/da_selftest.py`（低层自检）与
  `tools/da_soak_test.py`（现场长稳，支持 `--write-tag` 周期写点位验证）。
- `tests/test_channel_renewal.py`（约 26 秒）把"连上 1 小时整点断线"压缩成 6 秒
  通道寿命的端到端回归：验证 loading 状态下续约协程不退出、qasync 重入造成的
  "僵尸任务"能被看门狗识别替换、跨 3 个寿命周期始终 connected 且续约 >= 2 次。
  故障分析与修复说明见 `CHANNEL_RENEWAL_ANALYSIS.md`。

现场长稳（默认 75 分钟，正好跨过 1 小时整点故障点；只读、不改点位）：

```bash
python tools/renewal_soak_test.py --config dist/data/lighting_config.json --duration-minutes 75
```

它复用软件的 `OpcClientEngine`，统计续约次数、断线/重连、主动读取与订阅推送，
结束时输出 PASS/FAIL 和 `logs/renewal_soak_report.json`。

打包产物还需要做一次**运行时**冒烟验证（源码模式覆盖不到 `_MEIPASS` 资源定位、
可写目录选择这类只在冻结环境才走的路径）：

```bash
python tools/smoke_frozen_build.py dist\LightingControl_v1.4.0.exe
```

它会校验：主窗口初始化、qasync 事件循环启动、托盘创建、`ui/style.qss` 能从
`_MEIPASS` 读到、日志落在 exe 同级目录，以及窗口最小尺寸没有超过 1024x768。

## 📄 开源协议

本项目采用 [MIT License](LICENSE) 开源协议。
