# OPC DA 直连客户端（照明控制系统 DA 版）

本目录新增一个与现有 OPC UA 版功能对齐、但**直接通过 COM/DCOM 连接经典 OPC DA 服务器**的完整客户端：
连接/断开、浏览点位、实时订阅、分组控制、定时任务、日志、自动重连，复用同一套 PyQt5 界面与
DataManager/调度器，仅替换通信引擎与设置页的连接配置区。

- 入口：`main_da.py`（源码） / `dist/LightingControlDA_v1.2.1.exe`（打包产物）
- 引擎：`opcda/engine.py`（OpcDaClientEngine）
- 低层 COM 客户端：`opcda/com.py` + `opcda/client.py`
- 接口契约：`opcda_API_CONTRACT.md`

## 1. 环境要求

| 项 | 要求 |
|---|---|
| 操作系统 | Windows Server 2012 / 2012 R2 / Win7 SP1 及以上（与 UA 版一致） |
| 运行时 | 无额外运行库；不需要安装 OPC Core Components、pywin32 |
| Python（源码运行/构建） | 3.8（`venv38`，打包目标）；开发机可用 3.10+ |
| OPC 服务器 | 任意 OPC DA 2.0/3.0 服务器（本地或 DCOM 远程），如 Matrikon OPC Simulation、Kepware、西门子/施耐德网关等 |

> 客户端直接调用 `IOPCServer / IOPCItemMgt / IOPCSyncIO / IOPCAsyncIO2` 等 OPC DA 核心接口，
> **不依赖 OPC DA Automation Wrapper**（`OPC.Automation`），因此目标机器只要安装了 OPC DA 服务器
> 并注册了 ProgID 即可，不用额外装 OPC Foundation Core Components。

## 2. 快速开始（本机 DA 服务器）

1. 启动 OPC DA 服务器（例：Matrikon OPC Simulation Server，ProgID `Matrikon.OPC.Simulation.1`）；
2. 运行 `python main_da.py`（或双击 exe）；
3. 进入「设置」页：
   - **DCOM 主机**：本机填 `localhost`（或 `127.0.0.1`），远程填服务器计算机名/IP；
   - **OPC Server ProgID**：可点「刷新服务器列表」枚举（本机：OPC.ServerList/OPCEnum +
     注册表两套；远程：远程 OPC.ServerList/OPCEnum + Remote Registry 兜底），也可手工输入；
     本机地址写成主机名、localhost 或任意一块网卡的 IP 都能被正确识别为本机；
   - **订阅周期**：默认 500ms；
   - **浏览深度**：默认 3 层，地址空间很深时增大；
   - 勾选「启动时自动连接」「断线后自动重连」；
4. 点「连接并挂载服务器节点」，监控/分组/调度/日志页的使用方式与 UA 版完全相同。

### 2.1 手动添加控制点位（自动扫描之外的补充）

自动扫描会遍历 OPC DA 地址空间；如果个别点位不在扫描结果里（FLAT 命名空间、
浏览权限受限的分支、私有标签、超大地址空间只扫描了一部分），可在设置页
「手动添加控制点位」区补充：

1. 在 **OPC ItemID** 输入完整 ItemID（例如 `Device.Channel.Tag` 或 `Random.Int1`）；
2. 可选填写**显示名称**（留空则用 ItemID）；
3. 点「添加点位」：
   - 已连接：立即 AddItems 到当前组、读一次初值并发布到界面，订阅会自动覆盖新点位；
   - 未连接：仅保存到配置，连接成功后自动挂载；
4. 点位会持久化到 `data/lighting_config.json` 的 `da_manual_nodes`，重启/断线重连后自动恢复；
5. 在「已保存」列表中选中点位，点「删除选中」可同时从配置、引擎和服务器组中移除。

> 如果自动扫描失败（浏览抛错），只要配置了手动点位，连接仍会继续建立并使用手动点位。

> **别名不会被手动加点/重连覆盖**：别名（备注名）保存在 `data/lighting_config.json`
> 的 `aliases` 中，是用户配置；引擎扫描点位时自带的 `alias` 只是默认显示名。
> DataManager 以持久化别名为唯一事实来源，因此手动添加控制点、断线重连、
> 重新发布点位表都不会把已导入的别名冲回原始 ItemID（修复前会出现
> 「加了一个点位后所有别名消失、必须重新导入别名表」）。

## 3. 高兼容性设计

- **DA 2.0/3.0**：核心接口按规范实现；`IOPCCommon.SetLocaleID` 失败不影响连接；服务器可选接口缺失时降级。
- **浏览**：优先层次浏览（`IOPCBrowseServerAddressSpace`），服务器只支持 FLAT 时自动退回扁平枚举；
  层次深度/数量可配置，避免超大地址空间卡死。
- **订阅**：优先 `IOPCAsyncIO2 + IOPCDataCallback`（500ms 推送，只回传变化点位）；
  老服务器不支持异步接口时，引擎自动降级为 `IOPCSyncIO` 轮询（仍然批量读取，不逐点往返）。
- **数据类型**：按 `vtCanonicalDataType` 做 VARTYPE ↔ Python 转换（BOOL/I1/I2/I4/I8/UI1/UI2/UI4/UI8/R4/R8/BSTR 等），
  写值前按点位规范类型构造 VARIANT，避免类型不匹配被服务器拒绝。
- **质量码**：保留 OPC quality；非 Good 的推送不覆盖最后良好值，只在日志中告警。
- **远程 DCOM**：`host` 填远程计算机名即可（`CoCreateInstanceEx`）；详细配置见第 6 节。
- **位宽**：64 位客户端可连接 32/64 位 **out-of-proc**（EXE 型）DA 服务器（DCOM 自动编组）；
  仅当服务器是没有 DCOM 注册的 32 位 **in-proc DLL** 时，才需要 32 位客户端构建（见第 8 节）。

## 4. 高性能设计

- 所有读写/add/remove 都是**批量数组调用**（`IOPCItemMgt.AddItems`、`IOPCSyncIO.Read/Write`），
  1000+ 点位按 300~500 一批，无逐点网络往返；
- 单个 OPC group、单一订阅周期（默认 500ms），服务端只推送变化的点位；
- COM 在 GUI 主线程以 STA 运行，回调直接更新 DataManager；AddItems/Browse 分批后让出事件循环，
  避免界面卡顿；
- 启动时一次 SyncRead 拿全量初值；重连复用已浏览的点位表，不重新遍历地址空间。

## 5. 断线与自动重连

- 健康检查：每 ≤10 秒调用 `IOPCServer.GetStatus()`（不是"多久没收到数据"），并监控订阅/读写异常；
- **心跳看门狗自愈**：心跳看门狗任务若被静默退出（正常 return / 被取消 / qasync 重入造成的
  "僵尸 pending"），由 `_ensure_link_tasks()` 自动识别并重启；触发点包括 bootstrap 结束、
  心跳每 tick、SyncRead 轮询每轮、写入路径，以及 UI 刷新定时器（`ensure_liveness()`）的兜底。
  也就是说健康检测不会"自己停止"，断线一定能被发现；
- 周期成功心跳日志（`[HEARTBEAT] OPC DA link OK ...`）打在 **DEBUG** 级，不再污染 INFO 分类；
  日志页把级别切到 DEBUG 才能看到；真正的失败/自愈仍为 WARNING/ERROR；
- 断线后按指数退避 + 随机抖动重连（默认 10s→20s→40s→60s，可在设置页调整）；
- 重连成功后复用点位表，重新 AddItems + 订阅 + 读初值，再通知 UI 重算统计；
- 世代号机制保证旧连接的回调/任务不会污染新连接；断开操作幂等、有界，不会挂死。

## 6. 远程 DCOM 配置（现场常见问题）

远程连接时，"连不上"绝大多数是 DCOM 权限/防火墙，不是客户端问题。建议按以下顺序检查：

1. **网络/端口**：放行 TCP 135（RPC Endpoint Mapper）和动态 RPC 端口范围 49152-65535
   （或在服务器上把 DCOM 限制到固定端口）；关闭客户端/服务器防火墙做对照测试；
2. **服务器端**：`dcomcnfg` → 组件服务 → 计算机 → 我的电脑 → 属性：
   - COM 安全 → 访问权限/启动和激活权限：加入客户端运行账号，勾选本地/远程访问、本地/远程激活；
   - OPC 服务器的 AppID（如 Matrikon/Kepware）→ 标识：改为「交互式用户」或一个两台机器都有权限的域账号；
3. **客户端**：`dcomcnfg` → 我的电脑 → 属性 → 默认属性：默认身份验证级别「连接」，默认模拟级别「模拟」；
4. **工作组环境**：两台机器建同名同密码的本地账号，并以该账号运行客户端；或开启相关
   `LocalAccountTokenFilterPolicy`（仅内网、注意安全风险）；
5. **账号**：客户端以「交互式用户/服务账号」运行，确保对服务器有 DCOM 启动权限。

> 客户端当前使用进程身份进行 DCOM 认证；如需"以指定账号连接远程 DA"，建议用该账号启动客户端
> （计划任务/服务），或在域环境中给运行账号授权。

## 7. 自动化测试

- 单元测试（无 COM 环境）：
  ```
  python -m unittest tests.test_da_client_unit tests.test_da_engine tests.test_da_manual_ui -v
  ```
- Matrikon 集成测试（本机已安装 DA 服务器时）：
  ```
  set LC_DA_TEST=1
  python -m unittest tests.test_da_integration_matrikon -v
  ```
- 低层自检（输出服务器状态/浏览/读写/订阅统计）：
  ```
  python tools/da_selftest.py --host localhost --progid Matrikon.OPC.Simulation.1
  ```

## 8. 打包（Windows Server 2012）

```bat
build_da.bat
```

使用 `venv38`（Python 3.8 + PyInstaller 6.19）生成 `dist\LightingControlDA_v1.2.1.exe`。
可写数据（`da_connection` 配置、日志）在 exe 同级目录；只读资源打进 `_MEIPASS`。

如现场只有 32 位 in-proc DA 服务器：准备 32 位 Python 3.8 + PyQt5(win32) + comtypes 的独立 venv，
复用同一个 spec 打包即可（客户端代码不依赖位宽）；out-of-proc 服务器无需此步骤。

## 8.1 已验证环境与结果（2026-10-23）

| 验证项 | 结果 |
|---|---|
| 服务器 | Matrikon OPC Simulation Server 2.0（32 位 out-of-proc，Windows 服务；本机 64 位客户端） |
| 连接/状态 | CreateObject + IOPCServer.GetStatus 成功，连接 0.08-0.11s |
| 浏览 | 层次浏览 153 个点位（含 `Random.*`、`Bucket Brigade.*`、`Write Only.*`）；FLAT 回退同样 153 |
| 批量 AddItems | 153/153 成功（约 7ms，一次数组调用） |
| 批量 SyncRead | 139/153 有值（其余为 Matrikon 的 Write Only / 错误注入点，保持缓存值） |
| 批量 SyncWrite | `Bucket Brigade.Boolean/Bool1/Bool10` 等 hr=0，DEVICE 回读一致；只读点位返回 `OPC_E_BADRIGHTS`；数组写入安全抛错 |
| 订阅 IOPCAsyncIO2 | 500ms 组、5 秒 11 次 OnDataChange / 602 条更新，hClient→ItemID 全部映射成功；改周期 250ms 后继续回调；stop 后无残留 |
| 客户端自检 | `tools/da_selftest.py` 在 py3.12+comtypes1.4.17 与 py3.8+comtypes1.4.12 均 PASS=73 FAIL=0 |
| 引擎（无 Qt 单测） | 19 个用例：状态机/订阅子集/hClient 映射/坏品质/轮询降级/断线重连复用点位表/批量写/disconnect 幂等 |
| 引擎源码端到端 | 连接→浏览→AddItems→初值→订阅→写→断开全通过；模拟断线自动重连 0.8s、复用 153 点缓存、代数号前进 |
| Qt/qasync 真实 UI | offscreen 启动 DaMainWindow：153 点发布到 DataManager、订阅 async、写入成功、pump_events_enabled=False |
| 打包 exe | `dist/LightingControlDA_v1.1.0.exe`（66,617,731 bytes，md5 `abad1bbb742e92c27ec7ce0008dee818`；py3.8/PyInstaller onefile）。`tools/smoke_frozen_build.py` 全部 PASS；headless 启动后自动连接 Matrikon、浏览点位并发布、订阅建立；配置带 `da_manual_nodes` 时日志显示 `manual=1` 并自动挂载。 |
| 服务器重启 | Matrikon 以 Windows 服务运行，停止/启动需要管理员权限（普通账号 `net stop` 被拒绝）；引擎的重连路径已由"假客户端模拟断线"自动化用例覆盖，现场可用管理员重启服务或在测试网关上验证 |
| 打包 exe（v1.2.1，2026-11-17） | `dist/LightingControlDA_v1.2.1.exe`（66,630,927 bytes，md5 `6142d71d2729817a370d962e657e3546`；py3.8 / PyInstaller 6.19 onefile）。`tools/smoke_frozen_build.py` 全部 PASS（MainWindow 初始化、qasync 事件循环、系统托盘、`_MEIPASS` 资源、可写目录、1024x768 布局）。本版本包含：手动加点别名保护、心跳看门狗自愈（成功日志 DEBUG + 设置页“心跳 Xs 前”实时指示）、服务器列表 OPCEnum/远程注册表枚举、分组与调度页三列布局、定时任务看门狗/补跑/即时重检。 |

性能参考：153 点全量 AddItems 约 7ms、SyncRead 约 6ms、写入约 15ms 往返；订阅周期最小 100ms（默认 500ms）。

## 9. 已知限制 / FAQ

- **服务器列表为空**：枚举顺序为「OPC.ServerList/OPCEnum → 注册表 Component Categories」；
  本机来自注册表（64/32 位视图都扫），远程还可用 Remote Registry 兜底。远程枚举受 DCOM 权限与
  目标机组件安装情况影响，仍可直接手工输入 ProgID（在服务器注册表 `HKCR\<ProgID>` 可查）。
  状态栏会显示本次枚举的数量或失败原因（不再只弹一个空提示）。
- **写入被拒（E_ACCESSDENIED/BadUserAccess）**：点位只读，或服务器安全策略禁止写；用
  `tools/da_selftest.py` 看 access 权限（1=R, 2=W, 3=RW）。
- **读到 Bad 质量**：Write Only 点位不允许读；设备断线时会返回 Bad；引擎保留最后良好值并告警。
- **服务器重启**：自动重连会重新建组、加项、订阅；若服务器改了地址空间，需手动重新连接刷新点位表。
- **定时任务到点不执行**：已修复三类根因——(1) 调度守护协程静默退出/僵尸 pending 后
  由看门狗自动重启；(2) 事件循环被长任务或系统休眠阻塞跨过触发分钟时，下一 tick 会按
  `(上次检查, 当前]` 逐分钟补跑（最多补最近 10 分钟）；(3) 计划在本分钟已检查后才添加/
  修改时，会立即重检当前分钟。同一计划同一分钟通过去重键只执行一次。若仍不执行，
  请确认计划已勾选启用、星期包含当天、目标分组存在且组内有成员。
- **心跳日志看不到**：这是按现场要求设计的行为——周期心跳成功日志是 DEBUG 级（移出 INFO
  分类），默认「全部/INFO」看不到，且每 6 次探测才记 1 条（默认约 60 秒一条）。三种确认方式：
  (1) 设置页「链路状态」实时显示 **“心跳正常（Xs 前，每 10s 探测）”**，超过 3 个周期未更新
  会变色告警；(2) 「实时日志」顶部有提示，把级别切到 DEBUG 即可看到 `[HEARTBEAT]` 记录；
  (3) 连接成功时会有一条 INFO 说明心跳策略。链路异常与自愈事件仍是 WARNING/ERROR。
- **别名又被覆盖/消失**：请确认使用的是包含"别名保护"修复的版本（DataManager 以持久化
  别名为唯一事实来源）；手动加点、重连、重新发布点位表都不再改动用户别名。
- **DCOM 远程 vs 本机**：本机连接不走 DCOM 网络，不受防火墙/身份影响；远程才需要第 6 节配置。
- **数组类型点位**：DA 数组点位可读（返回 list），但客户端不支持写数组（显式抛 `OpcDaError`）；
  现场灯光控制只写 Bool/数值标量，不受影响。
- **服务器重启验证**：Matrikon 默认注册为 Windows 服务，普通用户无法 `net stop`；引擎的自动重连
  能力由 `tests/test_da_engine.py` 的假客户端断线用例覆盖，现场可在测试网关上停服验证（管理员）。
