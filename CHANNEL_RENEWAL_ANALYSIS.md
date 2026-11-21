# 灯光控制软件"连接满 1 小时整点断线"问题分析与自动化测试

- 分析对象：LightingControl V1.4.0（源码 `opc/client_engine.py`，打包产物 `dist/LightingControl_v1.4.0.exe`）
- 现场环境：本机 OPC DA 模拟控制点 → DA/UA 网关（neuopc 类）→ `opc.tcp://127.0.0.1:48401`
- 结论：**不是网络问题，也不是 DA→UA 网关的问题；是客户端"安全通道续约监督协程"在启动瞬间自杀，导致安全通道从头到尾没有续约过一次。** 服务端按 OPC UA 规范在 SecurityToken 寿命（1 小时）到期后拒绝旧 TokenId，客户端才被被动断开。

---

## 1. 现场证据：断线时刻 = 连接时刻 + 3600 秒

`dist/logs/app.log`：

    2026-10-23 11:32:28,533 - [INFO] - client_engine.py:737 - Connected to opc.tcp://127.0.0.1:48401/. SecureChannel timeout=3600000ms, Session timeout=60000ms
    2026-10-23 11:32:29,710 - [INFO] - client_engine.py:536 - [STATE] loading -> connected (bootstrap complete)
    ...（整整 1 小时内没有任何 [RENEW] 记录）...
    2026-10-23 12:32:28,652 - [WARNING] - connection.py:429 - Received an error: ... Reason='Could not verify security on incoming request.'
    2026-10-23 12:32:28,667 - [CRITICAL] - ua_client.py:116 - Received an error: ...
    2026-10-23 12:32:28,671 - [ERROR] - ua_client.py:100 - Got error status from server: The SecureChannelId and/or TokenId are not currently in use.(BadTcpSecureChannelUnknown)
    2026-10-23 12:32:35,344 - [INFO] - client_engine.py:536 - [STATE] connected -> disconnected (asyncua watchdog: Connection is closed)

    2026-10-23 12:32:35,357 - [WARNING] - tab_settings.py:564 - Connection lost but auto-reconnect is disabled (复选框未勾选)

三个关键事实：

1. `SecureChannel timeout=3600000ms` —— 客户端向服务端申请的安全通道寿命正好 1 小时；
2. 12:32:28 服务端报 `Could not verify security on incoming request.` + `BadTcpSecureChannelUnknown` —— 典型的 **SecurityToken 过期/未知**，即客户端没有在寿命内续约；
3. 断线发生在连接后 **3600 秒整**，与 token 寿命完全吻合。

另外，现场配置 `dist/data/lighting_config.json` 中 `"auto_reconnect": false`（日志里也明确提示复选框未勾选），所以这次断线后软件停在"待连接"，不会自动恢复——这是"断线后无法无感续约"的第二层原因。

## 2. 根因 A：续约协程在 loading 状态被误判为"未连接"而立即退出

`opc/client_engine.py` 为了修 asyncua 自带续约"失败一次就永久退出"的问题，接管了续约：连接成功后取消 asyncua 的 `_renew_channel_task`，另起 `_renew_supervisor`。问题出在这段（修复前）：

    async def _renew_supervisor(self, my_gen):
        global_logger.info("[RENEW] SecureChannel renewal supervisor started ...")
        try:
            while my_gen == self._generation and self.connected and self.client:   # <-- BUG
                ...

    # _connect_once() 中的启动时序（修复前）：
    self._takeover_channel_renewal(my_gen)     # create_task(_renew_supervisor)
    self._start_monitor(my_gen)
    self._set_state(self.STATE_LOADING, "transport established", ...)

`connected` 属性等价于 `state == "connected"`。而 create_task 出来的协程第一次被调度时，引擎还在 `loading`（节点遍历/建订阅阶段，bootstrap 尚未完成），于是 while 条件为 False，协程直接 return——**一次续约都没有做**。源码运行日志可以精确复现这个 1 毫秒的"启动即退出"：

    00:49:42,633 - [INFO] - client_engine.py:1040 - [RENEW] SecureChannel renewal supervisor started (generation 1).
    00:49:42,634 - [INFO] - client_engine.py:1097 - [RENEW] Renew supervisor task exited.

为什么恰好是 1 小时而不是 README 里说的 45 分钟？因为 asyncua 自带的 `_renew_channel_loop` 是在 **75% 寿命（45 分钟）** 续约，而它已经被本引擎取消；自研续约又在启动时退出。于是 0 次续约，安全通道一直用到 **100% 寿命（60 分钟）**，被服务端拒绝。

### 最小化复现（把 1 小时压缩成 8 秒）

`tools/diagnose_channel_expiry.py` 起一个 Basic256Sha256 测试服务器，把授予的通道寿命改成 8 秒，并在 token 过期仍未续约时主动断开 TCP（模拟真实网关的硬过期）。修复前实测：

    0.0s  OPEN_ISSUE
    0.1s  OPEN_ISSUE（GetEndpoints 探测 + 正式连接）
    0.1s  [RENEW] SecureChannel renewal supervisor started
    0.1s  [RENEW] Renew supervisor task exited            <-- 启动即退出
    8.1s  服务端 token 过期，关闭连接
    8.2s  [STATE] connected -> reconnecting（断开，非无感）

完全对应现场"1 小时断线"的形态。

### 根因 B（首次连接必现）：qasync/Qt 重入把后台任务变成"僵尸任务"

现场 `dist/logs/app.log` 里不仅没有 `[RENEW]`，连每分钟一条的 `[HEARTBEAT]` 也没有——说明首次连接时两个后台任务连"第一行日志"都没执行。用"从 exe 里抽出的冻结版 `opc.client_engine` + venv38(qasync 0.28/PyQt5 5.15/asyncua 1.1.5)"做的对照实验定位到：

- `ui/tabs/tab_settings.py::_connect_and_load()` 在 `await engine.connect()` 之后立刻
  `QProgressDialog.setValue(40)`；
- Qt 的 `QProgressDialog::setValue()` 在对话框已显示且为模态时会内部调用
  `QCoreApplication.processEvents()`（实测 0ms QTimer 会在 setValue 内部触发）；
- qasync 由 Qt 定时器驱动 asyncio，这次重入会在 `connect_flow` 协程尚未返回时再次
  进入事件循环，asyncio 直接抛
  `RuntimeError: Cannot enter into task ... while another task ... is being executed`；
  刚 create_task 的 `_renew_supervisor` / `_monitor_connection` / asyncua 自带续约任务
  的第一次 `__step` 因此丢失，Task 永远停在 pending（done()==False），之后不会再被调度。
- 该异常由 qasync 的 logger 发到 root handler（stderr）；打包版 `console=False`，
  stderr 不可见，所以 app.log 里完全看不到线索。这解释了"现场日志没有 RENEW/HEARTBEAT"。

对照实验结果（同一份源码快照、同一个 20 秒寿命服务器）：
带 QProgressDialog → renewal_count = 0、`_renew_task` pending；去掉对话框 →
renewal_count = 3、服务端每 10 秒收到一次 `OpenSecureChannel(type=Renew)`。

**这意味着只修根因 A 还不够**：僵尸任务是 pending，不是 done，
"task 为 None 或 done 才补拉"的看门狗逻辑抓不到它，现场故障会原样保留。

## 3. 附带发现：DA 网关的 1601 时间戳会吞掉所有实时推送

现场日志里还有每秒一条的：

    [ERROR] - client_engine.py:410 - datachange_notification failed: [Errno 22] Invalid argument
    File "opc\client_engine.py", line 404, in datachange_notification
    OSError: [Errno 22] Invalid argument

原因：DA→UA 网关对没有时标的 DA 点会把 `SourceTimestamp` 填成 1601-01-01（FILETIME 0），而 Windows 上 `datetime.astimezone()` 对 1970 年以前的时间会抛 `OSError [Errno 22]`。旧代码在把值交给界面前转换时间戳，异常被 `except` 捕获后整条推送被丢弃——**界面上的实时值其实一直没刷新**。已一并修复（转换失败时退回本地当前时间，数据照常上报）。

## 4. 修复内容（`opc/client_engine.py`）

1. 新增 `_link_usable()`：`loading` / `connected` 都算"链路可用"，续约与心跳不再把 loading 误判为断开；
2. 调整 `_connect_once()`：先把状态推进到 loading，再 create_task 续约/看门狗，消除启动竞态；
3. **在 `_connect_once()` 末尾主动 `await asyncio.sleep(0)` 两轮**：让刚创建的后台协程先跑到第一行（登记 started 时间戳并进入 sleep）再返回 UI，从根上避开 QProgressDialog.processEvents() 的重入窗口；
4. 重写 `_renew_supervisor()`：续约时刻由"上次成功续约/建链时间 + 50% 寿命"推算（不再从协程启动时刻重新计时），非预期退出会记录 WARNING；
5. 新增 `_ensure_link_tasks()`（async，24/7 自愈）：bootstrap 完成时、每次心跳时补拉"静默退出"的任务；并且用"任务创建时间 vs 协程真正开始执行时间"识别 qasync 重入造成的**僵尸任务**（pending 但永不执行，done() 为 False），取消并重建；
6. 监控循环同样改用 `_link_usable()`，避免长耗时 bootstrap（大点表遍历）把看门狗"熬死"；
6. 新增诊断计数 `renewal_count` / `last_renewal_monotonic`，供自动化测试和现场长稳工具断言；
7. 订阅时间戳转换增加 1601/1970 年以前保护。

## 5. 自动化测试

### 5.1 快速回归测试（约 26 秒，无需现场网关）

    python -m unittest tests.test_channel_renewal -v

包含 5 个用例：

- `test_renew_supervisor_survives_loading_state`：把引擎置为 loading，断言续约协程不退出，并在 600ms 寿命的 50% 时真正发出 `renew=True` 的 OpenSecureChannel（修复前此用例必失败）；
- `test_zombie_supervisor_is_replaced`：模拟"创建了 5 秒却一步都没执行"的僵尸续约任务，断言看门狗能识别并替换它、且新任务真正开始运行；
- `test_dead_supervisor_is_restarted_by_watchdog`：续约协程静默退出后必须被心跳看门狗补拉；
- `test_no_disconnect_across_three_token_lifetimes`：起真实 Basic256Sha256 服务器，把通道寿命压到 6 秒并在未续约时硬断开 TCP；连续跨 3 个寿命周期（约 21 秒）断言：始终 connected、无重连、0 次 token 过期、续约 >= 2 次；
- 2 个时间戳兼容用例（1601 时间戳不得丢推送）。

修复前实测：loading 用例与僵尸用例失败；端到端用例在约 6 秒被断开。修复后 6 个用例全部通过（py3.12/asyncua 1.1.8 与 py3.8/asyncua 1.1.5 双环境均通过）。

另用 qasync + QProgressDialog 的真实 GUI 场景做了对照（`tools/diagnose_qasync_zombie.py`，6 秒通道寿命；测试服务器用 `tools/_short_lifetime_server.py`）：修复前带对话框 renewal_count=0、续约任务 pending；修复后带对话框 24 秒内 `renewal_count=7`，无一次断线。

### 5.2 现场长稳测试（默认 75 分钟，跨过 1 小时整点）

    python tools/renewal_soak_test.py --config dist/data/lighting_config.json --duration-minutes 75

（也可用 `--host/--port/--user/--password` 手工指定；密码支持读取配置里的 DPAPI 密文，需同一 Windows 用户运行。）

它直接复用软件的 `OpcClientEngine`，持续记录：

- 每次续约的时间点、通道寿命、续约次数；
- 状态迁移与 `connection_lost`（任何一次掉线都判定失败，并继续观察能否自动恢复）；
- 每 10 秒主动读一次 `ServerState(i=2259)` 验证数据面；
- 订阅推送计数（跨续约后数据面是否仍然活着）。

默认 75 分钟正好覆盖"连接满 1 小时"的故障点，结束时输出 PASS/FAIL、原因和 JSON 报告（`logs/renewal_soak_report.json`）。退出码：0 通过 / 1 失败 / 2 初始连接失败。

短寿命服务器上实测：6 秒寿命、24 秒观察 → 7 次续约、0 次断线、27 条推送，判定 PASS。

### 5.3 全量回归

    python -m unittest discover -s tests -t .

（原有用例 + 新用例，约 90 秒，全部通过。）

## 6. 现场落地建议

1. **重新打包（已完成）**：修复已通过 `build_win2012.bat` 重新打进 `dist\LightingControl_v1.4.0.exe`
   （md5：旧 `a6d2a19d…` → 续约修复 `19bb27c8…` → 加入第 7 节自动重连修复后
   `bc647c78221deb081cb00faefda06f7b`）。冻结构建冒烟
   `tools/smoke_frozen_build.py` 全部通过；headless 对接 10 秒寿命网关实测：45 秒内
   `Secure channel renewed successfully` ×9、0 次断线、0 条 `[Errno 22]`。现场如需
   再改代码，重新执行 `build_win2012.bat` 即可；
2. **勾选"自动重连"**：`dist/data/lighting_config.json` 当前是 `"auto_reconnect": false`。续约修好后理论上不会再整点断线，但断网、网关重启仍需自动恢复，建议勾选；
3. **先做 5 分钟冒烟，再跑 75 分钟整点回归**：

       python tools/renewal_soak_test.py --config dist/data/lighting_config.json --duration-minutes 5
       python tools/renewal_soak_test.py --config dist/data/lighting_config.json --duration-minutes 75

4. 期望日志形态（修复后）：连接后 30 分钟、60 分钟（1 小时寿命的 50% 节点）各出现一条
   `[RENEW] Secure channel renewed successfully ... Active channel lifetime=3600000ms`，
   且 `[STATE]` 不再出现 `connected -> reconnecting/disconnected`。

## 7. 追加修复（2026-10-23 15:28 现场）:首次启动勾选自动重连无法连接

**现象**（现场截图）：启动后立即勾选"自动重连"，日志里 supervisor 直接发起连接，
服务端对 `ActivateSessionRequest` 返回 `BadUserAccessDenied(2149515264)`，按退避
无限重试；停止自动重连后手动点一次连接就成功。

    15:28:04  [STATE] disconnected -> reconnecting (reconnect supervisor requested)
    15:28:04  [RECONNECT] Attempt 1: retrying in 9.0s...
    15:28:13  ServiceFault (BadUserAccessDenied ...) in response to ActivateSessionRequest
    15:28:13  [RECONNECT] Attempt 1 failed (BadUserAccessDenied)

**根因：两条连接路径的参数注入不一致。**

- 手动连接 `_connect_and_load()` 会先调用 `_apply_form_to_engine()`，把界面上的
  host/port/用户名/密码注入引擎，再 `engine.connect()`；
- 自动重连路径 `_schedule_reconnect_forever()` → `engine.reconnect_forever()` →
  supervisor 直接调用 `engine.connect()`，**完全不经过表单注入**。而
  `MainWindow` 构造的是 `OpcClientEngine()` 默认值（localhost:48401、空用户名/密码），
  于是首次连接是匿名会话，要求鉴权的 DA→UA 网关在 ActivateSession 阶段直接拒绝。
  手动连接因为先注入了账号密码，所以一次成功。

**修复（ui/tabs/tab_settings.py）**

1. `_schedule_reconnect_forever()` 在打开总闸前调用
   `_apply_form_to_engine(interactive=False)`，保证 supervisor 首次连接就使用
   界面/配置里的地址与账号密码；
2. `_on_nodes_recovered()` 补充 `_publish_nodes_to_bus()`：supervisor 自己建立首次
   连接时没有 UI 连接流程参与，原先即使连上也显示"总点位数 0"；手动连接处的重复
   发布同时去掉，两条路径统一由该回调发布。

**回归测试（tests/test_auto_reconnect_intent.py 新增 3 个用例）**

- 配置持久化 auto_reconnect=true 冷启动 → 引擎必须已拿到 host/user/password；
- 启动后手动勾选自动重连 → 勾选动作必须立即注入当前表单凭据；
- supervisor 自建链后 `_on_nodes_recovered` 必须把 engine.nodes 发布到 DataManager。

修复前实测两个断言分别为 `'localhost' != '10.9.8.7'`、`'' != 'admin'`（即现场
"匿名 + 默认地址"），修复后全部通过。

**冻结构建验证**：仅接受用户名密码、8 秒通道寿命的测试网关 + 配置
`auto_reconnect=true` 冷启动重打包后的 exe：日志 `BadUserAccessDenied` **0 次**，
15:35:06 连接成功、15:35:07 发布 2 个点位、随后每 4 秒续约一次（5 次成功），
全程保持 connected。

---

附：本次修改文件
- `opc/client_engine.py`（续约/看门狗/时间戳修复）
- `tests/test_channel_renewal.py`（新增快速回归测试）
- `tests/test_secure_channel_renewal.py`（既有用例适配新的 1 秒最短续约周期）
- `tools/renewal_soak_test.py`（新增现场长稳测试程序）
- `ui/tabs/tab_settings.py`（自动重连路径凭据注入 + 重连后点位发布）
- `tests/test_auto_reconnect_intent.py`（新增冷启动勾选自动重连的凭据/点位回归用例）
