# Frozen v1.4.0 forensics: did the shipped exe renew its secure channel?

Date: 2026-10-23. Investigator: teammate frozen-forensics. Repo: D:\lightingcontrol.
Method: static extraction of the shipped PyInstaller PYZ + headless run of an unmodified copy of
the shipped exe against a purpose-built asyncua 1.1.8 server that grants a 20 s RevisedLifetime
and force-closes the TCP transport 24 s after the last OpenSecureChannel (absolute token expiry).

## Verdict

The shipped **v1.4.0** exe **never renews its OPC UA secure channel**.

* On the first (auto/manual) connect it logs **no `[RENEW]` and no `[HEARTBEAT]` at all**; the
  renewal supervisor and health monitor tasks are created but never execute their first step.
* On reconnects (triggered internally, no UI progress dialog) the supervisor *does* log
  `[RENEW] SecureChannel renewal supervisor started` and immediately
  `[RENEW] Renew supervisor task exited.` — it exits because the shipped while-condition
  requires `self.connected` (state CONNECTED) while it is created during state LOADING.
* The server never receives a Renew OpenSecureChannel from the exe. With a 20 s channel lifetime
  it force-closed the transport at ~24 s after every connect; the app then detected the loss and
  reconnected. With the production 1 h lifetime this is exactly the observed field failure:
  connect, silence for 1 h, `Could not verify security on incoming request` /
  `BadTcpSecureChannelUnknown` at exactly +1 h.

## Static confirmation: the exe contains the vulnerable code

* The PYZ embedded in `dist/LightingControl_v1.4.0.exe` is byte-identical to
  `build/LightingControlOPC_Win2012/PYZ-00.pyz` (sha256 a93f7e8e316b736d..., 6,519,754 bytes).
* Bundled `opc.client_engine` has 74 code objects whose firstlinenos match the pre-edit working
  source exactly (e.g. `connect`@606, `_takeover_channel_renewal`@1011, `_renew_supervisor`@1038,
  `disconnect`@1445). Disassembly shows the unconditional calls
  `self._takeover_channel_renewal(my_gen)` / `self._start_monitor(my_gen)` in `_connect_once`.
* Bundled Python 3.8 / asyncua 1.1.5 / qasync 0.28.0 / PyQt5 5.15.11 (tracebacks in the exe log
  point at `asyncio/tasks.py` line 494, which is Python 3.8; venv38 matches).

## Root cause of the missing `[RENEW]` lines (first connect): qasync re-entrancy zombie

`ui/tabs/tab_settings.py::_connect_and_load` builds a `QProgressDialog` with
`setMinimumDuration(0)` and calls `progress.setValue(40)` immediately after
`await self.engine.connect()` returns. Qt's `QProgressDialog::setValue()` calls
`QCoreApplication::processEvents()` (verified: a 0 ms QTimer queued before `setValue(40)` fires
*inside* the call). Under qasync, asyncio task steps are driven by Qt timers, so this nested
event pumping dispatches the just-created `_renew_supervisor` / `_monitor_connection` task
steps re-entrantly while the `_connect_and_load` task is still on the stack. asyncio refuses
this:

```
RuntimeError: Cannot enter into task <Task ... OpcClientEngine._renew_supervisor()> while
another task <Task ... connect_flow()> is being executed.
```

The exception is raised inside `Task.__step`, so the step is lost and the Task stays **pending
forever**; the coroutine body never runs (hence no `[RENEW] ... started`, no `[HEARTBEAT]`).
qasync logs this to logger `qasync._QEventLoop`; the app attaches only `asyncua` to its file
handler and the root stderr bridge is dead in a windowed (`console=False`) build, so the error
is completely invisible in `logs/app.log`. This matches the field log: zero `[RENEW]` and zero
`[HEARTBEAT]` lines across a 3-hour session whose only connection was the 11:32 startup connect
(auto-reconnect was disabled).

Proof (all under tmp_frozen_probe/):
* `tools/frozen_qasync_harness.py` loads the *extracted frozen* `opc.client_engine` code object
  under venv38 (qasync 0.28.0 / PyQt5 5.15.11 / asyncua 1.1.5) and drives `connect()` from a
  QTimer with the same QProgressDialog: no `[RENEW]`, `_renew_task.done()==False` and the
  `Cannot enter into task ...` RuntimeError is printed. Removing the dialog
  (`tools/frozen_qasync_nodialog.py`) makes `[RENEW] started` + `exited` appear normally.
* `tools/source_qasync_harness.py --dialog` with the current source snapshot (md5 68304b5,
  2026-10-23 14:54) + py3.12/asyncua 1.1.8 reproduces the zombie: after 32 s with a 20 s
  lifetime `renewal_count==0`, `_renew_task` still pending, no Renew OSC on the server.
  Same harness without `--dialog`: `renewal_count==3`, `OpenSecureChannel type=1` every 10 s
  (50 % of lifetime) — the supervisor logic itself works when it actually runs.

## Consequence for the in-progress source fix

`_ensure_link_tasks` (added by the Lead) revives a supervisor only when
`task is None or task.done()`. A zombie task is *pending*, not done, so it will **not** be
revived and the field bug would survive the rebuild.
Recommended belt-and-braces:
1. After creating `_renew_task`/`_monitor_task` in `_connect_once`, `await asyncio.sleep(0)` so
   both take their first step before control returns to UI code that pumps Qt events.
2. Track a `started` timestamp set as the first statement of each coroutine; in
   `_ensure_link_tasks`, cancel+recreate a task that is still pending without ever having
   started (e.g. >2 s after creation).
3. Do not call `QProgressDialog.setValue()` between task creation and the first await; or
   replace the progress dialog with a UI update that does not call `processEvents()`.
4. Optionally keep asyncua's internal `_renew_channel_loop` running until the custom supervisor
   has completed one renewal, as a safety net (shipped build cancels it, so there is no fallback
   at all).

## Secondary defects observed

* Health monitor (`_monitor_connection`) is zombified by the same mechanism on the first
  connect, so no `[HEARTBEAT]`, no `_evaluate_health`, no early warning.
* Zombie tasks cannot even be cancelled by `disconnect()`: the harness prints
  `Task was destroyed but it is pending!` / `coroutine ... was never awaited` at shutdown.
* `ui/tabs/tab_settings.py::_connect_and_load` is the trigger site; reconnect path
  (`_reconnect_supervisor`) has no dialog and therefore behaves differently (started+exited).

## Evidence files (scratch, untracked)

* `tmp_frozen_probe/app/` — copy of original exe + isolated config/logs (run1, auto_reconnect=true).
* `tmp_frozen_probe/app2/` — same, auto_reconnect=false (field-log analogue: 0 `[RENEW]`).
* `tmp_frozen_probe/app/logs/app.log`, `tmp_frozen_probe/app2/logs/app.log` — exe logs.
* `tmp_frozen_probe/out/server_run1.log` — server OSC/expiry trace (type=0 Issue vs type=1 Renew).
* `tmp_frozen_probe/out/exe_PYZ-00.pyz`, `dis_*.txt` — extracted bytecode and disassembly.
* `tmp_frozen_probe/tools/*.py` — test server, harnesses, extraction/comparison scripts.

> 注：以上临时证据目录（约 197 MB）在结论固化、修复验证完成后已清理；关键日志摘录与
> 结论保留在本文件正文中。

## Post-fix verification (Lead, 2026-10-23)

修复已按上文建议全部落地（详见 `CHANNEL_RENEWAL_ANALYSIS.md`）：

1. `_connect_once()` 启动续约/看门狗后主动 `await asyncio.sleep(0)` 两轮，让协程先跑到
   第一行再返回 UI，从源头躲开 `QProgressDialog.setValue()` 的 processEvents 重入窗口；
2. `_ensure_link_tasks()` 改为 async，并新增"创建时间 vs 真正开始执行时间"的僵尸任务识别
   （pending 超过 2s 且从未设置 started 时间戳 → cancel + 重建），补拉后同样让出一轮事件循环；
3. loading/connected 统一用 `_link_usable()` 判定；续约时刻由上次续约时间推算；
4. 重打包 `dist/LightingControl_v1.4.0.exe`（md5 由 a6d2a19d… 变为 19bb27c8…；随后修复
   冷启动自动重连的凭据注入问题，再次重打包为 `bc647c78221deb081cb00faefda06f7b`）。

验证结果：

* 修复后的源码 + qasync + QProgressDialog 复现脚本（6 s 通道寿命、24 s 观察）：
  `renewal_count = 7`、0 次断线（修复前同场景为 0）；
* 修复后的**冻结 exe**（headless，10 s 通道寿命网关，45 s 观察）：
  `[RENEW] SecureChannel renewal supervisor started` ×1，
  `Secure channel renewed successfully` ×9，无 `Connection lost`、无 `[Errno 22]`、全程 connected；
* `tools/smoke_frozen_build.py` 对重打包产物全部通过。
