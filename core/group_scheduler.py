import asyncio
import time
from collections import deque
from datetime import datetime, timedelta
from utils.logger import global_logger
import traceback

MAX_PENDING_ACTIONS = 100  # pending_actions 队列上限，防止内存泄漏
# 事件循环被长阻塞/系统休眠跨过触发分钟时，最多向前补跑多少分钟（防止跨天/时钟跳变导致刷屏）
MAX_CATCHUP_MINUTES = 10
# 已触发去重键保留数量（防止补跑/重复通知把同一计划在同一分钟跑多次）
FIRED_KEYS_LIMIT = 400
# 调度协程创建后超过该秒数仍未执行到第一行，视为 qasync 重入造成的"僵尸任务"
SCHEDULER_ZOMBIE_GRACE = 2.0

class GroupScheduler:
    """
    后台常驻调度器，基于 asyncio 协程，
    读取 PersistenceManager 里的时间表，到达触发时间后向 OpcClientEngine 下发控制任务。
    """
    def __init__(self, data_manager, opc_engine):
        self.dm = data_manager
        self.engine = opc_engine
        self.running = False
        self._task = None
        self.pending_actions = [] # 元素结构: {"group_id": uuid, "action": bool, "timestamp": datetime}
        # 锁惰性创建：Python 3.8 的 asyncio.Lock() 在"当前线程没有事件循环"时会抛
        # RuntimeError，而调度器是在事件循环启动前就构造的（UI 初始化阶段）。
        self._pending_lock = None
        self._batch_semaphore = None  # 控制批量写入并发
        self._background_tasks = set()  # 强引用保活池，防止执行中的任务被 Python GC 意外回收

        # ---- 调度循环自愈 / 触发可靠性（修复"定时任务不执行"）----
        # 后台协程可能因 qasync 重入变成"僵尸 pending"，或静默退出后无人重启；
        # running=True 但 _task 已死时，ensure_running() 负责重新拉起。
        self._task_created_at = 0.0
        self._task_started_at = 0.0
        self._launch_handle = None
        self._user_stopped = False
        # 上次已完整检查过的分钟（datetime，精确到分钟）
        self._last_checked_minute = None
        # 计划被增删改时置位：下一 tick 重新检查"当前分钟"，避免刚添加的计划
        # 因为本分钟已经检查过而永远不触发。
        self._recheck_at = None
        # 同一 (计划 id/分组, 日期, 时分) 只触发一次：补跑/重检/时钟回拨都不会重复执行
        self._fired_keys = set()
        self._fired_order = deque()

    def _ensure_pending_lock(self):
        """惰性创建并校验 pending_actions 锁（见 __init__ 中关于 Python 3.8 的说明）。"""
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        if self._pending_lock is None:
            self._pending_lock = asyncio.Lock()
        bound = getattr(self._pending_lock, "_loop", None)
        if running is not None and bound is not None and bound is not running:
            global_logger.warning("pending lock was bound to another event loop; recreating it.")
            self._pending_lock = asyncio.Lock()
        return self._pending_lock

    def _spawn_background_task(self, coro):
        """创建后台任务并在保活池中维持强引用，任务完成后自动清理"""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def start(self):
        """启动调度守护协程（幂等）。

        修复"定时任务不执行"的第一条：running 标志与后台任务必须始终一致。
        以前 running=True 但 _task 已静默退出/从未创建时，后续 start() 会直接
        return，调度器就永久死掉且没有任何日志。现在只要任务不健康就重新拉起。
        """
        self._user_stopped = False
        if self.running:
            task = self._task
            if task is not None and not task.done():
                global_logger.debug("Group Scheduler is already running, ignoring duplicate start().")
                return
            global_logger.warning(
                "Group Scheduler flag was running but its task was dead (%s); re-arming it.",
                "never created" if task is None else "already finished",
            )
        self.running = True
        # 延后 500ms 推入，确保 qasync.run_forever 已经接管（首次启动路径）
        self._arm_launch(delay=0.5)
        global_logger.info("Group Scheduler daemon start scheduled.")

    def _arm_launch(self, delay=0.0):
        """把调度协程的创建延后到事件循环里执行（并保存 handle 以便 stop 取消）。"""
        if self._launch_handle is not None:
            try:
                self._launch_handle.cancel()
            except Exception:
                pass
            self._launch_handle = None
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                global_logger.warning(
                    "Group Scheduler cannot start: no event loop available ({}).".format(e))
                return
        self._launch_handle = loop.call_later(delay, self._launch_task)

    def _launch_task(self):
        self._launch_handle = None
        if not self.running:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            try:
                loop = asyncio.get_event_loop()
            except RuntimeError as e:
                global_logger.warning(
                    "Group Scheduler launch skipped: no running loop ({}).".format(e))
                return
        self._task_created_at = time.monotonic()
        self._task_started_at = 0.0
        self._task = loop.create_task(self._scheduler_loop())
        global_logger.info("Group Scheduler core routine spawned into event loop.")

    def ensure_running(self, start_if_connected=True):
        """调度看门狗（同步，供 UI 刷新定时器调用）。

        覆盖三类"定时任务不执行"：
        1. 调度协程静默退出/被意外取消：running 仍是 True，但任务已 done；
        2. qasync 重入导致协程创建后一步都没跑就永久 pending（僵尸任务）；
        3. 连接已恢复但调度器从未启动（例如连接回调被跳过）。

        返回是否触发了重启。
        """
        if self._user_stopped:
            return False

        if not self.running:
            if not start_if_connected:
                return False
            if not bool(getattr(self.engine, "connected", False)):
                return False
            # 连接已恢复但调度器从未启动：直接拉起（会先补检查当前分钟）
            self.running = True
            self._arm_launch(delay=0.0)
            global_logger.warning(
                "Group Scheduler was not running while OPC link is connected; starting it now.")
            return True

        now = time.monotonic()
        # 已经有一次"重新拉起"在排队（call_later 尚未执行）：不要重复排队
        if self._launch_handle is not None:
            return False
        task = self._task
        if task is not None and not task.done():
            # 僵尸任务：创建后迟迟没有执行到第一行（_task_started_at 一直为 0）
            if (self._task_started_at <= 0.0 and self._task_created_at > 0.0
                    and (now - self._task_created_at) >= SCHEDULER_ZOMBIE_GRACE):
                global_logger.error(
                    "Group Scheduler task has been pending for {:.1f}s without ever starting "
                    "(qasync re-entrancy zombie); replacing it.".format(
                        now - self._task_created_at))
                task.cancel()
                self._task_created_at = now
                self._task_started_at = 0.0
                self._arm_launch(delay=0.0)
                return True
            return False

        if task is not None:
            detail = "cancelled" if task.cancelled() else "finished"
            global_logger.warning(
                "Group Scheduler loop task is not running ({}); re-arming it.".format(detail))
        else:
            global_logger.warning("Group Scheduler loop task is missing; re-arming it.")
        self._task_created_at = now
        self._task_started_at = 0.0
        self._arm_launch(delay=0.0)
        return True

    def notify_schedules_changed(self):
        """计划被增删改后调用：下一 tick 重新检查"计划变更时所在的分钟"。

        现场症状：在 08:00:10 添加一条 08:00 的计划，而调度器已在 08:00:00
        检查过本分钟，结果这条计划当天永远不会执行。重检配合去重键不会重复执行。
        """
        self._recheck_at = datetime.now()

    def stop(self):
        self._user_stopped = True
        self.running = False
        if self._launch_handle is not None:
            try:
                self._launch_handle.cancel()
            except Exception:
                pass
            self._launch_handle = None
        if self._task:
            self._task.cancel()
        for t in list(self._background_tasks):
            t.cancel()
        self._background_tasks.clear()
        global_logger.info("Group Scheduler daemon stopped.")

    async def _scheduler_loop(self):
        global_logger.info("Group Scheduler background tick loop actually started running.")
        # 第一行就登记"真正开始执行"的时间戳：ensure_running() 靠它识别僵尸任务
        self._task_started_at = time.monotonic()
        # 循环（重）启动后重检当前分钟：任务若在本分钟内静默死亡过，重启后也能
        # 补上本分钟尚未执行的计划；_fired_keys 保证已执行过的不会重复执行。
        self._recheck_at = datetime.now()
        # 延迟初始化 Semaphore，确保当前事件循环已启动
        if self._batch_semaphore is None:
            self._batch_semaphore = asyncio.Semaphore(5)  # 限制最多5个并发批量写入
        while self.running:
            try:
                now = datetime.now()
                # 1) 到点触发（支持补跑错过的分钟、计划变更后重检本分钟；去重防重复执行）
                self._process_due_minutes(now)
                # 2) 断线期间入队的补偿任务：连接恢复后派发
                await self._dispatch_pending_actions(now)
            except asyncio.CancelledError:
                break
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}\n{traceback.format_exc()}")

            await asyncio.sleep(2)  # 每2秒探测一次，防止错过

    # ----------------------------------------------------------------------------------
    # 触发判定（分钟级，带补跑与去重）
    # ----------------------------------------------------------------------------------

    def _minutes_to_check(self, now):
        """计算本次 tick 需要检查的分钟（升序），并推进检查进度。

        修复前只在"分钟发生变化"时检查当前分钟，导致两类现场故障：
        * 事件循环被长任务/系统休眠阻塞跨过触发分钟 -> 该计划当天永久漏执行；
        * 计划在本分钟已经被检查过之后才添加 -> 本分钟不会触发。
        现在按 (上次检查, 当前] 逐分钟补跑，并对"计划变更时所在分钟"额外重检；
        同一计划同一分钟由 _fired_keys 去重，绝不重复执行。
        """
        current = now.replace(second=0, microsecond=0)
        minutes = []
        last = self._last_checked_minute
        if last is None:
            minutes.append(current)
        elif current > last:
            span = int((current - last).total_seconds() // 60)
            if span <= MAX_CATCHUP_MINUTES:
                minutes.extend(last + timedelta(minutes=i) for i in range(1, span + 1))
            else:
                global_logger.warning(
                    "Group Scheduler missed {} minute(s) (event loop stall / clock jump); "
                    "only the most recent {} minute(s) will be caught up.".format(
                        span, MAX_CATCHUP_MINUTES))
                start = current - timedelta(minutes=MAX_CATCHUP_MINUTES - 1)
                minutes.extend(start + timedelta(minutes=i) for i in range(MAX_CATCHUP_MINUTES))
        elif current < last:
            back = int((last - current).total_seconds() // 60)
            if back <= 5:
                # 小范围时钟回拨：不补跑（等时钟追上来），但仍要处理"计划变更重检"
                minutes = []
            else:
                global_logger.warning(
                    "System clock moved backwards by {} minute(s); re-aligning scheduler.".format(back))
                minutes.append(current)

        recheck_at = self._recheck_at
        if recheck_at is not None:
            self._recheck_at = None
            recheck_minute = recheck_at.replace(second=0, microsecond=0)
            if ((current - recheck_minute).total_seconds() <= MAX_CATCHUP_MINUTES * 60
                    and recheck_minute not in minutes):
                minutes.append(recheck_minute)
        minutes.sort()

        # 正常推进或大幅调整后对齐检查进度；小范围回拨分支已提前 return
        if (self._last_checked_minute is None or current > self._last_checked_minute
                or (self._last_checked_minute - current).total_seconds() > 300):
            self._last_checked_minute = current
        return minutes

    def _process_due_minutes(self, now):
        for minute in self._minutes_to_check(now):
            self._fire_for_minute(minute)

    def _fire_for_minute(self, minute):
        """触发指定分钟到期的计划（同一计划同一分钟只执行一次）。"""
        schedules = self.dm.pm.get_schedules()
        if not schedules:
            return
        time_str = minute.strftime("%H:%M")
        date_key = minute.strftime("%Y-%m-%d")
        weekday = minute.weekday()  # 0=周一, 6=周日
        for sched in schedules:
            if not sched.get("enabled", True):
                continue
            if str(sched.get("time")) != time_str:
                continue
            weekdays = sched.get("weekdays")  # 0-6 列表，None 表示每天
            if weekdays is not None and weekday not in weekdays:
                global_logger.debug(
                    f"Schedule '{sched.get('id')}' time matched but weekday {weekday} "
                    f"not in {weekdays}, skipping.")
                continue

            key = (sched.get("id") or str(sched.get("group_id")), date_key, time_str)
            if key in self._fired_keys:
                continue
            self._mark_fired(key)

            group_id = sched.get("group_id")
            action = sched.get("action")
            group_obj = self.dm.pm.get_group_by_id(group_id)
            if not group_obj:
                global_logger.warning(
                    f"Scheduler triggered for non-existent group ID: {group_id}")
                continue
            group_name = group_obj.get("name")
            global_logger.info(
                f"Scheduler triggered! Executing group '{group_name}' action: "
                f"{'ON' if action else 'OFF'}")
            # 不阻塞主循环，开启任务执行并保留强引用防 GC
            self._spawn_background_task(self._execute_group_action(group_id, action))

    def _mark_fired(self, key):
        self._fired_keys.add(key)
        self._fired_order.append(key)
        while len(self._fired_order) > FIRED_KEYS_LIMIT:
            old = self._fired_order.popleft()
            self._fired_keys.discard(old)

    async def _dispatch_pending_actions(self, now):
        """连接恢复正常后，派发断线期间入队的补偿任务。"""
        if not self.engine.connected or not self.pending_actions:
            return
        expired_pkgs = []
        dispatch_pkgs = []
        async with self._ensure_pending_lock():
            current_actions = list(self.pending_actions)
            for action_pkg in current_actions:
                if action_pkg.get("processing"):
                    continue  # 防止重复派发

                ts_str = action_pkg["timestamp"].strftime("%H:%M:%S")
                g_id = action_pkg["group_id"]
                g_obj = self.dm.pm.get_group_by_id(g_id)
                g_display = g_obj.get("name") if g_obj else g_id

                # 检查是否过期 (超时 10 分钟作废)
                if (now - action_pkg["timestamp"]).total_seconds() > 600:
                    global_logger.warning(
                        f"Recovery: Discarded expired (10min+) pending task for group "
                        f"'{g_display}' (originally at {ts_str})")
                    expired_pkgs.append(action_pkg)
                else:
                    global_logger.info(
                        f"Recovery: Async dispatch for missed task for group '{g_display}' "
                        f"(originally at {ts_str})")
                    action_pkg["processing"] = True
                    dispatch_pkgs.append((action_pkg, g_display))

            # 立即移除已过期任务
            for pkg in expired_pkgs:
                try:
                    self.pending_actions.remove(pkg)
                except ValueError:
                    pass

        # 在锁外派发异步任务
        for action_pkg, g_display in dispatch_pkgs:
            # BUG-02 修复：使用实例方法代替循环内嵌套定义的局部异步函数
            self._spawn_background_task(self._recovery_execute(action_pkg, g_display))

    async def _recovery_execute(self, pkg, name_disp):
        """BUG-02 修复：将原内嵌循环的 _do_recover 提升为实例方法，避免闭包风险并减少每次循环重建函数对象的开销。"""
        try:
            success = await self._execute_group_action(pkg["group_id"], pkg["action"], is_recovery=True)
            if success:
                async with self._ensure_pending_lock():
                    if pkg in self.pending_actions:
                        self.pending_actions.remove(pkg)
            else:
                # 恢复失败或部分失败，解开 processing 标记待下次重试
                pkg["processing"] = False
        except Exception as e:
            pkg["processing"] = False
            global_logger.error(f"Recovery execution failed for group '{name_disp}': {e}")

    async def _execute_group_action(self, group_id, action, is_recovery=False):
        """Risk-3 修复：加入总超时保护，防止大组写入归山网络时占用 Semaphore 过久阻塞其他组任务。"""
        group_obj = self.dm.pm.get_group_by_id(group_id)
        group_name = group_obj.get("name") if group_obj else "Unknown"

        if not self.engine.connected:
            if not is_recovery:
                # 入队前去重：同组+同操作不重复入队
                async with self._ensure_pending_lock():
                    already_queued = any(
                        p.get("group_id") == group_id and p["action"] == action
                        for p in self.pending_actions
                    )
                    if not already_queued:
                        # 严格执行 MAX_PENDING_ACTIONS 上限，防止断线期间内存无限膨胀
                        if len(self.pending_actions) >= MAX_PENDING_ACTIONS:
                            discarded = self.pending_actions.pop(0)
                            global_logger.warning(
                                f"pending_actions reached MAX_PENDING_ACTIONS ({MAX_PENDING_ACTIONS}). "
                                f"Discarded oldest action for group ID {discarded.get('group_id')}."
                            )
                        global_logger.warning(f"Scheduler active but OPC engine is disconnected. Task for group '{group_name}' queued for recovery.")
                        self.pending_actions.append({"group_id": group_id, "action": action, "timestamp": datetime.now()})
                    else:
                        global_logger.debug(f"Duplicate pending task for '{group_name}' (ID: {group_id}) skipped.")
            return False

        # 递归获取成员（包含所有子分组的控制点）
        members = self.dm.pm.get_group_nodes_recursive(group_id)
        if not members:
             global_logger.warning(f"Scheduler active but group '{group_name}' (ID: {group_id}) is empty or has no nodes recursive.")
             return False

        # 方案B：使用 WriteList 批量写入，限制并发数
        total_success = 0
        total_fail = 0
        # Risk-3 修复：给整个组写入加总超时（≤90s），防止超大组+弱网场景下
        # 占用 Semaphore 超过 90s 阻塞后续组的定时任务
        try:
            total_success, total_fail = await asyncio.wait_for(
                self._do_batch_write_with_result(group_id, action, group_name, list(members)),
                timeout=90.0
            )
        except asyncio.TimeoutError:
            global_logger.error(f"Group action '{group_name}' execution timed out after 90s, treating as full failure.")
            total_fail = len(members)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            global_logger.error(f"Group action '{group_name}' unexpected error: {e}")
            total_fail = len(members)

        # 如果写入过程中连接断开，将此任务入队等待重连后恢复
        if not self.engine.connected and not is_recovery and total_fail > 0:
            async with self._ensure_pending_lock():
                already_queued = any(
                    p.get("group_id") == group_id and p["action"] == action
                    for p in self.pending_actions
                )
                if not already_queued:
                    if len(self.pending_actions) >= MAX_PENDING_ACTIONS:
                        discarded = self.pending_actions.pop(0)
                        global_logger.warning(
                            f"pending_actions reached MAX_PENDING_ACTIONS ({MAX_PENDING_ACTIONS}). "
                            f"Discarded oldest action for group ID {discarded.get('group_id')}."
                        )
                    global_logger.warning(
                        f"Scheduled task for group '{group_name}' failed due to connection loss during execution. "
                    )
                    self.pending_actions.append({"group_id": group_id, "action": action, "timestamp": datetime.now()})
            return False

        global_logger.info(f"Group action '{group_name}' execution completed ({len(members)} nodes recursive).")
        return total_fail == 0

    async def _do_batch_write_with_result(self, group_id, action, group_name, member_list):
        """Risk-3 修复：封装实际批量写入逻辑，与 asyncio.wait_for 配合实现总超时。"""
        total_success = 0
        total_fail = 0
        async with self._batch_semaphore:
            batch_size = 100  # 增大批次，减少网络往返

            for i in range(0, len(member_list), batch_size):
                # 写入过程中如果连接断开（被 _on_write_failure 触发），立即停止后续批次
                if not self.engine.connected:
                    remaining = len(member_list) - i
                    global_logger.warning(f"Connection lost during batch write for '{group_name}', {remaining} nodes skipped.")
                    total_fail += remaining
                    break

                batch = member_list[i:i+batch_size]
                # 构建显示名列表
                display_names = [self.dm.get_alias_by_node_id(nid) for nid in batch]
                success_count, fail_count = await self.engine.write_values_batch(batch, action, display_names)
                total_success += success_count
                total_fail += fail_count
                if fail_count > 0:
                    global_logger.error(f"Batch write partially failed: {success_count} ok, {fail_count} failed")

        return total_success, total_fail
