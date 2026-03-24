import asyncio
from datetime import datetime
from utils.logger import global_logger
import traceback

MAX_PENDING_ACTIONS = 100  # pending_actions 队列上限，防止内存泄漏

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
        self._pending_lock = asyncio.Lock()  # 保护 pending_actions 列表
        self._batch_semaphore = None  # 控制批量写入并发

    def start(self):
        if not self.running:
            self.running = True
            
            # 使用更安全的 ensure_future，或者避免在事件循环尚未 run_forever 之前挂起
            # 为防 QEventLoop (qasync) 丢弃初期任务，我们延时将调度器推入准备好的消息队列中
            def _launch_task():
                try:
                    loop = asyncio.get_running_loop()
                except RuntimeError:
                    loop = asyncio.get_event_loop()
                self._task = loop.create_task(self._scheduler_loop())
                global_logger.info("Group Scheduler core routine spawned into event loop.")
            
            # 延后 500 毫秒推入，确保 qasync.run_forever 已经接管
            loop = asyncio.get_event_loop()
            loop.call_later(0.5, _launch_task)
            global_logger.info("Group Scheduler daemon start scheduled.")

    def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            global_logger.info("Group Scheduler daemon stopped.")

    async def _scheduler_loop(self):
        global_logger.info("Group Scheduler background tick loop actually started running.")
        # 延迟初始化 Semaphore，确保当前事件循环已启动
        if self._batch_semaphore is None:
            self._batch_semaphore = asyncio.Semaphore(5)  # 限制最多5个并发批量写入
        # 使用 (hour, minute) 元组避免跨小时重复问题
        last_triggered_time = (-1, -1)
        while self.running:
            try:
                now = datetime.now()
                current_time_key = (now.hour, now.minute)
                
                # 每分钟只触发一次判断
                if current_time_key != last_triggered_time:
                    schedules = self.dm.pm.get_schedules()
                    current_time_str = now.strftime("%H:%M")
                    
                    for sched in schedules:
                        if not sched.get("enabled", True):
                            continue
                            
                        trigger_time = sched.get("time") 
                        if trigger_time == current_time_str:
                            # 星期过滤逻辑
                            weekdays = sched.get("weekdays") # 0-6 列表，如果为 None 则默认每天
                            current_weekday = now.weekday() # 0=周一, 6=周日
                            
                            if weekdays is not None and current_weekday not in weekdays:
                                global_logger.debug(f"Schedule '{sched.get('id')}' time matched but weekday {current_weekday} not in {weekdays}, skipping.")
                                continue

                            group_id = sched.get("group_id") # 关联 UUID
                            action = sched.get("action") # True / False
                            
                            group_obj = self.dm.pm.get_group_by_id(group_id)
                            if not group_obj:
                                global_logger.warning(f"Scheduler triggered for non-existent group ID: {group_id}")
                                continue
                            
                            group_name = group_obj.get("name")
                            global_logger.info(f"Scheduler triggered! Executing group '{group_name}' action: {'ON' if action else 'OFF'}")
                            # 不阻塞主循环，开启任务执行
                            asyncio.create_task(self._execute_group_action(group_id, action))
                            
                    last_triggered_time = current_time_key
                
                # 若连接正常且有未决任务，则分配后台异步补偿任务
                if self.engine.connected and self.pending_actions:
                    expired_pkgs = []
                    dispatch_pkgs = []
                    async with self._pending_lock:
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
                                global_logger.warning(f"Recovery: Discarded expired (10min+) pending task for group '{g_display}' (originally at {ts_str})")
                                expired_pkgs.append(action_pkg)
                            else:
                                global_logger.info(f"Recovery: Async dispatch for missed task for group '{g_display}' (originally at {ts_str})")
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
                        async def _do_recover(pkg, name_disp):
                            try:
                                success = await self._execute_group_action(pkg["group_id"], pkg["action"], is_recovery=True)
                                if success:
                                    async with self._pending_lock:
                                        if pkg in self.pending_actions:
                                            self.pending_actions.remove(pkg)
                                else:
                                    # 恢复失败或部分失败，解开 processing 标记待下次重试
                                    pkg["processing"] = False
                            except Exception as e:
                                pkg["processing"] = False
                                global_logger.error(f"Recovery execution failed for group '{name_disp}': {e}")

                        asyncio.create_task(_do_recover(action_pkg, g_display))
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}\n{traceback.format_exc()}")
                
            await asyncio.sleep(2)  # 每2秒探测一次，防止错过

    async def _execute_group_action(self, group_id, action, is_recovery=False):
        group_obj = self.dm.pm.get_group_by_id(group_id)
        group_name = group_obj.get("name") if group_obj else "Unknown"

        if not self.engine.connected:
            if not is_recovery:
                # 入队前去重：同组+同操作不重复入队
                async with self._pending_lock:
                    already_queued = any(
                        p.get("group_id") == group_id and p["action"] == action
                        for p in self.pending_actions
                    )
                    if not already_queued:
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
        async with self._batch_semaphore:
            member_list = list(members)
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

        # 如果写入过程中连接断开，将此任务入队等待重连后恢复
        if not self.engine.connected and not is_recovery and total_fail > 0:
            async with self._pending_lock:
                already_queued = any(
                    p.get("group_id") == group_id and p["action"] == action
                    for p in self.pending_actions
                )
                if not already_queued:
                    global_logger.warning(
                        f"Scheduled task for group '{group_name}' failed due to connection loss during execution. "
                    )
                    self.pending_actions.append({"group_id": group_id, "action": action, "timestamp": datetime.now()})
            return False

        global_logger.info(f"Group action '{group_name}' execution completed ({len(members)} nodes recursive).")
        return total_fail == 0
