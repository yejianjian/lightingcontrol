import asyncio
from datetime import datetime
from utils.logger import global_logger
import traceback

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
        self.pending_actions = [] # 元素结构: {"group": name, "action": bool, "timestamp": datetime}
        # 并发控制：限制同时进行的写入任务数量，防止瞬间冲击服务器和网络
        self._semaphore = asyncio.Semaphore(30) 

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
        last_triggered_minute = -1
        while self.running:
            try:
                now = datetime.now()
                current_minute = now.minute
                
                # 每分钟只触发一次判断
                if current_minute != last_triggered_minute:
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

                            group_name = sched.get("group")
                            action = sched.get("action") # True / False
                            
                            global_logger.info(f"Scheduler triggered! Executing group '{group_name}' action: {'ON' if action else 'OFF'}")
                            # 不阻塞主循环，开启任务执行
                            asyncio.create_task(self._execute_group_action(group_name, action))
                            
                    last_triggered_minute = current_minute
                
                # 若连接正常且有未决任务，则执行补偿并清理过期任务
                if self.engine.connected and self.pending_actions:
                    actions_to_run = self.pending_actions.copy()
                    self.pending_actions.clear()
                    
                    for action_pkg in actions_to_run:
                        ts_str = action_pkg["timestamp"].strftime("%H:%M:%S")
                        # 检查是否过期 (超时 10 分钟作废)
                        if (now - action_pkg["timestamp"]).total_seconds() <= 600:
                            global_logger.info(f"Recovery: Executing missed task for group '{action_pkg['group']}' (originally at {ts_str})")
                            asyncio.create_task(self._execute_group_action(action_pkg["group"], action_pkg["action"], is_recovery=True))
                        else:
                            global_logger.warning(f"Recovery: Discarded expired (10min+) pending task for group '{action_pkg['group']}' (originally at {ts_str})")
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}\n{traceback.format_exc()}")
                
            await asyncio.sleep(2)  # 每2秒探测一次，防止错过

    async def _execute_group_action(self, group_name, action, is_recovery=False):
        if not self.engine.connected:
            if not is_recovery:
                # 入队前去重：同组+同操作不重复入队
                already_queued = any(
                    p["group"] == group_name and p["action"] == action
                    for p in self.pending_actions
                )
                if not already_queued:
                    global_logger.warning(f"Scheduler active but OPC engine is disconnected. Task for group '{group_name}' queued for recovery.")
                    self.pending_actions.append({"group": group_name, "action": action, "timestamp": datetime.now()})
                else:
                    global_logger.debug(f"Duplicate pending task for '{group_name}' skipped.")
            return
            
        members = self.dm.pm.get_groups().get(group_name, [])
        if not members:
             global_logger.warning(f"Scheduler active but group '{group_name}' is empty or does not exist.")
             return

        async def _limited_write(nid, val):
            async with self._semaphore:
                try:
                    success = await self.engine.write_node_value(nid, val)
                    if not success:
                        global_logger.error(f"Scheduled write failed for node {nid}")
                except Exception as e:
                    global_logger.error(f"Exception during scheduled write for {nid}: {e}")

        # 并发执行组内所有节点的写入，但受 semaphore 限制
        tasks = [asyncio.create_task(_limited_write(nid, action)) for nid in members]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            global_logger.info(f"Group action '{group_name}' execution completed ({len(members)} nodes).")
