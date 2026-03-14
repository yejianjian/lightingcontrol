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
                            group_name = sched.get("group")
                            action = sched.get("action") # True / False
                            
                            global_logger.info(f"Scheduler triggered! Executing group '{group_name}' action: {'ON' if action else 'OFF'}")
                            await self._execute_group_action(group_name, action)
                            
                    last_triggered_minute = current_minute
                
                # 若连接正常且有未决任务，则执行补偿并清理过期任务
                if self.engine.connected and self.pending_actions:
                    for action_pkg in self.pending_actions:
                        ts_str = action_pkg["timestamp"].strftime("%H:%M:%S")
                        # 检查是否过期 (超时 10 分钟作废)
                        if (now - action_pkg["timestamp"]).total_seconds() <= 600:
                            global_logger.info(f"Recovery: Executing missed task for group '{action_pkg['group']}' (originally at {ts_str})")
                            await self._execute_group_action(action_pkg["group"], action_pkg["action"], is_recovery=True)
                        else:
                            global_logger.warning(f"Recovery: Discarded expired (10min+) pending task for group '{action_pkg['group']}' (originally at {ts_str})")
                    # 只要连着网，本轮不管是成功补偿还是过期丢弃，都将其清空
                    self.pending_actions = []
                    
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

        def _task_err_callback(t):
            if not t.cancelled() and t.exception():
                global_logger.error(f"Scheduled write task failed: {t.exception()}")

        for nid in members:
            # 不需要等待每一个结果同步，将其推向后台即可实现并行执行
            task = asyncio.ensure_future(self.engine.write_node_value(nid, action))
            task.add_done_callback(_task_err_callback)
