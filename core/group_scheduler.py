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

    def start(self):
        if not self.running:
            self.running = True
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.get_event_loop()
            self._task = loop.create_task(self._scheduler_loop())
            global_logger.info("Group Scheduler daemon started.")

    def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
            global_logger.info("Group Scheduler daemon stopped.")

    async def _scheduler_loop(self):
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
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}\n{traceback.format_exc()}")
                
            await asyncio.sleep(2)  # 每2秒探测一次，防止错过

    async def _execute_group_action(self, group_name, action):
        if not self.engine.connected:
            global_logger.warning("Scheduler active but OPC engine is disconnected. Discarding scheduled task.")
            return
            
        members = self.dm.pm.get_groups().get(group_name, [])
        if not members:
             global_logger.warning(f"Scheduler active but group '{group_name}' is empty or does not exist.")
             return

        def _task_err_callback(t):
            if t.exception():
                global_logger.error(f"Scheduled write task failed: {t.exception()}")

        for nid in members:
            # 不需要等待每一个结果同步，将其推向后台即可实现并行执行
            task = asyncio.ensure_future(self.engine.write_node_value(nid, action))
            task.add_done_callback(_task_err_callback)
