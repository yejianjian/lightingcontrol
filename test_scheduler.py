import asyncio
from datetime import datetime

class DummyLogger:
    def info(self, msg): print(f"[INFO] {msg}")
    def error(self, msg): print(f"[ERROR] {msg}")
    def warning(self, msg): print(f"[WARNING] {msg}")

global_logger = DummyLogger()

class DummyDM:
    class DummyPM:
        def get_schedules(self):
            return [{"enabled": True, "time": datetime.now().strftime("%H:%M"), "group": "test", "action": True}]
        def get_groups(self):
            return {"test": ["1", "2"]}
    pm = DummyPM()

class DummyEngine:
    connected = True
    async def write_node_value(self, nid, action):
        print(f"Writing to {nid}: {action}")

class GroupScheduler:
    def __init__(self, dm, engine):
        self.dm = dm
        self.engine = engine
        self.running = False
        self._task = None

    def start(self):
        if not self.running:
            self.running = True
            loop = asyncio.get_event_loop()
            self._task = loop.create_task(self._scheduler_loop())
            print("Scheduler started")

    async def _scheduler_loop(self):
        print("Loop started")
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
                    
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}")
                
            await asyncio.sleep(2)  # 每2秒探测一次

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
            task = asyncio.ensure_future(self.engine.write_node_value(nid, action))
            task.add_done_callback(_task_err_callback)

async def main():
    gs = GroupScheduler(DummyDM(), DummyEngine())
    gs.start()
    await asyncio.sleep(5)

asyncio.run(main())
