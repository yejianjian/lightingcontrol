# ======================================================================
# ⚠️ 已弃用 (DEPRECATED)
# 此文件为早期独立版本的 GroupScheduler 测试副本，与实际代码
# (core/group_scheduler.py) 严重不同步。请勿基于此文件开发或测试。
# 建议后续使用 pytest + unittest.mock 编写规范化测试。
# ======================================================================

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
            # 模拟一个在今天执行的任务 (6=Sunday, Assuming today is 2026-03-15 Sun)
            return [{"enabled": True, "time": datetime.now().strftime("%H:%M"), "group": "test", "action": True, "weekdays": [6]}]
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
                current_weekday = now.weekday()
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
                            weekdays = sched.get("weekdays")
                            if weekdays is not None and current_weekday not in weekdays:
                                print(f"Schedule matched time but weekday {current_weekday} not in {weekdays}, skipping.")
                                continue

                            group_name = sched.get("group")
                            action = sched.get("action")
                            
                            global_logger.info(f"Scheduler triggered! Executing group '{group_name}' action: {'ON' if action else 'OFF'}")
                            await self._execute_group_action(group_name, action)
                            
                    last_triggered_minute = current_minute
                    
            except Exception as e:
                global_logger.error(f"Scheduler error: {e}")
                
            await asyncio.sleep(2)

    async def _execute_group_action(self, group_name, action):
        if not self.engine.connected:
            global_logger.warning("OPC engine disconnected.")
            return
            
        members = self.dm.pm.get_groups().get(group_name, [])
        for nid in members:
            task = asyncio.ensure_future(self.engine.write_node_value(nid, action))

async def main():
    gs = GroupScheduler(DummyDM(), DummyEngine())
    gs.start()
    await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
