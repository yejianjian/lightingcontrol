import sys
import os
import asyncio
import unittest
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.group_scheduler import GroupScheduler, MAX_PENDING_ACTIONS

class DummyDM:
    class DummyPM:
        def get_schedules(self):
            return []
        def get_groups(self):
            return []
        def get_group_by_id(self, gid):
            return {"name": f"Group_{gid}", "id": gid}
        def get_group_nodes_recursive(self, gid):
            return [f"node_{gid}"]
    pm = DummyPM()

class DummyEngine:
    connected = False  # 模拟断线状态以触发入队
    async def write_values_batch(self, nodes, val, names=None):
        return len(nodes), 0

class TestSchedulerResilience(unittest.TestCase):
    def test_background_task_retention_and_queue_limit(self):
        async def _run():
            scheduler = GroupScheduler(DummyDM(), DummyEngine())

            # 1. 测试任务保活池 (_spawn_background_task)
            async def sample_coro():
                await asyncio.sleep(0.05)
                return "done"

            task = scheduler._spawn_background_task(sample_coro())
            self.assertIn(task, scheduler._background_tasks)
            await task
            # 任务完成后自动从集合中清除
            await asyncio.sleep(0.01)
            self.assertNotIn(task, scheduler._background_tasks)

            # 2. 测试 MAX_PENDING_ACTIONS (100) 边界保护
            self.assertEqual(MAX_PENDING_ACTIONS, 100)

            # 连续尝试入队 120 个不同分组的操作
            for i in range(120):
                # 内部是 disconnected 状态，会触发入队
                await scheduler._execute_group_action(f"g_{i}", True)

            async with scheduler._ensure_pending_lock():
                # 队列必须被精确截断在 MAX_PENDING_ACTIONS (100)
                self.assertEqual(len(scheduler.pending_actions), 100)
                # 最旧的 20 个必须已经被弹出，队列头部应当是 g_20
                first_group_id = scheduler.pending_actions[0]["group_id"]
                self.assertEqual(first_group_id, "g_20")
                # 队列尾部应当是 g_119
                last_group_id = scheduler.pending_actions[-1]["group_id"]
                self.assertEqual(last_group_id, "g_119")

            # 3. 停止清理
            scheduler.stop()
            self.assertEqual(len(scheduler._background_tasks), 0)

        asyncio.run(_run())

if __name__ == "__main__":
    unittest.main()
