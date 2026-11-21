"""GroupScheduler 定时触发回归测试。

现场问题"定时任务不执行"的三个根因（本文件逐条覆盖）：
1. 调度协程静默退出/僵尸 pending 后 running 仍为 True，调度器永久死掉；
2. 事件循环被长任务/系统休眠阻塞跨过触发分钟，该分钟的计划当天永久漏执行；
3. 计划在本分钟已被检查过之后才添加，本分钟不会触发（测试时最常遇到）。
"""
import asyncio
import os
import sys
import unittest
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.group_scheduler as gs_mod

REAL_SLEEP = asyncio.sleep


class FakeDateTime(object):
    """datetime.now() 的可控替身（返回真实 datetime，比较/运算语义不变）。"""

    current = datetime(2026, 11, 17, 7, 59, 55)

    @classmethod
    def now(cls):
        return cls.current


class FakeAsyncio(object):
    """转发真实 asyncio，只替换 sleep()：每次 sleep 推进 1 秒假时钟。"""

    CancelledError = asyncio.CancelledError
    TimeoutError = asyncio.TimeoutError
    jump_to = None
    jump_done = False

    def __getattr__(self, name):
        return getattr(asyncio, name)

    async def sleep(self, delay):
        if FakeAsyncio.jump_to is not None and not FakeAsyncio.jump_done:
            FakeAsyncio.jump_done = True
            FakeDateTime.current = FakeAsyncio.jump_to
        else:
            FakeDateTime.current += timedelta(seconds=1)
        await REAL_SLEEP(0)

    async def wait_for(self, coro, timeout):
        return await coro


class FakePM(object):
    def __init__(self):
        self.schedules = []
        self.groups = {}

    def get_schedules(self):
        return [dict(s) for s in self.schedules]

    def get_group_by_id(self, gid):
        return self.groups.get(gid)

    def get_group_nodes_recursive(self, gid):
        return set(self.groups.get(gid, {}).get("nodes", []))


class FakeDM(object):
    pm = FakePM()

    def get_alias_by_node_id(self, nid):
        return nid


class FakeEngine(object):
    connected = True

    def __init__(self):
        self.writes = []

    async def write_values_batch(self, nodes, val, names=None):
        self.writes.append((sorted(nodes), val))
        return len(nodes), 0


def _schedule(gid="g1", time_str="08:00", weekdays=None, enabled=True):
    return {"id": "s-" + gid, "group_id": gid, "time": time_str,
            "weekdays": weekdays, "action": True, "enabled": enabled}


class TestSchedulerFiring(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self._orig_datetime = gs_mod.datetime
        self._orig_asyncio = gs_mod.asyncio
        gs_mod.datetime = FakeDateTime
        gs_mod.asyncio = FakeAsyncio()
        FakeAsyncio.jump_to = None
        FakeAsyncio.jump_done = False
        self._tasks = []

    async def asyncTearDown(self):
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        gs_mod.datetime = self._orig_datetime
        gs_mod.asyncio = self._orig_asyncio

    def _make_scheduler(self, schedules, start_dt):
        FakeDateTime.current = start_dt
        dm, eng = FakeDM(), FakeEngine()
        dm.pm.schedules = list(schedules)
        dm.pm.groups = {}
        for s in dm.pm.schedules:
            dm.pm.groups[s["group_id"]] = {"name": "G", "id": s["group_id"],
                                           "nodes": ["n1"]}
        scheduler = gs_mod.GroupScheduler(dm, eng)
        scheduler.running = True
        task = asyncio.ensure_future(scheduler._scheduler_loop())
        self._tasks.append(task)
        return scheduler, eng, task

    async def _spin(self, predicate, max_loops=400):
        for _ in range(max_loops):
            await REAL_SLEEP(0)
            if predicate():
                return True
        return False

    async def test_fires_future_minute(self):
        scheduler, eng, _ = self._make_scheduler([_schedule()],
                                                 datetime(2026, 11, 17, 7, 59, 55))
        self.assertTrue(await self._spin(lambda: bool(eng.writes)))
        self.assertEqual(eng.writes, [(["n1"], True)])

    async def test_fires_when_started_mid_minute(self):
        scheduler, eng, _ = self._make_scheduler([_schedule()],
                                                 datetime(2026, 11, 17, 8, 0, 30))
        self.assertTrue(await self._spin(lambda: bool(eng.writes)))

    async def test_catches_up_minute_skipped_by_stall(self):
        # 事件循环阻塞：时钟直接从 08:00:58 跳到 08:02:05，08:01 的计划必须补跑
        FakeAsyncio.jump_to = datetime(2026, 11, 17, 8, 2, 5)
        scheduler, eng, _ = self._make_scheduler(
            [_schedule(time_str="08:01")], datetime(2026, 11, 17, 8, 0, 58))
        self.assertTrue(await self._spin(lambda: bool(eng.writes)),
                        "跨过 08:01 后没有补跑，计划会永久漏执行")

    async def test_notify_after_minute_checked_fires_new_schedule(self):
        # 08:00:10 已检查过本分钟，此时再添加 08:00 的计划 -> notify 后必须立即执行
        scheduler, eng, _ = self._make_scheduler([], datetime(2026, 11, 17, 8, 0, 10))
        for _ in range(5):
            await REAL_SLEEP(0)
        self.assertFalse(eng.writes)
        scheduler.dm.pm.schedules.append(_schedule())
        scheduler.dm.pm.groups["g1"] = {"name": "G", "id": "g1", "nodes": ["n1"]}
        scheduler.notify_schedules_changed()
        self.assertTrue(await self._spin(lambda: bool(eng.writes), max_loops=50))

    async def test_same_schedule_not_fired_twice_in_same_minute(self):
        scheduler, eng, _ = self._make_scheduler([_schedule()],
                                                 datetime(2026, 11, 17, 7, 59, 58))
        self.assertTrue(await self._spin(lambda: bool(eng.writes)))
        # 反复通知 + 同一分钟内继续运行，去重键必须阻止重复执行
        for _ in range(5):
            scheduler.notify_schedules_changed()
            for _ in range(10):
                await REAL_SLEEP(0)
        self.assertEqual(len(eng.writes), 1)

    async def test_weekday_filter(self):
        # 2026-11-17 是星期二（weekday=1）：只配星期一 -> 不执行；改成星期二 -> 执行
        scheduler, eng, _ = self._make_scheduler(
            [_schedule(weekdays=[0], time_str="08:00")],
            datetime(2026, 11, 17, 7, 59, 58))
        for _ in range(4):
            await REAL_SLEEP(0)
        self.assertEqual(eng.writes, [])
        # 仍停留在 08:00 分钟内：修改星期后 notify 应重检本分钟并执行
        scheduler.dm.pm.schedules[0]["weekdays"] = [1]
        scheduler.notify_schedules_changed()
        self.assertTrue(await self._spin(lambda: bool(eng.writes)))

    async def test_disabled_schedule_is_not_fired(self):
        scheduler, eng, _ = self._make_scheduler(
            [_schedule(enabled=False)], datetime(2026, 11, 17, 7, 59, 58))
        for _ in range(120):
            await REAL_SLEEP(0)
        self.assertEqual(eng.writes, [])

    async def test_watchdog_restarts_dead_task(self):
        scheduler = gs_mod.GroupScheduler(FakeDM(), FakeEngine())
        scheduler.running = True

        async def _finished():
            return None
        dead = asyncio.ensure_future(_finished())
        await dead
        scheduler._task = dead
        self.assertTrue(scheduler.ensure_running())
        # 让 call_later(0) 里的 _launch_task 真正执行
        for _ in range(5):
            await REAL_SLEEP(0)
        self.assertIsNotNone(scheduler._task)
        self.assertNotEqual(scheduler._task, dead)
        self.assertFalse(scheduler._task.done())
        scheduler.stop()

    async def test_watchdog_does_not_restart_after_stop(self):
        scheduler = gs_mod.GroupScheduler(FakeDM(), FakeEngine())
        scheduler.start()
        for _ in range(5):
            await REAL_SLEEP(0)
        scheduler.stop()
        self.assertFalse(scheduler.ensure_running())
        self.assertTrue(scheduler._user_stopped)

    async def test_watchdog_starts_scheduler_after_reconnect(self):
        # 连接已恢复但调度器从未启动（例如连接回调被跳过）：看门狗负责拉起
        engine = FakeEngine()
        scheduler = gs_mod.GroupScheduler(FakeDM(), engine)
        self.assertFalse(scheduler.running)
        self.assertTrue(scheduler.ensure_running())
        self.assertTrue(scheduler.running)
        for _ in range(5):
            await REAL_SLEEP(0)
        self.assertIsNotNone(scheduler._task)
        scheduler.stop()


if __name__ == "__main__":
    unittest.main()
