import unittest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import time

from opc.client_engine import OpcClientEngine


class TestSecureChannelRenewal(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.engine = OpcClientEngine(host="127.0.0.1", port=4840)
        self.engine.client = MagicMock()
        self.engine.client.secure_channel_timeout = 3600000  # 1 hour
        self.engine.client.open_secure_channel = AsyncMock()
        self.engine.client.uaclient._open_secure_channel_exchange = None

        # 模拟 asyncua 已经开始接管：世代号已建立、状态为已连接
        self.engine._generation = 1
        self.engine._set_state(OpcClientEngine.STATE_CONNECTED, "test setup")
        self.gen = self.engine._generation

        # mock server_state node
        mock_node = MagicMock()
        mock_node.read_value = AsyncMock(return_value=0)
        self.engine.client.nodes = MagicMock()
        self.engine.client.nodes.server_state = mock_node
        self.engine.client.get_node = MagicMock(return_value=mock_node)

    async def asyncTearDown(self):
        await self.engine.disconnect()

    async def test_renew_channel_with_retry_success(self):
        """测试正常重试并在第 1 次即成功续约"""
        await self.engine._renew_channel_with_retry(self.gen)
        self.engine.client.open_secure_channel.assert_awaited_once_with(renew=True)

    async def test_renew_channel_with_retry_flaky_recovery(self):
        """测试前两次抛出异常，第三次重试成功"""
        self.engine.client.open_secure_channel.side_effect = [
            asyncio.TimeoutError("timeout 1"),
            Exception("network glitch"),
            None
        ]
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await self.engine._renew_channel_with_retry(self.gen)
            self.assertEqual(self.engine.client.open_secure_channel.await_count, 3)
            self.assertEqual(mock_sleep.await_count, 2)

    async def test_renew_channel_exhausted_retries(self):
        """测试全部 3 次重试失败后抛出异常"""
        self.engine.client.open_secure_channel.side_effect = Exception("persistent server failure")
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(Exception):
                await self.engine._renew_channel_with_retry(self.gen)
            self.assertEqual(self.engine.client.open_secure_channel.await_count, 3)

    async def test_renew_supervisor_triggers_reconnect_on_failure(self):
        """测试 supervisor 在续约彻底失败时主动触发受控重连（而不是等待 1 小时被切断）"""
        self.engine.on_connection_lost = MagicMock()

        async def always_fail(*args, **kwargs):
            raise Exception("renew fail")

        with patch.object(self.engine, "_renew_channel_with_retry", side_effect=always_fail):
            # 设置极小 duration，并把"上次续约时间"拨回过去，让续约立刻到期执行
            self.engine.client.secure_channel_timeout = 100  # 0.1s
            self.engine._last_renewed_at = time.monotonic() - 1.0
            supervisor_task = asyncio.create_task(self.engine._renew_supervisor(self.gen))
            await asyncio.sleep(0.35)

            self.assertFalse(self.engine.connected)
            self.engine.on_connection_lost.assert_called_once()
            if not supervisor_task.done():
                supervisor_task.cancel()

    async def test_monitor_channel_lifetime_warning_and_critical(self):
        """测试心跳监控在安全通道超过 90% 寿命未续约时触发保护性断线"""
        self.engine._channel_connected_at = time.monotonic() - 3600  # 超过 3600*0.9 = 3240 秒
        self.engine._last_renewed_at = 0.0
        self.engine.on_connection_lost = MagicMock()

        # _monitor_connection 会先调用 _ensure_link_tasks 自愈后台任务；本用例只验证
        # 寿命超期判定，因此先放两个"活着"的占位任务，避免补拉逻辑引入额外 sleep。
        async def _alive():
            await asyncio.sleep(1000)

        self.engine._renew_task = asyncio.create_task(_alive())
        self.engine._renew_started_at = time.monotonic()
        self.engine._monitor_task = asyncio.create_task(_alive())
        self.engine._monitor_started_at = time.monotonic()

        # 运行一次心跳循环逻辑
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError()]
            try:
                await self.engine._monitor_connection(self.gen)
            except (asyncio.CancelledError, Exception):
                pass

            self.assertFalse(self.engine.connected)
            self.engine.on_connection_lost.assert_called_once()

    async def test_disconnect_does_not_deadlock_with_active_tasks(self):
        """测试 disconnect 在后台任务存活或耗时阻塞时，能依靠 Socket 强关和超时机制瞬间完成，不发生死锁"""
        async def hang_coro():
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                pass

        self.engine._monitor_task = asyncio.create_task(hang_coro())
        self.engine._renew_task = asyncio.create_task(hang_coro())

        t0 = time.monotonic()
        await self.engine.disconnect()
        elapsed = time.monotonic() - t0

        # 验证必定在 1 秒内完成退出，绝不挂起
        self.assertLess(elapsed, 1.0)
        self.assertFalse(self.engine.connected)
        self.assertIsNone(self.engine.client)


if __name__ == "__main__":
    unittest.main()
