"""重连稳定性回归测试。

覆盖的具体缺陷：
* 订阅长期没有数据变化被误判为断线（灯光点位可能数小时不变）；
* asyncua ``open_secure_channel`` 超时后 ``_open_secure_channel_exchange`` 残留，
  导致后续"重试"全部瞬间失败；
* 写入失败与看门狗重复触发断线（同一生命周期内多次触发）；
* 旧世代的回调污染新连接的状态；
* ``disconnect()`` 不幂等、可能挂死。
"""

import asyncio
import os
import sys
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from opc.client_engine import OpcClientEngine, SubHandler, ConnectionSuperseded


class FakeClient:
    """最小化的 asyncua Client 替身，只实现引擎会触碰到的部分。"""

    def __init__(self, secure_channel_timeout=3_600_000, socket_open=True):
        self.secure_channel_timeout = secure_channel_timeout
        self.session_timeout = 60_000
        self._monitor_server_task = None
        self._renew_channel_task = None
        self._closing = False
        self._endpoint_server_uri = None
        self.nodes = MagicMock()
        self.uaclient = MagicMock()
        self.uaclient._open_secure_channel_exchange = None
        self.uaclient._publish_task = None
        self.uaclient.protocol = MagicMock()
        self.uaclient.protocol.state = 0 if socket_open else 1
        self._node = MagicMock()
        self._node.read_value = AsyncMock(return_value=0)
        self._node.read_data_value = AsyncMock(return_value=MagicMock(Value=None, SourceTimestamp=None))
        self.socket_closed = False

    def get_node(self, nid):
        return self._node

    def disconnect_socket(self):
        self.socket_closed = True

    async def disconnect(self):
        self.socket_closed = True

    async def open_secure_channel(self, renew=False):
        return None

    async def write_values(self, nodes, data_values, raise_on_partial_error=True):
        return []


class TestConnectionHealthNotDataDriven(unittest.IsolatedAsyncioTestCase):
    """核心回归：没有数据推送 ≠ 断线。"""

    async def asyncSetUp(self):
        self.engine = OpcClientEngine(host="127.0.0.1", port=48401)
        self.engine.client = FakeClient()
        self.engine._generation = 7
        self.engine._channel_connected_at = time.monotonic()
        self.engine._last_renewed_at = time.monotonic()
        self.engine._set_state(OpcClientEngine.STATE_CONNECTED, "test setup")
        self.engine.on_connection_lost = MagicMock()
        self.lost_calls = []
        self.engine.on_connection_lost.side_effect = lambda: self.lost_calls.append(time.monotonic())
        # 订阅 handler 记录了"很久没有推送"
        self.engine._sub_handler = SubHandler(lambda *a: None)
        self.engine._sub_handler.last_data_time = time.monotonic() - 3600

    async def test_no_data_push_for_an_hour_is_not_treated_as_disconnect(self):
        """灯光状态可能整天不变，订阅不推送不能作为断线判据。"""
        failure = self.engine._evaluate_health()
        self.assertIsNone(failure, f"health check must not fail on data silence, got: {failure}")

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_sleep.side_effect = [None, asyncio.CancelledError()]
            await self.engine._monitor_connection(self.engine._generation)

        self.assertEqual(self.lost_calls, [], "silent subscription must not trigger reconnect")
        self.assertEqual(self.engine.state, OpcClientEngine.STATE_CONNECTED)

    async def test_dead_publish_task_is_detected(self):
        """订阅底层发布任务死亡必须被识别为链路故障。"""
        dead = asyncio.get_event_loop().create_future()
        dead.set_exception(RuntimeError("publish loop died"))
        self.engine.client.uaclient._publish_task = dead
        failure = self.engine._evaluate_health()
        self.assertIsNotNone(failure)
        self.assertIn("publish", failure)
        dead.exception()  # 取回异常，避免 "exception was never retrieved" 噪音

    async def test_channel_renewal_overdue_is_detected(self):
        self.engine.client.secure_channel_timeout = 100_000  # 100s lifetime
        self.engine._last_renewed_at = time.monotonic() - 95  # > 90%
        failure = self.engine._evaluate_health()
        self.assertIsNotNone(failure)
        self.assertIn("overdue", failure)


class TestRenewalRetryActuallyRetries(unittest.IsolatedAsyncioTestCase):
    """核心回归：续约首次超时后，重试必须能真正发出去。"""

    async def asyncSetUp(self):
        self.engine = OpcClientEngine(host="127.0.0.1", port=48401)
        self.engine.client = FakeClient()
        self.engine._generation = 3
        self.seen_exchange = []

    async def test_stale_secure_channel_exchange_is_reset_before_retry(self):
        uaclient = self.engine.client.uaclient
        # 模拟 asyncua 首次续约超时后的残留状态
        uaclient._open_secure_channel_exchange = object()
        state = {"attempt": 0}

        async def flaky_open_secure_channel(renew=False):
            state["attempt"] += 1
            # 记录每次调用时 exchange 是否已被复位
            self.seen_exchange.append(uaclient._open_secure_channel_exchange)
            if state["attempt"] < 2:
                raise asyncio.TimeoutError("response never came back")
            return None

        self.engine.client.open_secure_channel = flaky_open_secure_channel

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await self.engine._renew_channel_with_retry(self.engine._generation)

        self.assertEqual(state["attempt"], 2)
        # 第一次调用前必须已复位；若未复位，真实 asyncua 会立刻抛
        # "Two Open Secure Channel requests can not happen too close to each other"
        self.assertIsNone(self.seen_exchange[0])
        self.assertIsNone(self.seen_exchange[1])

    async def test_retry_gives_up_after_three_attempts_and_records_cause(self):
        self.engine.client.open_secure_channel = AsyncMock(side_effect=asyncio.TimeoutError("no answer"))
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with self.assertRaises(asyncio.TimeoutError):
                await self.engine._renew_channel_with_retry(self.engine._generation)
        self.assertEqual(self.engine.client.open_secure_channel.await_count, 3)

    async def test_renewal_superseded_generation_aborts(self):
        """世代已变（用户断开/重连）时续约必须立刻退出，不再打服务器。"""
        self.engine.client.open_secure_channel = AsyncMock()
        with self.assertRaises(ConnectionSuperseded):
            await self.engine._renew_channel_with_retry(self.engine._generation + 1)


class TestConnectionLostIsSingleShot(unittest.IsolatedAsyncioTestCase):
    """核心回归：断线通知在一个连接生命周期内只触发一次，且旧世代不能污染新连接。"""

    async def asyncSetUp(self):
        self.engine = OpcClientEngine()
        self.engine._generation = 11
        self.engine._set_state(OpcClientEngine.STATE_CONNECTED, "setup")
        self.lost_calls = []
        self.engine.on_connection_lost = lambda: self.lost_calls.append(1)

    async def test_write_failures_and_watchdog_trigger_once(self):
        # 连续写入失败达到阈值
        for _ in range(self.engine._max_write_failures):
            self.engine._on_write_failure(self.engine._generation)
        # 看门狗/续约随后再报一次（真实场景中会同时到达）
        self.engine._handle_connection_lost("watchdog after write failures", self.engine._generation)
        # asyncua 内部回调再来一次
        await self.engine._on_asyncua_connection_lost(ConnectionError("socket gone"), self.engine._generation)

        self.assertEqual(len(self.lost_calls), 1, "on_connection_lost must fire exactly once per connection")
        self.assertNotEqual(self.engine.state, OpcClientEngine.STATE_CONNECTED)

    async def test_stale_generation_callbacks_are_ignored(self):
        stale_gen = self.engine._generation - 1
        await self.engine._on_asyncua_connection_lost(ConnectionError("old socket"), stale_gen)
        self.engine._handle_connection_lost("stale watchdog", stale_gen)
        self.assertEqual(self.lost_calls, [])
        self.assertEqual(self.engine.state, OpcClientEngine.STATE_CONNECTED)

    async def test_reconnect_state_is_reported_when_auto_reconnect_enabled(self):
        self.engine._auto_reconnect = True
        self.engine._handle_connection_lost("server gone", self.engine._generation)
        self.assertEqual(self.engine.state, OpcClientEngine.STATE_RECONNECTING)
        self.assertFalse(self.engine.connected)


class TestDisconnectIsIdempotentAndBounded(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_and_concurrent_disconnect_finishes_quickly(self):
        engine = OpcClientEngine()
        engine.client = FakeClient()

        async def hang():
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                pass

        engine._monitor_task = asyncio.create_task(hang())
        engine._renew_task = asyncio.create_task(hang())
        engine._reconnect_task = asyncio.create_task(hang())

        t0 = time.monotonic()
        # 并发 + 重复调用，必须全部安全返回
        await asyncio.gather(engine.disconnect(), engine.disconnect(), engine.disconnect())
        await engine.disconnect()
        elapsed = time.monotonic() - t0

        self.assertLess(elapsed, 5.0, "disconnect must be bounded")
        self.assertIsNone(engine.client)
        self.assertFalse(engine.connected)
        self.assertEqual(engine.state, OpcClientEngine.STATE_DISCONNECTED)
        for task in (engine._monitor_task, engine._renew_task, engine._reconnect_task):
            self.assertIsNone(task)

    async def test_disconnect_clears_stale_asyncua_request_futures(self):
        """断线时 asyncua 不会主动失败已发送的请求 Future，必须清表避免迟到响应命中旧 id。"""
        engine = OpcClientEngine()
        client = FakeClient()
        client.uaclient._callbackmap = {1: object(), 2: object()}
        engine.client = client

        await engine.disconnect()

        self.assertEqual(client.uaclient._callbackmap, {})

    async def test_disconnect_on_dead_socket_skips_graceful_close(self):
        engine = OpcClientEngine()
        client = FakeClient(socket_open=False)
        client.disconnect = AsyncMock(side_effect=AssertionError("should not try graceful close"))
        engine.client = client

        await engine.disconnect()

        client.disconnect.assert_not_awaited()
        self.assertIsNone(engine.client)


class TestOperationsAbortWhenSuperseded(unittest.IsolatedAsyncioTestCase):
    async def test_node_browse_aborts_when_generation_changes(self):
        engine = OpcClientEngine()
        engine._generation = 5
        engine._set_state(OpcClientEngine.STATE_LOADING, "loading")

        calls = {"n": 0}

        class SlowNode:
            def __init__(self, engine):
                self.engine = engine

            async def get_children(self):
                calls["n"] += 1
                # 模拟遍历过程中用户点了断开
                self.engine._generation += 1
                return []

        engine.client = MagicMock()

        with self.assertRaises(ConnectionSuperseded):
            await engine._browse_recursive(SlowNode(engine), [], 3, 5)

    async def test_get_all_nodes_aborts_when_generation_changes(self):
        engine = OpcClientEngine()
        engine._generation = 5
        engine._set_state(OpcClientEngine.STATE_LOADING, "loading")
        engine.client = MagicMock()
        engine.client.nodes.objects = MagicMock(return_value="objects-node")

        async def superseding_browse(node, out, depth, gen):
            engine._generation += 1

        engine._browse_recursive = superseding_browse
        with self.assertRaises(ConnectionSuperseded):
            await engine.get_all_nodes()


class TestBootstrapReusesCachedNodes(unittest.IsolatedAsyncioTestCase):
    """重连不应清空点位表，也不应每次都重新遍历上千个节点。"""

    async def test_bootstrap_skips_browse_when_cache_present(self):
        engine = OpcClientEngine()
        engine._generation = 2
        engine._set_state(OpcClientEngine.STATE_LOADING, "loading")
        engine.client = FakeClient()
        engine.nodes = {
            "ns=2;s=L1": {"name": "L1", "node_id": "ns=2;s=L1", "type": "Boolean",
                          "value": False, "timestamp": "", "node_obj": None},
        }
        engine.get_all_nodes = AsyncMock(side_effect=AssertionError("must not re-browse cached nodes"))

        await engine.bootstrap()

        engine.get_all_nodes.assert_not_awaited()
        self.assertEqual(engine.state, OpcClientEngine.STATE_CONNECTED)
        # node_obj 必须被重绑到新 Client 上（旧世代的 Node 对象不可再使用）
        self.assertIsNotNone(engine.nodes["ns=2;s=L1"]["node_obj"])


class TestSessionTimeoutIsClamped(unittest.IsolatedAsyncioTestCase):
    """Session 保活窗口决定了 asyncua 内部看门狗的断线发现速度，必须可配且有界。"""

    async def test_clamping(self):
        engine = OpcClientEngine()
        engine.session_timeout_ms = 1
        self.assertEqual(engine._clamped_session_timeout(), 10_000)
        engine.session_timeout_ms = 10 ** 9
        self.assertEqual(engine._clamped_session_timeout(), 3_600_000)
        engine.session_timeout_ms = 60_000
        self.assertEqual(engine._clamped_session_timeout(), 60_000)

    async def test_backoff_is_exponential_and_capped(self):
        engine = OpcClientEngine()
        engine.reconnect_base_delay = 10.0
        engine.reconnect_max_delay = 60.0
        d1, d2, d3, d8 = (engine._next_reconnect_delay(i) for i in (1, 2, 3, 8))
        self.assertLess(d1, d2)
        self.assertLess(d2, d3)
        self.assertLessEqual(d3, 60.0 * 1.2)
        self.assertLessEqual(d8, 60.0 * 1.2)


class TestConfigureInvalidatesNodeCache(unittest.IsolatedAsyncioTestCase):
    async def test_endpoint_change_drops_cached_nodes(self):
        engine = OpcClientEngine(host="10.0.0.1", port=4840)
        engine.nodes = {"ns=2;s=L1": {"node_id": "ns=2;s=L1", "node_obj": object()}}
        engine.configure(host="10.0.0.2", port=4840)
        self.assertEqual(engine.nodes, {})

    async def test_same_endpoint_keeps_cached_nodes(self):
        engine = OpcClientEngine(host="10.0.0.1", port=4840)
        engine.nodes = {"ns=2;s=L1": {"node_id": "ns=2;s=L1", "node_obj": object()}}
        engine.configure(username="op", password="pw")
        self.assertIn("ns=2;s=L1", engine.nodes)


if __name__ == "__main__":
    unittest.main()
