import sys
import os
import asyncio
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from opc.client_engine import OpcClientEngine
from asyncua import ua

class DummyStatusCode:
    def __init__(self, good=True, name="Good"):
        self._good = good
        self.name = name
    def is_good(self):
        return self._good
    def __repr__(self):
        return f"StatusCode({self.name})"

class MockClient:
    def __init__(self, fail_indices=None):
        self.fail_indices = fail_indices or []
        self.nodes = {}

    def get_node(self, nid):
        return nid

    async def write_values(self, nodes, data_values, raise_on_partial_error=True):
        # 验证必须传入 raise_on_partial_error=False
        assert raise_on_partial_error is False, "write_values must be called with raise_on_partial_error=False"
        results = []
        for i, n in enumerate(nodes):
            if i in self.fail_indices:
                results.append(DummyStatusCode(good=False, name="BadNotWritable"))
            else:
                results.append(DummyStatusCode(good=True, name="Good"))
        return results

class TestBatchWriteResilience(unittest.TestCase):
    def test_partial_failure_tolerance(self):
        async def _run():
            engine = OpcClientEngine()
            engine.connected = True
            engine.client = MockClient(fail_indices=[1, 3])  # 节点 1 和 3 损坏/只读

            # 10 个测试节点
            node_ids = [f"ns=2;s=Light_{i}" for i in range(10)]
            display_names = [f"灯具_{i}" for i in range(10)]

            # 初始连续失败计数置为 2
            engine._consecutive_write_failures = 2

            success_cnt, fail_cnt = await engine.write_values_batch(node_ids, True, display_names)

            # 必须有 8 个成功，2 个失败
            self.assertEqual(success_cnt, 8)
            self.assertEqual(fail_cnt, 2)

            # 因为有 8 个成功（部分成功），连续失败计数必须被重置为 0，不能累加到 3 触发重连！
            self.assertEqual(engine._consecutive_write_failures, 0)
            self.assertTrue(engine.connected)

            # 测试全部 100% 失败的情形
            engine.client = MockClient(fail_indices=list(range(10)))
            engine._consecutive_write_failures = 0
            success_cnt, fail_cnt = await engine.write_values_batch(node_ids, True, display_names)
            self.assertEqual(success_cnt, 0)
            self.assertEqual(fail_cnt, 10)
            # 100% 失败时递增失败计数
            self.assertEqual(engine._consecutive_write_failures, 1)

        asyncio.run(_run())

if __name__ == "__main__":
    unittest.main()
