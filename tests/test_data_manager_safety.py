import sys
import os
import shutil
import tempfile
import threading
import time
import random
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.data_manager import DataManager
from utils.persistence import PersistenceManager

class TestDataManagerSafety(unittest.TestCase):
    def test_concurrent_read_write_and_count_consistency(self):
        test_file = "tmp_dm_test.json"
        if os.path.exists(test_file):
            os.remove(test_file)

        pm = PersistenceManager(test_file)
        dm = DataManager(pm)

        # 预置 20 个点位
        for i in range(20):
            dm.update_node(f"node_{i}", {"name": f"Light_{i}", "value": False, "type": "Boolean"})

        self.assertEqual(dm.on_count, 0)
        self.assertEqual(dm.off_count, 20)

        stop_event = threading.Event()
        errors = []

        # 写入线程：频繁更新点位状态，使用 int (1/0), bool (True/False), 字符串 ('1'/'0')
        def writer():
            value_choices = [True, False, 1, 0, "1", "0", "true", "false", "on", "off"]
            while not stop_event.is_set():
                try:
                    nid = f"node_{random.randint(0, 19)}"
                    val = random.choice(value_choices)
                    dm.update_node(nid, {"value": val})
                    time.sleep(0.001)
                except Exception as e:
                    errors.append(e)

        # 读取线程：频繁读取节点列表快照
        def reader():
            while not stop_event.is_set():
                try:
                    nlist = dm.get_node_list()
                    self.assertEqual(len(nlist), 20)
                    for n in nlist:
                        _ = n.get("value")
                    _ = dm.get_alias_by_node_id("node_0")
                    time.sleep(0.001)
                except Exception as e:
                    errors.append(e)

        threads = [
            threading.Thread(target=writer),
            threading.Thread(target=writer),
            threading.Thread(target=reader),
            threading.Thread(target=reader),
        ]

        for t in threads:
            t.start()

        time.sleep(1.5)
        stop_event.set()

        for t in threads:
            t.join()

        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

        self.assertEqual(len(errors), 0, f"DataManager concurrency errors: {errors}")

        # 校验计数器的自洽性：dm.on_count + dm.off_count 必须等于 20
        self.assertEqual(dm.on_count + dm.off_count, 20, f"Expected on+off=20, got on={dm.on_count}, off={dm.off_count}")

        # 测试自愈能力 recalculate_counts
        old_on, old_off = dm.on_count, dm.off_count
        dm.recalculate_counts()
        self.assertEqual(dm.on_count, old_on)
        self.assertEqual(dm.off_count, old_off)

class TestAliasAuthority(unittest.TestCase):
    """别名（alias）是用户配置，绝不能被引擎/刷新的默认显示名覆盖。

    现场问题：DA 版手动添加一个控制点位后，_publish_nodes_to_bus() 会把引擎点位表
    重新灌进 DataManager；旧实现用 dict.update() 把引擎自带的 alias（原始 ItemID）
    写回已有点位，导致用户导入的别名全部消失，只能重新导入别名表。
    """

    def setUp(self):
        self._tmp = tempfile.mkdtemp(prefix="tmp_dm_alias_")
        self._cfg = os.path.join(self._tmp, "lighting_config.json")
        from utils.persistence import PersistenceManager
        from core.data_manager import DataManager
        self.pm = PersistenceManager(self._cfg)
        self.dm = DataManager(self.pm)

    def tearDown(self):
        shutil.rmtree(self._tmp, ignore_errors=True)

    @staticmethod
    def _engine_node(node_id, name=None, alias=None, value=1):
        return {
            "node_id": node_id,
            "name": name or node_id,
            "alias": node_id if alias is None else alias,
            "type": "Int32",
            "value": value,
            "timestamp": "2026-01-01 00:00:00",
        }

    def test_engine_publish_does_not_overwrite_saved_alias(self):
        self.dm.set_alias("Random.Int1", "一楼大厅")
        self.dm.update_node("Random.Int1", self._engine_node("Random.Int1"))
        self.assertEqual(self.dm.nodes["Random.Int1"]["alias"], "一楼大厅")

        # 再次发布（模拟手动加点 / 重连后的 _publish_nodes_to_bus）
        self.dm.update_node("Random.Int1", self._engine_node("Random.Int1"))
        self.assertEqual(self.dm.nodes["Random.Int1"]["alias"], "一楼大厅")
        self.assertEqual(self.dm.pm.get_all_aliases().get("Random.Int1"), "一楼大厅")

    def test_alias_survives_subscription_value_updates(self):
        self.dm.set_alias("Random.Int1", "大厅主灯")
        self.dm.update_node("Random.Int1", self._engine_node("Random.Int1"))
        self.dm.update_node("Random.Int1", {"value": True, "timestamp": "t2"})
        node = self.dm.nodes["Random.Int1"]
        self.assertEqual(node["alias"], "大厅主灯")
        self.assertEqual(node["value"], True)

    def test_persisted_alias_applies_when_node_is_discovered(self):
        # 先导入别名（此时点位还没发现），再连接发布点位
        self.dm.set_alias("Device.Tag1", "三层走廊")
        self.dm.update_node("Device.Tag1", self._engine_node("Device.Tag1"))
        self.assertEqual(self.dm.nodes["Device.Tag1"]["alias"], "三层走廊")

    def test_engine_alias_used_as_fallback_without_user_alias(self):
        self.dm.update_node(
            "Random.Int2",
            self._engine_node("Random.Int2", name="Int2", alias="Int2"),
        )
        self.assertEqual(self.dm.nodes["Random.Int2"]["alias"], "Int2")

    def test_batch_apply_aliases_is_locked_and_persisted(self):
        self.dm.update_node("n1", self._engine_node("n1"))
        self.dm.update_node("n2", self._engine_node("n2"))
        applied = self.dm.batch_apply_aliases({"n1": "灯一", "n2": "灯二"})
        self.assertEqual(applied, 2)
        self.assertEqual(self.dm.nodes["n1"]["alias"], "灯一")
        self.assertEqual(self.dm.nodes["n2"]["alias"], "灯二")
        self.assertEqual(self.dm.pm.get_all_aliases().get("n1"), "灯一")
        self.assertIn("n1", self.dm.get_dirty_nodes_and_clear())


if __name__ == "__main__":
    unittest.main()
