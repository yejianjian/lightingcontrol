import sys
import os
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.persistence import PersistenceManager

class TestPersistenceConcurrency(unittest.TestCase):
    def test_multi_threaded_persistence(self):
        test_file = "tmp_concurrency_test.json"
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

        pm = PersistenceManager(test_file)

        errors = []
        num_threads = 8
        ops_per_thread = 40

        def worker(thread_idx):
            try:
                for i in range(ops_per_thread):
                    gid = pm.add_group(f"Group_T{thread_idx}_{i}")
                    pm.update_group_members(gid, [f"node_{thread_idx}_{i}_{x}" for x in range(5)])
                    pm.set_alias(f"node_{thread_idx}_{i}_0", f"Alias_T{thread_idx}_{i}")
                    
                    # 读取快照
                    groups = pm.get_groups()
                    aliases = pm.get_all_aliases()
                    self.assertIsInstance(groups, list)
                    self.assertIsInstance(aliases, dict)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 清理临时文件
        if os.path.exists(test_file):
            os.remove(test_file)
        if os.path.exists(test_file + ".bak"):
            os.remove(test_file + ".bak")

        self.assertEqual(len(errors), 0, f"Concurrency errors encountered: {errors}")

if __name__ == "__main__":
    unittest.main()
