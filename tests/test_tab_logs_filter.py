import unittest
from collections import deque
from ui.tabs.tab_logs import TabLogs


class TestTabLogsFilter(unittest.TestCase):
    def test_filter_matching(self):
        # 创建模拟对象或直接测试 _matches_filter 逻辑
        tab = TabLogs.__new__(TabLogs)
        tab._all_log_records = deque()
        
        class MockCombo:
            def __init__(self, text):
                self._text = text
            def currentText(self):
                return self._text

        class MockEdit:
            def __init__(self, text):
                self._text = text
            def text(self):
                return self._text

        tab.cb_level = MockCombo("全部")
        tab.le_search = MockEdit("")

        # 1. "全部" 模式：接收任何日志
        self.assertTrue(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - System ok"))
        self.assertTrue(tab._matches_filter(30, "WARNING", "2026-09-06 - [WARNING] - Warning msg"))
        self.assertTrue(tab._matches_filter(40, "ERROR", "2026-09-06 - [ERROR] - Error occurred"))

        # 2. "ERROR" 模式：仅接收 ERROR
        tab.cb_level = MockCombo("ERROR")
        self.assertFalse(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - System ok"))
        self.assertFalse(tab._matches_filter(30, "WARNING", "2026-09-06 - [WARNING] - Warning msg"))
        self.assertTrue(tab._matches_filter(40, "ERROR", "2026-09-06 - [ERROR] - Error occurred"))

        # 3. "WARNING" 模式：仅接收 WARNING
        tab.cb_level = MockCombo("WARNING")
        self.assertFalse(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - System ok"))
        self.assertTrue(tab._matches_filter(30, "WARNING", "2026-09-06 - [WARNING] - Warning msg"))
        self.assertFalse(tab._matches_filter(40, "ERROR", "2026-09-06 - [ERROR] - Error occurred"))

        # 4. 关键词搜索过滤联动
        tab.cb_level = MockCombo("全部")
        tab.le_search = MockEdit("renew")
        self.assertTrue(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - [RENEW] renewing channel"))
        self.assertFalse(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - Normal message"))

        # 5. 级别 + 关键词双重过滤
        tab.cb_level = MockCombo("ERROR")
        tab.le_search = MockEdit("renew")
        self.assertTrue(tab._matches_filter(40, "ERROR", "2026-09-06 - [ERROR] - [RENEW] failed"))
        self.assertFalse(tab._matches_filter(20, "INFO", "2026-09-06 - [INFO] - [RENEW] success"))


if __name__ == "__main__":
    unittest.main()
