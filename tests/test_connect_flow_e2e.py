"""端到端连接流程测试：真实加密服务器 -> 点位必须出现在数据总线与仪表盘。

这是对"连上了却一个点位都没有"的直接复现与防回归：走完整的 UI 连接流程
（TabSettings.on_connect_clicked -> connect -> bootstrap -> 发布点位），
断言 DataManager 里的点位数量、仪表盘计数与点位表内容。
"""

import asyncio
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
os.environ["QT_QPA_PLATFORM"] = "offscreen"

try:
    from PyQt5.QtWidgets import QApplication
    QT_AVAILABLE = True
except ImportError:
    try:
        from PySide6.QtWidgets import QApplication
        QT_AVAILABLE = True
    except ImportError:
        QT_AVAILABLE = False

try:
    from tests.test_reconnect_integration import _ServerProcess
    SERVER_HELPER_AVAILABLE = True
except Exception:
    SERVER_HELPER_AVAILABLE = False

def _free_port():
    """向系统申请一个空闲端口，避免与上一次失败运行遗留的服务器进程撞端口。"""
    import socket as _socket
    with _socket.socket(_socket.AF_INET, _socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@unittest.skipUnless(QT_AVAILABLE and SERVER_HELPER_AVAILABLE, "需要 Qt 与集成测试服务器")
class TestFullConnectFlow(unittest.TestCase):
    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self):
        self.workdir = tempfile.mkdtemp(prefix="lc_e2e_")
        self.cfgdir = tempfile.mkdtemp(prefix="lc_e2e_cfg_")
        self.port = _free_port()
        self.server = _ServerProcess(self.port, self.workdir)
        self.server.start()
        # READY 只代表子进程写到了这一行，端口可能还没真正 accept；等它可连接再连，
        # 否则会随机撞上 ConnectionRefused（服务端初始化需要几秒）。
        self.server.wait_accepting(timeout=60.0)
        self._patch = patch("utils.persistence._get_base_dir", return_value=self.cfgdir)
        self._patch.start()

        # 关掉自动重连与自动连接：否则 MainWindow 构造时就会按默认配置去连 127.0.0.1:48401，
        # 那条失败路径会污染日志、并与本用例手动发起的连接互相干扰。
        import json
        os.makedirs(os.path.join(self.cfgdir, "data"), exist_ok=True)
        with open(os.path.join(self.cfgdir, "data", "lighting_config.json"), "w",
                  encoding="utf-8") as f:
            json.dump({"aliases": {}, "groups": [], "schedules": [],
                       "auto_connect": False, "auto_reconnect": False,
                       "connection": {"host": "127.0.0.1", "port": str(self.port),
                                      "user": "", "password": "", "ns_filter": "ns=2;"}},
                      f, ensure_ascii=False)

    def tearDown(self):
        try:
            self.server.kill()
        finally:
            try:
                if getattr(self, "window", None) is not None:
                    self.window.opc_engine.stop_reconnect()
                    self.window.hide()
                    self.window.deleteLater()
                    self.app.processEvents()
            finally:
                self._patch.stop()
                shutil.rmtree(self.workdir, ignore_errors=True)
                shutil.rmtree(self.cfgdir, ignore_errors=True)

    def test_connect_flow_populates_points_and_dashboard(self):
        from ui.main_window import MainWindow

        self.window = MainWindow()
        tab = self.window.tab_settings
        tab.le_host.setText("127.0.0.1")
        tab.le_port.setText(str(self.port))
        tab.le_ns_filter.setText("ns=2;")

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(tab._connect_and_load())
        finally:
            loop.run_until_complete(self.window.opc_engine.disconnect())
            loop.close()

        dm = self.window.dm
        self.assertEqual(
            sorted(dm.nodes.keys()),
            ["ns=2;s=Light_1", "ns=2;s=Light_2"],
            "连接流程结束后数据总线里没有点位 —— 界面会显示 0 个点位",
        )
        # 仪表盘计数（两个都是 False 布尔量）
        self.assertEqual(dm.on_count, 0)
        self.assertEqual(dm.off_count, 2)
        # 点位表内容完整，且不含底层 asyncua 引用
        listed = dm.get_node_list()
        self.assertEqual(len(listed), 2)
        sample = dm.nodes["ns=2;s=Light_1"]
        self.assertEqual(sample["name"], "Light 1")
        self.assertIs(sample["value"], False)
        self.assertNotIn("node_obj", sample)


if __name__ == "__main__":
    unittest.main()
