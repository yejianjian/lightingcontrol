"""自动重连策略与界面一致性回归测试。

复现的真实故障（现场日志）::

    23:35:45,196  [STATE] disconnected -> reconnecting (reconnect supervisor requested)
    23:35:45,585  User requested to stop auto-reconnect
    23:35:45,585  supervisor exited (connected=False, auto_reconnect=False)
    ...
    00:35:59,820  [STATE] connected -> disconnected (asyncua watchdog: Connection is closed)

启动时设置页把自动重连总闸打开了，500ms 后的"自动连接"调用
``on_connect_clicked()``，而该槽把 reconnecting 状态下的点击解释为"用户想停止重连"
—— 于是自动连接自己把总闸关掉了。复选框仍勾着，一小时后真断线时就落在"待连接"。

本文件把这条时序钉住：自动连接不得改变自动重连的意图。
"""

import asyncio
import json
import os
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
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


def _write_config(cfgdir, **overrides):
    cfg = {"aliases": {}, "groups": [], "schedules": [],
           "auto_connect": False, "auto_reconnect": True,
           "connection": {"host": "127.0.0.1", "port": "48401", "user": "",
                          "password": "", "ns_filter": "ns=2;"}}
    cfg.update(overrides)
    os.makedirs(os.path.join(cfgdir, "data"), exist_ok=True)
    with open(os.path.join(cfgdir, "data", "lighting_config.json"), "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False)


@unittest.skipUnless(QT_AVAILABLE, "PyQt5/PySide6 不可用")
class TestAutoReconnectIntent(unittest.IsolatedAsyncioTestCase):
    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    async def asyncSetUp(self):
        self.cfgdir = None
        self._patch = None
        self.window = None

    def _build_window(self, **cfg_overrides):
        self.cfgdir = tempfile.mkdtemp(prefix="lc_recon_cfg_")
        _write_config(self.cfgdir, **cfg_overrides)
        self._patch = patch("utils.persistence._get_base_dir", return_value=self.cfgdir)
        self._patch.start()
        from ui.main_window import MainWindow
        self.window = MainWindow()
        return self.window

    async def asyncTearDown(self):
        try:
            if self.window is not None:
                self.window.opc_engine.stop_reconnect()
                self.window.hide()
                self.window.deleteLater()
                self.app.processEvents()
        finally:
            if self._patch is not None:
                self._patch.stop()
            if self.cfgdir:
                shutil.rmtree(self.cfgdir, ignore_errors=True)

    async def test_auto_reconnect_checked_opens_the_engine_gate(self):
        """勾选自动重连时引擎总闸必须是开的。"""
        w = self._build_window(auto_reconnect=True)
        self.assertTrue(w.tab_settings.cb_auto_reconnect.isChecked())
        self.assertTrue(w.opc_engine._auto_reconnect,
                        "复选框勾着但引擎总闸是关的 —— 断线后不会自动重连")

    async def test_auto_connect_does_not_cancel_auto_reconnect(self):
        """核心回归：启动自动连接不得把自动重连总闸关掉。"""
        w = self._build_window(auto_connect=True, auto_reconnect=True)
        tab = w.tab_settings

        # 启动瞬间（模拟 500ms 后的自动连接）：此时若没有服务器，状态是 reconnecting
        if w.opc_engine.state != 'reconnecting':
            w.opc_engine._set_state(w.opc_engine.STATE_RECONNECTING, "test setup")
        self.assertTrue(w.opc_engine._auto_reconnect)

        tab.start_auto_connect()
        await asyncio.sleep(0.05)   # create_task 只是排队，让连接协程真正推进到网络阶段

        self.assertTrue(
            w.opc_engine._auto_reconnect,
            "自动连接把自动重连总闸关掉了 —— 这正是现场'勾着却不重连'的根因"
        )
        # 自动连接应当真的发起连接并进入连接流程，而不是做断开
        self.assertIsNotNone(tab._connect_task, "自动连接没有发起连接任务")
        self.assertNotEqual(w.opc_engine.state, 'disconnected',
                            "自动连接之后落到了断开态（被当成了断开操作）")
        tab._connect_task.cancel()
        try:
            await tab._connect_task
        except (asyncio.CancelledError, Exception):
            pass

    async def test_auto_connect_is_skipped_when_already_connected(self):
        w = self._build_window()
        tab = w.tab_settings
        w.opc_engine._set_state(w.opc_engine.STATE_CONNECTED, "test setup")
        tab.start_auto_connect()
        self.assertIsNone(tab._connect_task, "已连接时不应再发起自动连接")

    async def test_connection_lost_with_checked_box_arms_the_supervisor(self):
        """断线时若复选框勾着，必须发出"启动重连 supervisor"的请求并打开总闸。

        这里只验证**意图与请求**（总闸 + 是否调用了 reconnect_forever），不检查
        supervisor 任务是否已被测试用例自身的循环调度 —— 那是调度细节，不是不变量。
        """
        w = self._build_window(auto_reconnect=True)
        tab = w.tab_settings
        self.assertTrue(tab.cb_auto_reconnect.isChecked())
        w.opc_engine._set_state(w.opc_engine.STATE_CONNECTED, "test setup")
        # 模拟总闸被外部路径关掉（复选框仍勾着，二者不一致）
        w.opc_engine.stop_reconnect()
        self.assertFalse(w.opc_engine._auto_reconnect)
        self.assertTrue(tab.cb_auto_reconnect.isChecked())

        calls = []
        real_forever = w.opc_engine.reconnect_forever

        async def spy_forever():
            calls.append(1)
            return await real_forever()

        w.opc_engine.reconnect_forever = spy_forever
        try:
            tab._handle_connection_lost()
            await asyncio.sleep(0.2)
        finally:
            w.opc_engine.reconnect_forever = real_forever

        self.assertTrue(w.opc_engine._auto_reconnect,
                        "断线后总闸没被恢复到复选框所声明状态")
        self.assertEqual(len(calls), 1,
                         "断线后没有请求启动重连 supervisor（复选框勾着却被停在待连接）")
        w.opc_engine.stop_reconnect()

    async def test_unchecking_before_loss_does_not_arm_supervisor(self):
        """用户取消勾选后断线：不得请求启动 supervisor，必须停在待连接。"""
        w = self._build_window(auto_reconnect=True)
        tab = w.tab_settings
        w.opc_engine._set_state(w.opc_engine.STATE_CONNECTED, "test setup")
        tab.cb_auto_reconnect.setChecked(False)
        await asyncio.sleep(0.05)
        self.assertFalse(w.opc_engine._auto_reconnect)

        calls = []
        real_forever = w.opc_engine.reconnect_forever

        async def spy_forever():
            calls.append(1)
            return await real_forever()

        w.opc_engine.reconnect_forever = spy_forever
        try:
            tab._handle_connection_lost()
            await asyncio.sleep(0.2)
        finally:
            w.opc_engine.reconnect_forever = real_forever

        self.assertEqual(calls, [],
                         "用户已取消自动重连，断线时不应再请求启动 supervisor")
        self.assertFalse(w.opc_engine._auto_reconnect)

    async def test_button_stop_also_unchecks_the_box(self):
        """点"停止自动重连"必须同步取消勾选，否则界面会误导值守人员。"""
        w = self._build_window(auto_reconnect=True)
        tab = w.tab_settings
        w.opc_engine._set_state(w.opc_engine.STATE_RECONNECTING, "test setup")

        tab.on_connect_clicked()   # reconnecting 状态下点击 == 停止自动重连
        await asyncio.sleep(0)

        self.assertFalse(tab.cb_auto_reconnect.isChecked(),
                         "已停止自动重连但复选框仍勾着，界面与实际不一致")
        self.assertFalse(w.opc_engine._auto_reconnect)
        self.assertFalse(w.dm.pm.data_store.get("auto_reconnect"))

    async def test_unchecking_before_loss_keeps_manual_mode(self):
        """用户确实取消勾选时，断线必须停在待连接（尊重用户意图）。"""
        w = self._build_window(auto_reconnect=True)
        tab = w.tab_settings
        w.opc_engine._set_state(w.opc_engine.STATE_CONNECTED, "test setup")
        tab.cb_auto_reconnect.setChecked(False)
        await asyncio.sleep(0.05)
        self.assertFalse(tab.cb_auto_reconnect.isChecked())
        self.assertFalse(w.opc_engine._auto_reconnect)

        tab._handle_connection_lost()
        await asyncio.sleep(0.2)

        self.assertFalse(w.opc_engine._auto_reconnect)
        task = w.opc_engine._reconnect_task
        self.assertTrue(task is None or task.done(),
                        "用户已取消自动重连，断线时不应再拉起 supervisor")

    # ----------------------------------------------------------------------------------
    # 冷启动"勾选自动重连"路径：supervisor 直接 connect()，必须已注入表单里的账号密码
    # （现场故障：首次启动勾选自动重连后一直 BadUserAccessDenied，停止重连手动连接却成功）
    # ----------------------------------------------------------------------------------
    async def test_auto_reconnect_from_startup_uses_saved_credentials(self):
        conn = {"host": "10.9.8.7", "port": "48401", "user": "admin",
                "password": "s3cret", "ns_filter": "ns=2;"}
        w = self._build_window(auto_reconnect=True, connection=conn)
        engine = w.opc_engine
        self.assertEqual(engine.host, "10.9.8.7",
                         "冷启动开启自动重连时引擎没拿到配置里的主机")
        self.assertEqual(engine.port, 48401)
        self.assertEqual(engine.username, "admin",
                         "冷启动开启自动重连时引擎处于匿名状态，网关会拒绝 ActivateSession")
        self.assertEqual(engine.password, "s3cret",
                         "冷启动开启自动重连时没有注入密码")
        self.assertTrue(engine._auto_reconnect)

    async def test_ticking_auto_reconnect_injects_form_credentials(self):
        conn = {"host": "10.9.8.7", "port": "48401", "user": "admin",
                "password": "s3cret", "ns_filter": "ns=2;"}
        w = self._build_window(auto_reconnect=False, connection=conn)
        tab = w.tab_settings
        self.assertFalse(w.opc_engine._auto_reconnect)

        tab.cb_auto_reconnect.setChecked(True)
        await asyncio.sleep(0)

        self.assertTrue(w.opc_engine._auto_reconnect)
        self.assertEqual(w.opc_engine.username, "admin",
                         "勾选自动重连后引擎仍是匿名，supervisor 首次连接会 BadUserAccessDenied")
        self.assertEqual(w.opc_engine.password, "s3cret")
        self.assertEqual(w.opc_engine.host, "10.9.8.7")

    async def test_nodes_recovered_publishes_nodes_without_manual_connect(self):
        """supervisor 自己建链时也必须把 engine.nodes 灌进 DataManager，否则界面 0 点位。"""
        w = self._build_window(auto_reconnect=False)
        tab = w.tab_settings
        w.opc_engine.nodes = {
            "ns=2;s=Light_1": {
                "node_id": "ns=2;s=Light_1",
                "name": "Light 1",
                "type": "Boolean",
                "value": True,
                "timestamp": "2026-10-23 15:30:00",
            },
        }
        self.assertEqual(w.dm.get_node_list(), [])
        with patch.object(tab, "_start_scheduler"):
            tab._on_nodes_recovered()

        published = w.dm.get_node_list()
        self.assertEqual(len(published), 1, "自动重连建立连接后界面没有发布任何点位")
        self.assertEqual(published[0]["node_id"], "ns=2;s=Light_1")
        self.assertTrue(published[0]["value"])


if __name__ == "__main__":
    unittest.main()
