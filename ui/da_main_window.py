"""OPC DA 版主窗口。

MainWindow 已经把协议相关部分抽成两个类属性插槽（ENGINE_CLASS / SETTINGS_CLASS），
这里只做替换，监控/分组/调度/日志页面完全复用：

    ENGINE_CLASS   = OpcDaClientEngine   # COM/DCOM + 订阅/轮询降级 + 自治重连
    SETTINGS_CLASS = DaTabSettings       # DCOM 主机 / ProgID / 更新周期 / 浏览深度

另外关闭引擎的主动消息泵（pump_events_enabled=False）：Qt 事件循环本身就在泵
Windows 消息（qasync 主线程），引擎再调 client.pump_events() 会递归泵 Qt 消息，
可能重入事件循环并产生 asyncio "Cannot enter into task ..." 僵尸任务。
"""

from opcda.engine import OpcDaClientEngine
from ui.main_window import MainWindow
from ui.tabs.tab_settings_da import DaTabSettings
from utils.logger import global_logger


class DaMainWindow(MainWindow):
    """灯光自动控制系统（OPC DA 版）主窗口。"""

    ENGINE_CLASS = OpcDaClientEngine
    SETTINGS_CLASS = DaTabSettings

    TITLE = "灯光自动控制系统 DA 版 V1.2.1"

    def __init__(self):
        super().__init__()
        self.setWindowTitle(self.TITLE)

        # Qt 主线程的 Qt 事件循环负责泵 COM 消息，引擎不要重复泵（见模块 docstring）
        try:
            self.opc_engine.pump_events_enabled = False
        except Exception as exc:  # pragma: no cover - 防御性，不影响启动
            global_logger.warning("Failed to disable engine event pumping: {}".format(exc))

        global_logger.info(
            "OPC DA main window initialized. Protocol=OPC DA (COM/DCOM), "
            "engine={}, host={}, prog_id={}".format(
                type(self.opc_engine).__name__,
                getattr(self.opc_engine, "host", "?"),
                getattr(self.opc_engine, "prog_id", "?"),
            )
        )
