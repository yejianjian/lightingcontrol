"""OPC DA 版程序入口（与 main.py 同构，只是换成 DaMainWindow）。

运行：python main_da.py
打包：build_da.bat -> dist/LightingControlDA_v1.2.1.exe
"""

import sys
import asyncio
import traceback

try:
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtCore import Qt
except ImportError:
    from PySide6.QtWidgets import QApplication
    from PySide6.QtCore import Qt

from qasync import QEventLoop
from ui.da_main_window import DaMainWindow
from utils.logger import global_logger

sys.excepthook = lambda type, value, tb: global_logger.error(
    "Uncaught exception", exc_info=(type, value, tb)
)


def attach_to_default_desktop():
    """如果在沙箱或虚拟桌面中被启动，切换到用户活动物理桌面 WinSta0/Default"""
    try:
        import ctypes
        user32 = ctypes.windll.user32
        h_def = user32.OpenDesktopW("Default", 0, False, 0x01FF)
        if h_def:
            user32.SetThreadDesktop(h_def)
    except Exception:
        pass


if __name__ == "__main__":
    attach_to_default_desktop()
    global_logger.info("Application starting... [protocol=OPC DA (COM/DCOM)]")
    global_logger.info(
        "OPC DA build: direct COM/DCOM client (comtypes), no OPC Core Components / pywin32 needed; "
        "subscription uses IOPCAsyncIO2 and falls back to SyncRead polling for legacy DA1 servers."
    )

    # 启用 High-DPI 缩放与自适应字体（必须在实例化 QApplication 之前调用）
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiFontPointSizes'):
        QApplication.setAttribute(Qt.AA_UseHighDpiFontPointSizes, True)

    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = DaMainWindow()
    window.show()
    window.raise_()
    window.activateWindow()

    with loop:
        try:
            global_logger.info("Event loop running.")
            loop.run_forever()
        except Exception as e:
            global_logger.error(f"Event loop crashed: {e}")
