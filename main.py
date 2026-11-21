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
from ui.main_window import MainWindow
from utils.logger import global_logger

sys.excepthook = lambda type, value, tb: global_logger.error("Uncaught exception", exc_info=(type, value, tb))

def attach_to_default_desktop():
    """如果在沙箱或虚拟桌面中被启动，切换到用户活动物理桌面 WinSta0\\Default"""
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
    global_logger.info("Application starting...")

    # 启用 High-DPI 缩放与自适应字体（必须在实例化 QApplication 之前调用）
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiFontPointSizes'):
        QApplication.setAttribute(Qt.AA_UseHighDpiFontPointSizes, True)

    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = MainWindow()
    window.show()
    window.raise_()
    window.activateWindow()

    with loop:
        try:
            global_logger.info("Event loop running.")
            loop.run_forever()
        except Exception as e:
            global_logger.error(f"Event loop crashed: {e}")
