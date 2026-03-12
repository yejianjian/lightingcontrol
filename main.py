import sys
import asyncio
import traceback

try:
    from PyQt5.QtWidgets import QApplication
except ImportError:
    from PySide6.QtWidgets import QApplication

from qasync import QEventLoop
from ui.main_window import MainWindow
from utils.logger import global_logger

sys.excepthook = lambda type, value, tb: global_logger.error("Uncaught exception", exc_info=(type, value, tb))

if __name__ == "__main__":
    global_logger.info("Application starting...")
    app = QApplication(sys.argv)
    loop = QEventLoop(app)
    asyncio.set_event_loop(loop)

    window = MainWindow()
    window.show()

    with loop:
        try:
            global_logger.info("Event loop running.")
            loop.run_forever()
        except Exception as e:
            global_logger.error(f"Event loop crashed: {e}")
