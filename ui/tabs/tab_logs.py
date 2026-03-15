try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout
    from PyQt5.QtCore import QObject, pyqtSignal
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout
    from PySide6.QtCore import QObject, Signal as pyqtSignal

import logging
from utils.logger import global_logger


class _LogBridge(QObject):
    """信号桥：将跨线程的日志消息安全地转发到 Qt 主线程"""
    log_message = pyqtSignal(str)


class UILogHandler(logging.Handler):
    """
    一个日志拦截句柄，将原本打在控制台和文件里的日志分流输送到指定的UI文本框。
    使用信号槽机制确保线程安全（兼容 PyQt5 和 PySide6）。
    """
    def __init__(self, widget):
        super().__init__()
        self._bridge = _LogBridge()
        self._bridge.log_message.connect(widget.appendPlainText)

    def emit(self, record):
        try:
            msg = self.format(record)
            self._bridge.log_message.emit(msg)
        except RuntimeError:
            pass  # widget 已销毁


class TabLogs(QWidget):
    def __init__(self):
        super().__init__()
        self._setup_ui()
        self._hook_logger()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        
        btn_layout = QHBoxLayout()
        btn_clear = QPushButton("清空日志面板")
        btn_clear.clicked.connect(self.on_clear)
        btn_layout.addStretch()
        btn_layout.addWidget(btn_clear)
        layout.addLayout(btn_layout)

        self.txt_logs = QPlainTextEdit()
        self.txt_logs.setReadOnly(True)
        self.txt_logs.setMaximumBlockCount(5000)  # 限制最大行数，防止内存无限增长
        # 用深色背景贴合开发者习惯
        self.txt_logs.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas, Courier New, monospace;")
        layout.addWidget(self.txt_logs)
        
    def _hook_logger(self):
        ui_handler = UILogHandler(self.txt_logs)
        # 套用和系统 logger 相同的格式模板
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        ui_handler.setFormatter(formatter)
        ui_handler.setLevel(logging.INFO)
        global_logger.addHandler(ui_handler)
        
        global_logger.info("UI Log Capture Initialized.")

    def on_clear(self):
        self.txt_logs.clear()
