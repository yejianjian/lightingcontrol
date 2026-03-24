try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout, QLineEdit, QLabel, QComboBox
    from PyQt5.QtCore import QObject, pyqtSignal, Qt, QTimer
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout, QLineEdit, QLabel, QComboBox
    from PySide6.QtCore import QObject, Signal as pyqtSignal, Qt, QTimer

import logging
from collections import deque
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
        self._widget = widget
        self._bridge = _LogBridge()
        self._bridge.log_message.connect(self._on_log_message)

    def _on_log_message(self, msg):
        if hasattr(self._widget, '_all_log_lines'):
            self._widget._all_log_lines.append(msg)
        # 仅在无过滤条件时直接追加，否则由防抖定时器触发过滤重建
        if hasattr(self._widget, '_has_active_filter') and self._widget._has_active_filter():
            self._widget._schedule_filter()
        else:
            self._widget.appendPlainText(msg)

    def emit(self, record):
        try:
            msg = self.format(record)
            self._bridge.log_message.emit(msg)
        except RuntimeError:
            # widget 已销毁，静默忽略
            pass
        except Exception as e:
            # 记录其他异常
            pass


class TabLogs(QWidget):
    def __init__(self):
        super().__init__()
        self._ui_handler = None  # 保存 handler 引用，销毁时移除
        self._filter_timer = None  # 防抖定时器
        self._setup_ui()
        self._hook_logger()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # 搜索和过滤栏
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("搜索:"))
        self.le_search = QLineEdit()
        self.le_search.setPlaceholderText("输入关键词搜索日志...")
        self.le_search.textChanged.connect(self._schedule_filter)
        search_layout.addWidget(self.le_search)

        search_layout.addWidget(QLabel(" 级别:"))
        self.cb_level = QComboBox()
        self.cb_level.addItems(["全部", "INFO", "WARNING", "ERROR", "DEBUG"])
        self.cb_level.currentTextChanged.connect(self._schedule_filter)
        search_layout.addWidget(self.cb_level)

        btn_clear = QPushButton("清空日志")
        btn_clear.clicked.connect(self.on_clear)
        search_layout.addStretch()
        search_layout.addWidget(btn_clear)
        layout.addLayout(search_layout)

        self.txt_logs = QPlainTextEdit()
        self.txt_logs.setReadOnly(True)
        self.txt_logs.setMaximumBlockCount(5000)  # 限制最大行数，防止内存无限增长
        # 用深色背景贴合开发者习惯
        self.txt_logs.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas, Courier New, monospace;")
        layout.addWidget(self.txt_logs)

        # Bug-2: 使用 deque 限制日志行数上限，防止内存泄漏
        self._all_log_lines = deque(maxlen=5000)

        # Risk-3: 防抖定时器
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(100)
        self._filter_timer.timeout.connect(self._apply_filter)

    def _hook_logger(self):
        self._ui_handler = UILogHandler(self.txt_logs)
        # 套用和系统 logger 相同的格式模板
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        self._ui_handler.setFormatter(formatter)
        self._ui_handler.setLevel(logging.INFO)
        global_logger.addHandler(self._ui_handler)

        global_logger.info("UI Log Capture Initialized.")

    def on_clear(self):
        self.txt_logs.clear()
        self._all_log_lines.clear()

    def _has_active_filter(self):
        """检查是否有激活的过滤条件"""
        return bool(self.le_search.text()) or self.cb_level.currentText() != "全部"

    def _schedule_filter(self, *args):
        """防抖触发过滤：100ms 内合并多次调用"""
        if self._filter_timer:
            self._filter_timer.start()

    def _apply_filter(self):
        """根据搜索关键词和级别过滤日志"""
        keyword = self.le_search.text().lower()
        level_filter = self.cb_level.currentText()

        self.txt_logs.clear()
        for line in self._all_log_lines:
            # 级别过滤
            if level_filter != "全部":
                if f"[{level_filter}]" not in line:
                    continue

            # 关键词过滤
            if keyword and keyword not in line.lower():
                continue

            self.txt_logs.appendPlainText(line)

    def closeEvent(self, event):
        """窗口关闭时移除 handler，防止泄漏"""
        self._cleanup()
        super().closeEvent(event)

    def _cleanup(self):
        """移除日志 handler"""
        if self._ui_handler is not None:
            global_logger.removeHandler(self._ui_handler)
            self._ui_handler = None
            global_logger.info("UI Log handler removed.")
