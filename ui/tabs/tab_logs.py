try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout, QLineEdit, QLabel, QComboBox
    from PyQt5.QtCore import QObject, pyqtSignal, Qt, QTimer
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QPushButton, QHBoxLayout, QLineEdit, QLabel, QComboBox
    from PySide6.QtCore import QObject, Signal as pyqtSignal, Qt, QTimer

import logging
from collections import deque
from utils.logger import global_logger
from utils.logger import MAX_LOG_MESSAGE_CHARS, LibraryNoiseFilter, set_verbose_library_logging

# GUI 侧单条日志的显示上限：即便上游漏过大报文，也绝不让 QPlainTextEdit 在
# 事件循环线程上排版几百 KB 的文本。
UI_MAX_MESSAGE_CHARS = 1000
# 批量刷新间隔（毫秒）：日志高频到达时合并成一次重绘
UI_FLUSH_INTERVAL_MS = 150
# 单次刷新最多追加的行数，避免积压后一次性卡住界面
UI_MAX_LINES_PER_FLUSH = 200


class _LogBridge(QObject):
    """信号桥：将跨线程的日志消息安全地转发到 Qt 主线程"""
    log_record = pyqtSignal(int, str, str)  # levelno, levelname, formatted_msg


class UILogHandler(logging.Handler):
    """
    一个日志拦截句柄，将原本打在控制台和文件里的日志分流输送到指定的UI文本框。
    使用信号槽机制确保线程安全（兼容 PyQt5 和 PySide6）。
    """
    def __init__(self, tab_logs):
        super().__init__()
        self._tab_logs = tab_logs
        self._bridge = _LogBridge()
        self._bridge.log_record.connect(self._on_log_record)
        # 与文件/控制台 handler 使用同一套噪声过滤，避免把 asyncua 的整报文 dump
        # 送进 GUI 线程逐个排版
        self.addFilter(LibraryNoiseFilter())

    def _on_log_record(self, levelno, levelname, msg):
        if self._tab_logs and hasattr(self._tab_logs, 'handle_incoming_log'):
            self._tab_logs.handle_incoming_log(levelno, levelname, msg)

    def emit(self, record):
        try:
            msg = self.format(record)
            if len(msg) > UI_MAX_MESSAGE_CHARS:
                msg = msg[:UI_MAX_MESSAGE_CHARS] + f"…[truncated {len(msg) - UI_MAX_MESSAGE_CHARS} chars]"
            self._bridge.log_record.emit(record.levelno, record.levelname, msg)
        except RuntimeError:
            # widget 已销毁，静默忽略
            pass
        except Exception:
            pass


class TabLogs(QWidget):
    def __init__(self):
        super().__init__()
        self._ui_handler = None  # 保存 handler 引用，销毁时移除
        self._filter_timer = None  # 防抖定时器
        self._flush_timer = None  # 日志批量上屏定时器
        self._pending_lines = deque()  # 待上屏的日志行
        self._all_log_records = deque(maxlen=5000)  # 存储 (levelno, levelname, msg)
        self._setup_ui()
        self._hook_logger()

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(2, 4, 2, 2)
        layout.setSpacing(4)

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
        self.cb_level.currentTextChanged.connect(self._on_level_changed)
        search_layout.addWidget(self.cb_level)

        btn_clear = QPushButton("清空日志")
        btn_clear.clicked.connect(self.on_clear)
        search_layout.addStretch()
        search_layout.addWidget(btn_clear)
        layout.addLayout(search_layout)

        # 心跳等调试日志按现场要求不进入 INFO 分类；这里给出可发现的提示，
        # 避免值守人员误以为"没有心跳检测"。
        self.lbl_level_hint = QLabel(
            "提示：心跳/调试日志为 DEBUG 级，查看请把上方级别切换为 DEBUG"
            "（约 60 秒一条）；设置页「链路状态」会实时显示心跳时间。")
        self.lbl_level_hint.setStyleSheet("color: #64748b; font-size: 12px;")
        self.lbl_level_hint.setWordWrap(True)
        layout.addWidget(self.lbl_level_hint)

        self.txt_logs = QPlainTextEdit()
        self.txt_logs.setReadOnly(True)
        self.txt_logs.setMaximumBlockCount(5000)  # 限制最大行数，防止内存无限增长
        # 用深色背景贴合开发者习惯
        self.txt_logs.setStyleSheet("background-color: #1e1e1e; color: #d4d4d4; font-family: Consolas, Courier New, monospace;")
        layout.addWidget(self.txt_logs)

        # 防抖定时器
        self._filter_timer = QTimer(self)
        self._filter_timer.setSingleShot(True)
        self._filter_timer.setInterval(80)
        self._filter_timer.timeout.connect(self._apply_filter)

        # 批量上屏定时器：日志到达时只入队，由定时器合并刷新。
        # 断线重连或批量下发期间每秒可能产生成百上千条记录，逐条 appendPlainText
        # 会把事件循环线程堵住，进而拖慢心跳读取并造成误判断线。
        self._flush_timer = QTimer(self)
        self._flush_timer.setSingleShot(True)
        self._flush_timer.setInterval(UI_FLUSH_INTERVAL_MS)
        self._flush_timer.timeout.connect(self._flush_pending)

    def _hook_logger(self):
        # 传递 self (TabLogs) 给 UILogHandler，确保缓存与过滤状态正确对接
        self._ui_handler = UILogHandler(self)
        formatter = logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s')
        self._ui_handler.setFormatter(formatter)
        self._ui_handler.setLevel(logging.DEBUG)
        global_logger.addHandler(self._ui_handler)

        # 同时将 UI 日志句柄挂载到 asyncua 第三方库的 logger，确保底层通道协商与续约记录实时展示
        try:
            asyncua_logger = logging.getLogger('asyncua')
            if self._ui_handler not in asyncua_logger.handlers:
                asyncua_logger.addHandler(self._ui_handler)
        except Exception:
            pass

        global_logger.info("UI Log Capture Initialized.")

    def handle_incoming_log(self, levelno, levelname, msg):
        """接收新到达的日志记录：只入队，由 _flush_pending 合并上屏。"""
        self._all_log_records.append((levelno, levelname, msg))
        if self._matches_filter(levelno, levelname, msg):
            self._pending_lines.append(msg)
            # 队列过长时丢弃最旧的，宁可少几行也不能让内存/延迟无限增长
            while len(self._pending_lines) > 1000:
                self._pending_lines.popleft()
            if self._flush_timer is not None and not self._flush_timer.isActive():
                self._flush_timer.start()

    def _flush_pending(self):
        """把待上屏日志一次性追加到文本框（带上限，避免长积压造成卡顿）。"""
        if not self._pending_lines:
            return
        batch = []
        for _ in range(min(len(self._pending_lines), UI_MAX_LINES_PER_FLUSH)):
            batch.append(self._pending_lines.popleft())
        self.txt_logs.setUpdatesEnabled(False)
        try:
            self.txt_logs.appendPlainText("\n".join(batch))
        finally:
            self.txt_logs.setUpdatesEnabled(True)
        if self._pending_lines and self._flush_timer is not None:
            self._flush_timer.start()

    def _on_level_changed(self, text):
        """级别下拉框改变时动态调整日志输出级别并触发过滤重绘。

        DEBUG 只对**本应用**放开。放开第三方库的 DEBUG 会让 asyncua 把每次
        Browse/Read/Publish 的完整报文写成单条几百 KB 的记录，日志页与 GUI 线程都
        会被拖垮，所以 asyncua 固定保持在 INFO 以上（INFO 仍受噪声过滤保护）。
        """
        if text == "DEBUG":
            global_logger.setLevel(logging.DEBUG)
            set_verbose_library_logging(True)
        else:
            global_logger.setLevel(logging.INFO)
            set_verbose_library_logging(False)
        self._schedule_filter()

    def on_clear(self):
        self.txt_logs.clear()
        self._all_log_records.clear()
        self._pending_lines.clear()

    def _matches_filter(self, levelno, levelname, msg):
        """判断单条日志是否满足当前的级别与搜索关键词条件"""
        level_filter = self.cb_level.currentText()
        if level_filter != "全部":
            if level_filter != levelname and f"[{level_filter}]" not in msg:
                return False

        keyword = self.le_search.text().strip().lower()
        if keyword and keyword not in msg.lower():
            return False

        return True

    def _schedule_filter(self, *args):
        """防抖触发过滤：80ms 内合并多次调用"""
        if self._filter_timer:
            self._filter_timer.start()

    def _apply_filter(self):
        """根据搜索关键词和级别完整重构日志文本框"""
        self._pending_lines.clear()
        lines = [msg for _levelno, levelname, msg in self._all_log_records
                 if self._matches_filter(_levelno, levelname, msg)]
        self.txt_logs.setUpdatesEnabled(False)
        try:
            self.txt_logs.setPlainText("\n".join(lines))
        finally:
            self.txt_logs.setUpdatesEnabled(True)

        # 滚动条跟随到最新位置
        sb = self.txt_logs.verticalScrollBar()
        if sb:
            sb.setValue(sb.maximum())

    def closeEvent(self, event):
        """窗口关闭时移除 handler，防止泄漏"""
        self._cleanup()
        super().closeEvent(event)

    def _cleanup(self):
        """移除日志 handler"""
        if hasattr(self, '_flush_timer') and self._flush_timer is not None:
            self._flush_timer.stop()
        if hasattr(self, '_pending_lines'):
            self._pending_lines.clear()
        if self._ui_handler is not None:
            self._ui_handler._tab_logs = None
            global_logger.removeHandler(self._ui_handler)
            try:
                logging.getLogger('asyncua').removeHandler(self._ui_handler)
            except Exception:
                pass
            self._ui_handler = None
            global_logger.info("UI Log handler removed.")
