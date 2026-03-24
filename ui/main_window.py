try:
    from PyQt5.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit,
        QSystemTrayIcon, QMenu, QAction
    )
    from PyQt5.QtCore import Qt, pyqtSignal, QTimer
    from PyQt5.QtGui import QIcon
except ImportError:
    from PySide6.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit,
        QSystemTrayIcon, QMenu, QAction
    )
    from PySide6.QtCore import Qt, Signal as pyqtSignal, QTimer
    from PySide6.QtGui import QIcon

import asyncio
from datetime import datetime
from opc.client_engine import OpcClientEngine
from core.data_manager import DataManager
from utils.logger import global_logger
from utils.persistence import PersistenceManager
from utils.paths import get_base_path
from ui.tabs.tab_monitor import TabMonitor
from ui.tabs.tab_group import TabGroup
from ui.tabs.tab_settings import TabSettings
from ui.tabs.tab_logs import TabLogs
from core.group_scheduler import GroupScheduler

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("灯光自动控制系统 V1.2.3beta")
        # 宽屏比例适配
        self.resize(1100, 750)
        
        # 挂载我们生成的应用 Logo
        import os
        
        base_path = get_base_path()
            
        logo_path = os.path.join(base_path, "lighting_logo.png")
        if os.path.exists(logo_path):
            self.setWindowIcon(QIcon(logo_path))

        # 核心数据与引擎
        self.pm = PersistenceManager()
        self.dm = DataManager(self.pm)
        self.opc_engine = OpcClientEngine()  # 参数由设置页注入

        # 检查配置文件是否损坏
        if self.pm.was_load_corrupted():
            QMessageBox.warning(self, "配置警告",
                "配置文件已损坏，已使用默认空配置。请检查 data/lighting_config.json 和备份文件。\n"
                "部分数据可能已丢失，建议重新配置。")

        # 调度器挂载但不立即启动（等 OPC 连接成功后再启动）
        self.scheduler = GroupScheduler(self.dm, self.opc_engine)
        
        self._setup_ui()
        self._apply_qss()
        self._bind_events()
        global_logger.info("MainWindow initialized with QTabWidget architecture.")

        # 自动连接
        if self.pm.data_store.get("auto_connect", False):
            global_logger.info("Auto-connect enabled, initiating connection...")
            # 延时 500ms 确保 UI 完全初始化后再连接
            QTimer.singleShot(500, self._do_auto_connect)

        # 应用保存的主题
        if self.pm.data_store.get("dark_mode", False):
            QTimer.singleShot(100, lambda: self._apply_theme(True))

    def _apply_theme(self, dark_mode):
        """应用主题到主窗口"""
        if not hasattr(self, '_original_qss'):
            return

        if dark_mode:
            # 深色模式 - 在原始样式基础上追加深色覆盖样式
            qss = self._original_qss + """
/* 深色模式覆盖 */
QMainWindow { background-color: #1e1e1e; }
QWidget { background-color: #1e1e1e; color: #e0e0e0; }
QLabel { background-color: transparent; }
QCheckBox { background-color: transparent; }
QRadioButton { background-color: transparent; }
QGroupBox { background-color: transparent; border: 1px solid #3d3d3d; border-radius: 6px; margin-top: 2ex; }
QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px; color: #888888; background-color: transparent; }
QFrame#DashboardCard { background-color: #2d2d2d; border: 1px solid #4d4d4d; border-radius: 12px; }
QLabel#GreetingLabel { color: #e0e0e0; }
QPushButton { background-color: #3d3d3d; color: #e0e0e0; border: 1px solid #4d4d4d; border-radius: 6px; padding: 6px 16px; font-size: 13px; font-weight: 600; }
QPushButton:hover { background-color: #4d4d4d; border: 1px solid #666666; }
QPushButton:pressed { background-color: #5d5d5d; }
QPushButton:disabled { background-color: #1e1e1e; color: #555555; border: 1px solid #333333; }
QLineEdit, QComboBox, QSpinBox { background-color: #2d2d2d; color: #e0e0e0; border: 1px solid #4d4d4d; }
QLineEdit:focus, QComboBox:focus, QSpinBox:focus { border: 1px solid #3b82f6; background-color: #3d3d3d; }
QTableView, QTreeView { background-color: #2d2d2d; color: #e0e0e0; border: 1px solid #3d3d3d; gridline-color: #3d3d3d; alternate-background-color: #252525; }
QHeaderView::section { background-color: #252525; color: #888888; border-bottom: 1px solid #3d3d3d; }
QTableCornerButton::section { background-color: #252525; border: 1px solid #3d3d3d; }
QLabel#DashValueLabel { color: #e0e0e0; }
QLabel#DashTitleLabel { color: #888888; }
QLabel#BottomClock { color: #888888; }
QPlainTextEdit { background-color: #1e1e1e; color: #d4d4d4; border: 1px solid #3d3d3d; }
QScrollBar:vertical { background: #252525; }
QScrollBar::handle:vertical { background: #4d4d4d; }
QScrollBar:horizontal { background: #252525; }
QScrollBar::handle:horizontal { background: #4d4d4d; }
QTabBar::tab { background-color: transparent; color: #888888; }
QTabBar::tab:selected { color: #3b82f6; }
QTabBar::tab:hover:!selected { color: #e0e0e0; }
QComboBox QAbstractItemView { background-color: #2d2d2d; color: #e0e0e0; border: 1px solid #4d4d4d; selection-background-color: #4d4d4d; selection-color: #e0e0e0; outline: none; }
"""
            # 更新 Dashboard 特殊部件颜色
            if hasattr(self, 'card_dash_total'):
                self.card_dash_total.value_label.setStyleSheet("color: #e0e0e0;")
            if hasattr(self, 'card_dash_off'):
                self.card_dash_off.value_label.setStyleSheet("color: #94a3b8;")
        else:
            # 浅色模式 - 使用原始样式
            qss = self._original_qss
            # 恢复 Dashboard 特殊部件颜色
            if hasattr(self, 'card_dash_total'):
                self.card_dash_total.value_label.setStyleSheet("")
            if hasattr(self, 'card_dash_off'):
                self.card_dash_off.value_label.setStyleSheet("color: #64748b;")

        self.setStyleSheet(qss)

    def _do_auto_connect(self):
        """执行自动连接"""
        if hasattr(self, 'tab_settings'):
            self.tab_settings.on_connect_clicked()

    def _apply_qss(self):
        import os
        base_path = get_base_path()
        qss_path = os.path.join(base_path, "ui", "style.qss")
        self._original_qss = ""
        if os.path.exists(qss_path):
            # 使用 errors='replace' 处理编码问题
            try:
                with open(qss_path, "r", encoding="utf-8", errors="replace") as f:
                    self._original_qss = f.read()
            except Exception:
                pass
        else:
            global_logger.warning(f"QSS 样式文件不存在: {qss_path}，界面将使用默认样式")
        self.setStyleSheet(self._original_qss)

    def _setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        # ==========================================
        # 顶部：Header
        # ==========================================
        header_layout = QHBoxLayout()
        username = self.pm.data_store.get("username", "Admin")
        lbl_greeting = QLabel(f"你好，{username} 👋")
        lbl_greeting.setObjectName("GreetingLabel")
        lbl_greeting.setStyleSheet("font-size: 24px; font-weight: bold;")
        header_layout.addWidget(lbl_greeting)
        header_layout.addStretch()
        main_layout.addLayout(header_layout)

        # ==========================================
        # 仪表盘 Dashboard
        # ==========================================
        dash_layout = QHBoxLayout()
        
        self.card_dash_total = self._create_dash_panel("总点位数", "0")
        self.card_dash_on = self._create_dash_panel("当前开启", "0", value_color="#10b981")
        self.card_dash_off = self._create_dash_panel("当前关闭", "0", value_color="#64748b")
        self.card_dash_mode = self._create_dash_panel("系统状态", "待连接", value_color="#f59e0b")

        dash_layout.addWidget(self.card_dash_total, 1)
        dash_layout.addWidget(self.card_dash_on, 1)
        dash_layout.addWidget(self.card_dash_off, 1)
        dash_layout.addWidget(self.card_dash_mode, 1)

        main_layout.addLayout(dash_layout)

        # ==========================================
        # 中间偏下：核心内容区 Tabs
        # ==========================================
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        
        # 1. 监控与控制
        self.tab_monitor = TabMonitor(self.dm, self.opc_engine)
        self.tabs.addTab(self.tab_monitor, "监控与控制")

        # 2. 分组与调度
        self.tab_group = TabGroup(self.dm, self.opc_engine)
        self.tabs.addTab(self.tab_group, "分组与调度")

        # 3. 设置与连接
        self.tab_settings = TabSettings(self.opc_engine, self.dm)
        self.tabs.addTab(self.tab_settings, "设置")

        # 4. 实时日志
        self.tab_logs = TabLogs()
        self.tabs.addTab(self.tab_logs, "实时日志")

        main_layout.addWidget(self.tabs)

        # 底部实时时钟
        self.lbl_clock = QLabel()
        self.lbl_clock.setAlignment(Qt.AlignCenter)
        self.lbl_clock.setObjectName("BottomClock")
        main_layout.addWidget(self.lbl_clock)
        self._update_clock()  # 立即显示一次

        # 系统托盘图标
        self._create_tray_icon()

    def _update_clock(self):
        now = datetime.now()
        weekdays = ["星期一", "星期二", "星期三", "星期四", "星期五", "星期六", "星期日"]
        weekday_str = weekdays[now.weekday()]
        self.lbl_clock.setText(f"🕒 当前时间: {now.strftime('%Y-%m-%d %H:%M:%S')} {weekday_str}")

    def _create_dash_panel(self, title, init_value, title_color="#64748b", value_color="#0f172a"):
        card = QFrame()
        card.setObjectName("DashboardCard")
        
        layout = QVBoxLayout(card)
        layout.setContentsMargins(20, 16, 20, 16)
        layout.setSpacing(4)
        
        lbl_title = QLabel(title)
        lbl_title.setObjectName("DashTitleLabel")
        
        lbl_value = QLabel(init_value)
        lbl_value.setObjectName("DashValueLabel")
        if value_color != "#0f172a":
            lbl_value.setStyleSheet(f"color: {value_color};")
            
        layout.addWidget(lbl_title)
        layout.addWidget(lbl_value)
        
        card.value_label = lbl_value
        return card

    def _bind_events(self):
        # 绑定定时器处理UI的防抖/批处理刷新，解决单点高频上报造成的重绘卡顿
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self._on_refresh_timer)
        # 从配置读取刷新间隔，默认500ms
        refresh_interval = self.pm.data_store.get("refresh_interval", 500)
        self.refresh_timer.start(refresh_interval)

        # 每秒别时钟
        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)

    def _on_refresh_timer(self):
        # 原子地获取脏节点，避免与订阅回调产生竞态
        dirty_nodes = self.dm.get_dirty_nodes_and_clear()
        if not dirty_nodes and not self.dm.structure_changed:
            return

        # 获取仪表盘统计数据 (直接读取 O(1) 计数器)
        total = len(self.dm.nodes)

        self.card_dash_total.value_label.setText(str(total))
        self.card_dash_on.value_label.setText(str(self.dm.on_count))
        self.card_dash_off.value_label.setText(str(self.dm.off_count))

        # 判断是对可视区域做增量渲染，还是由于结构改变做了整体重新构建
        if self.dm.structure_changed:
            self.tab_monitor.on_refresh_clicked()
            self.dm.structure_changed = False
        elif dirty_nodes:
            # 委托子界面进行精确的行变动画重绘
            if hasattr(self.tab_monitor.model, 'update_nodes'):
                self.tab_monitor.model.update_nodes(dirty_nodes)
            if hasattr(self.tab_group.model_in, 'update_nodes'):
                self.tab_group.model_in.update_nodes(dirty_nodes)
            if hasattr(self.tab_group.model_out, 'update_nodes'):
                self.tab_group.model_out.update_nodes(dirty_nodes)

    def _create_tray_icon(self):
        """创建系统托盘图标和菜单"""
        import os
        base_path = get_base_path()
        icon_path = os.path.join(base_path, "lighting_logo.png")

        self.tray_icon = QSystemTrayIcon(self)
        if os.path.exists(icon_path):
            self.tray_icon.setIcon(QIcon(icon_path))

        self.tray_icon.setToolTip("灯光自动控制系统")

        # 创建托盘菜单
        tray_menu = QMenu()

        self._tray_show_action = QAction("显示主窗口", self)
        self._tray_show_action.triggered.connect(self._tray_show_window)
        tray_menu.addAction(self._tray_show_action)

        tray_menu.addSeparator()

        self._tray_exit_action = QAction("退出程序", self)
        self._tray_exit_action.triggered.connect(self._tray_exit)
        tray_menu.addAction(self._tray_exit_action)

        self.tray_icon.setContextMenu(tray_menu)

        # 点击托盘图标显示/隐藏窗口
        self.tray_icon.activated.connect(self._tray_on_activated)

        self.tray_icon.show()
        global_logger.info("System tray icon initialized.")

    def _tray_on_activated(self, reason):
        """托盘图标被激活"""
        if reason == QSystemTrayIcon.Trigger:
            self._tray_toggle_window()

    def _tray_toggle_window(self):
        """切换窗口显示/隐藏"""
        if self.isVisible():
            self.hide()
            self._tray_show_action.setText("显示主窗口")
        else:
            self._tray_show_window()

    def _tray_show_window(self):
        """显示主窗口"""
        self.show()
        self.activateWindow()
        self._tray_show_action.setText("最小化到托盘")

    def _tray_exit(self):
        """从托盘退出程序"""
        self._force_close = True
        self.close()

    def closeEvent(self, event):
        # 检查是否设置了"关闭到托盘"
        close_to_tray = self.pm.data_store.get("close_to_tray", False)

        if close_to_tray and not getattr(self, '_force_close', False):
            # 最小化到托盘而不是退出
            self.hide()
            self._tray_show_action.setText("显示主窗口")
            event.ignore()
            return

        # 鉴权弹窗，阻止误关（使用顶层已导入的 QInputDialog/QLineEdit）
        text, ok = QInputDialog.getText(self, "安全锁", "正在尝试退出集控系统。\n请输入退出授权密码:", QLineEdit.Password)

        exit_password = self.pm.data_store.get("exit_password", "8888")
        if ok and text == exit_password:
            # 停止所有定时器
            self.refresh_timer.stop()
            self.clock_timer.stop()
            # 停止调度器
            if hasattr(self, 'scheduler'):
                self.scheduler.stop()
            # 断开 OPC 连接 — 使用 ensure_future 避免 run_until_complete 死锁
            if self.opc_engine.connected:
                try:
                    disconnect_future = asyncio.ensure_future(self.opc_engine.disconnect())
                    # 断连完成后再停止事件循环
                    def _on_disconnect_done(fut):
                        try:
                            loop = asyncio.get_event_loop()
                            loop.stop()
                        except Exception:
                            pass
                    disconnect_future.add_done_callback(_on_disconnect_done)
                except Exception as e:
                    global_logger.warning(f"Error during shutdown disconnect: {e}")
                    # 断连失败时仍需停止事件循环
                    QTimer.singleShot(500, lambda: self._stop_event_loop())
            else:
                # 未连接时直接停止事件循环
                QTimer.singleShot(500, lambda: self._stop_event_loop())

            event.accept()
            global_logger.info("Application shutdown by user.")
        else:
            if ok:
                 QMessageBox.critical(self, "错误", "密码错误，无法退出。")
            event.ignore()

    def _stop_event_loop(self):
        """安全停止事件循环"""
        try:
            loop = asyncio.get_event_loop()
            loop.stop()
        except Exception:
            pass
