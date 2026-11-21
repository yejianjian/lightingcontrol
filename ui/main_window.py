try:
    from PyQt5.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit,
        QSystemTrayIcon, QMenu, QAction, QApplication
    )
    from PyQt5.QtCore import Qt, pyqtSignal, QTimer
    from PyQt5.QtGui import QIcon
except ImportError:
    from PySide6.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit,
        QSystemTrayIcon, QMenu, QAction, QApplication
    )
    from PySide6.QtCore import Qt, Signal as pyqtSignal, QTimer
    from PySide6.QtGui import QIcon

import asyncio
from datetime import datetime
from core.data_manager import DataManager
from utils.logger import global_logger
from utils.persistence import PersistenceManager
from utils.paths import get_resource_path
from ui.tabs.tab_monitor import TabMonitor
from ui.tabs.tab_group import TabGroup
from ui.tabs.tab_settings import TabSettings
from ui.tabs.tab_logs import TabLogs
from core.group_scheduler import GroupScheduler

class MainWindow(QMainWindow):
    # 协议/引擎插槽：默认 OPC UA；OPC DA 版本通过子类替换这两个类属性，
    # 其余页面（监控、分组、日志、调度）只依赖引擎的公共接口，完全复用。
    # 默认引擎惰性解析：UA 版在 __init__ 中按需导入 opc.client_engine，DA 版子类直接
    # 替换为 OpcDaClientEngine。这样 DA 打包时不会因为基类的顶层 import 把整条
    # asyncua/cryptography 依赖链拖进 exe（见 LightingControlDA.spec 的 excludes）。
    ENGINE_CLASS = None
    SETTINGS_CLASS = TabSettings

    # 最小窗口尺寸：必须低于 1024x768 屏幕的可用高度（减去任务栏约 40px），
    # 否则用户在全屏下也看不到底部内容（这是之前"界面太大"的直接原因）。
    MIN_WINDOW_WIDTH = 860
    MIN_WINDOW_HEIGHT = 520

    def _apply_adaptive_geometry(self):
        """按可用屏幕计算初始窗口尺寸，并设置不会撑破屏幕的最小尺寸。"""
        try:
            screen = QApplication.primaryScreen()
            avail = screen.availableGeometry() if screen else None
        except Exception:
            avail = None

        if avail is None or avail.width() <= 0 or avail.height() <= 0:
            self.resize(1100, 720)
            self.setMinimumSize(self.MIN_WINDOW_WIDTH, self.MIN_WINDOW_HEIGHT)
            return

        # 留出 20px 边距，避免紧贴屏幕边缘
        target_w = min(1100, max(640, avail.width() - 20))
        target_h = min(750, max(480, avail.height() - 20))
        self.resize(target_w, target_h)
        self.setMinimumSize(
            min(self.MIN_WINDOW_WIDTH, target_w),
            min(self.MIN_WINDOW_HEIGHT, target_h),
        )
        global_logger.info(
            f"Screen available {avail.width()}x{avail.height()}; window {target_w}x{target_h}, "
            f"minimum {self.minimumWidth()}x{self.minimumHeight()}"
        )

    def __init__(self):
        super().__init__()
        self.setWindowTitle("灯光自动控制系统 V1.4.0")

        # 依据可用屏幕自适应初始尺寸。
        # 现场既有 1366x768 也有 1024x768 甚至更小的老屏，固定 resize(1100,750)
        # 在 768 高的屏幕上会被任务栏挤掉底部内容。
        self._apply_adaptive_geometry()

        # 挂载我们生成的应用 Logo
        import os
        
        base_path = get_resource_path()
            
        logo_path = os.path.join(base_path, "lighting_logo.png")
        if os.path.exists(logo_path):
            self.setWindowIcon(QIcon(logo_path))

        # 核心数据与引擎
        self.pm = PersistenceManager()
        self.dm = DataManager(self.pm)
        # 引擎惰性解析：DA 版子类已把 ENGINE_CLASS 换成 OpcDaClientEngine，不会导入 UA；
        # UA 版这里才真正 import opc.client_engine，避免 DA 打包被基类顶层 import 拖累。
        engine_cls = self.ENGINE_CLASS
        if engine_cls is None:
            from opc.client_engine import OpcClientEngine
            engine_cls = OpcClientEngine
        self.opc_engine = engine_cls()  # 参数由设置页注入

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
        # 布局构建完成后再压低窗口的最小尺寸下限：内容的最小尺寸提示可能大于屏幕，
        # 这里显式放开，让用户能把窗口缩小到屏幕以内（配合各页的滚动区域）。
        self._relax_minimum_size()
        global_logger.info(
            f"MainWindow initialized. size={self.width()}x{self.height()}, "
            f"minimum={self.minimumWidth()}x{self.minimumHeight()}"
        )

        # 自动连接
        if self.pm.data_store.get("auto_connect", False):
            global_logger.info("Auto-connect enabled, initiating connection...")
            # 延时 500ms 确保 UI 完全初始化后再连接
            QTimer.singleShot(500, self._do_auto_connect)

        # 应用保存的主题
        if self.pm.data_store.get("dark_mode", False):
            QTimer.singleShot(100, lambda: self._apply_theme(True))

    def _relax_minimum_size(self):
        """把窗口最小尺寸压到不超过屏幕可用区域。

        Qt 会把"内容最小尺寸提示"合并进窗口的最小尺寸。当某个页面的控件天然较宽
        （例如成员穿梭表的筛选行）时，窗口最小尺寸会超过屏幕，用户即便全屏也看不到
        底部内容。这里用显式 setMinimumSize 覆盖该下限，并让中心控件可被压缩。
        """
        try:
            screen = QApplication.primaryScreen()
            avail = screen.availableGeometry() if screen else None
        except Exception:
            avail = None

        max_w = avail.width() - 16 if avail else self.MIN_WINDOW_WIDTH
        max_h = avail.height() - 16 if avail else self.MIN_WINDOW_HEIGHT
        min_w = max(640, min(self.MIN_WINDOW_WIDTH, max_w))
        min_h = max(480, min(self.MIN_WINDOW_HEIGHT, max_h))

        central = self.centralWidget()
        if central is not None:
            central.setMinimumSize(0, 0)
        if hasattr(self, 'tabs'):
            self.tabs.setMinimumSize(0, 0)
        self.setMinimumSize(min_w, min_h)

        # 若当前尺寸已超出屏幕，主动收缩
        if avail is not None:
            if self.width() > max_w or self.height() > max_h:
                self.resize(min(self.width(), max_w), min(self.height(), max_h))

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
        """执行自动连接。

        绝不能走 ``on_connect_clicked()``：那个槽把"处于 reconnecting 状态时的点击"
        解释为**用户想停止自动重连**。启动时自动重连总闸已经打开，500ms 后的这次
        自动连接会被当成用户按下了停止键，导致 `_auto_reconnect` 被关掉、复选框却
        还勾着 —— 之后真断线时只会停在"待连接"。
        """
        if hasattr(self, 'tab_settings'):
            self.tab_settings.start_auto_connect()

    def _apply_qss(self):
        import os
        base_path = get_resource_path()
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
        self.tab_settings = self.SETTINGS_CLASS(self.opc_engine, self.dm)
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
        # 兜底自愈：OPC 引擎的链路健康心跳/轮询任务如果静默退出（或僵尸 pending），
        # 由 UI 刷新定时器负责补拉它们。必须放在"无脏节点直接 return"之前，
        # 否则画面静止时（灯光点位长时间不变）这条兜底路径永远不会执行。
        ensure_liveness = getattr(self.opc_engine, "ensure_liveness", None)
        if callable(ensure_liveness):
            try:
                ensure_liveness()
            except Exception as e:
                # 兜底逻辑失败绝不能影响界面刷新
                global_logger.debug(f"ensure_liveness() failed (non-fatal): {e}")

        # 调度器看门狗：调度守护协程如果静默退出/变成僵尸 pending，定时任务会
        # 永远不再触发（界面与连接都正常，极难排查）。UI 刷新定时器负责兜底重启。
        ensure_scheduler = getattr(getattr(self, "scheduler", None), "ensure_running", None)
        if callable(ensure_scheduler):
            try:
                ensure_scheduler()
            except Exception as e:
                global_logger.debug(f"scheduler.ensure_running() failed (non-fatal): {e}")

        # 心跳存活状态上屏：心跳成功日志是 DEBUG 级，默认日志页看不到；
        # 设置页链路状态需要实时显示"心跳 Xs 前"，便于现场确认检测在运行。
        update_heartbeat = getattr(getattr(self, "tab_settings", None),
                                   "update_heartbeat_status", None)
        if callable(update_heartbeat):
            try:
                update_heartbeat()
            except Exception as e:
                global_logger.debug(f"update_heartbeat_status() failed (non-fatal): {e}")

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
            # 结构变化（设置页手动增删点位 / 重连后重建节点表）时，分组页已选分组的
            # 成员快照同样过期，必须一起重取，否则用户会基于旧列表增删成员。
            current_group = None
            try:
                current_group = self.tab_group.tree_groups.currentItem()
            except Exception as e:
                global_logger.debug(f"read tab_group current item failed (non-fatal): {e}")
            if current_group is not None:
                try:
                    self.tab_group.refresh_members()
                except Exception as e:
                    global_logger.debug(f"tab_group.refresh_members() failed (non-fatal): {e}")
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
        base_path = get_resource_path()
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
            # 停止设置页后台任务：服务器枚举线程解绑、手动点位任务取消。
            # QTabWidget 子页面收不到主窗口 closeEvent，必须由顶层显式触发，
            # 否则退出瞬间的回调可能访问已销毁的控件。
            shutdown_bg = getattr(getattr(self, "tab_settings", None),
                                  "shutdown_background_tasks", None)
            if callable(shutdown_bg):
                try:
                    shutdown_bg()
                except Exception as e:
                    global_logger.debug("shutdown_background_tasks() failed (non-fatal): {}".format(e))
            # 停止调度器
            if hasattr(self, 'scheduler'):
                self.scheduler.stop()
            # 关闭自动重连总闸：否则关闭流程中 supervisor 可能又把连接拉起来
            try:
                self.opc_engine.stop_reconnect()
            except Exception as e:
                global_logger.warning(f"Error stopping reconnect supervisor: {e}")
            # 断开 OPC 连接 — 使用 ensure_future 避免 run_until_complete 死锁。
            # 无论当前是否"已连接"都要走一遍：断开是幂等的，且重连中/加载中同样需要收尾。
            try:
                disconnect_future = asyncio.ensure_future(self.opc_engine.disconnect())
                self._shutdown_disconnect_future = disconnect_future

                def _on_disconnect_done(fut):
                    try:
                        if not fut.cancelled():
                            exc = fut.exception()
                            if exc is not None:
                                global_logger.warning(f"Disconnect during shutdown raised: {exc}")
                    except Exception:
                        pass
                    self._stop_event_loop()

                disconnect_future.add_done_callback(_on_disconnect_done)
                # 兜底：即使 disconnect 因底层驱动异常没能返回，也在 5 秒后强制收尾
                self._shutdown_watchdog = QTimer(self)
                self._shutdown_watchdog.setSingleShot(True)
                self._shutdown_watchdog.timeout.connect(self._stop_event_loop)
                self._shutdown_watchdog.start(5000)
            except Exception as e:
                global_logger.warning(f"Error during shutdown disconnect: {e}")
                # 断连失败时仍需停止事件循环
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
