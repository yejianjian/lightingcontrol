try:
    from PyQt5.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit
    )
    from PyQt5.QtCore import Qt, pyqtSignal, QTimer
    from PyQt5.QtGui import QIcon
except ImportError:
    from PySide6.QtWidgets import (
        QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QTabWidget, QLabel, QFrame, QMessageBox, QInputDialog, QLineEdit
    )
    from PySide6.QtCore import Qt, Signal as pyqtSignal, QTimer
    from PySide6.QtGui import QIcon

import asyncio
from datetime import datetime
from opc.client_engine import OpcClientEngine
from core.data_manager import DataManager
from utils.logger import global_logger
from utils.persistence import PersistenceManager
from ui.tabs.tab_monitor import TabMonitor
from ui.tabs.tab_group import TabGroup
from ui.tabs.tab_settings import TabSettings
from ui.tabs.tab_logs import TabLogs
from core.group_scheduler import GroupScheduler

class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("灯光自动控制系统 - Central Lighting Control (by jianjian)")
        # 宽屏比例适配
        self.resize(1100, 750)
        
        # 挂载我们生成的应用 Logo
        import os
        import sys
        
        # 兼容 PyInstaller 打包后的临时目录路径
        if getattr(sys, 'frozen', False):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.dirname(os.path.dirname(__file__))
            
        logo_path = os.path.join(base_path, "lighting_logo.png")
        if os.path.exists(logo_path):
            self.setWindowIcon(QIcon(logo_path))

        # 核心数据与引擎
        self.pm = PersistenceManager()
        self.dm = DataManager(self.pm)
        self.opc_engine = OpcClientEngine()  # 参数由设置页注入
        
        # 调度器挂载但不立即启动（等 OPC 连接成功后再启动）
        self.scheduler = GroupScheduler(self.dm, self.opc_engine)
        
        self._setup_ui()
        self._bind_events()
        global_logger.info("MainWindow initialized with QTabWidget architecture.")

    def _setup_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)

        # ==========================================
        # 顶部：状态仪表盘 Dashboard
        # ==========================================
        dash_layout = QHBoxLayout()
        
        self.lbl_dash_total = self._create_dash_panel("总点位数: 0")
        self.lbl_dash_on = self._create_dash_panel("开启: 0", color="green")
        self.lbl_dash_off = self._create_dash_panel("关闭: 0", color="black")
        
        self.lbl_dash_mode = self._create_dash_panel("系统状态 (待连接)", color="gray", bg_color="#E0E0E0")

        dash_layout.addWidget(self.lbl_dash_total, 1)
        dash_layout.addWidget(self.lbl_dash_on, 1)
        dash_layout.addWidget(self.lbl_dash_off, 1)
        dash_layout.addWidget(self.lbl_dash_mode, 2)

        main_layout.addLayout(dash_layout)

        # ==========================================
        # 中间偏下：核心内容区 Tabs
        # ==========================================
        self.tabs = QTabWidget()
        self.tabs.setDocumentMode(True)
        
        # 1. 监控与控制
        self.tab_monitor = TabMonitor(self.dm, self.opc_engine)
        self.tabs.addTab(self.tab_monitor, "🕹️ 监控与控制")
        
        # 2. 分组与调度
        self.tab_group = TabGroup(self.dm, self.opc_engine) 
        self.tabs.addTab(self.tab_group, "📁 分组与调度")
        
        # 3. 设置与连接
        self.tab_settings = TabSettings(self.opc_engine, self.dm)
        self.tabs.addTab(self.tab_settings, "⚙️ 设置")
        
        # 4. 实时日志
        self.tab_logs = TabLogs()
        self.tabs.addTab(self.tab_logs, "📄 实时日志")

        main_layout.addWidget(self.tabs)

        # 底部实时时钟
        self.lbl_clock = QLabel()
        self.lbl_clock.setAlignment(Qt.AlignCenter)
        self.lbl_clock.setStyleSheet("background-color: #1e293b; color: #94a3b8; padding: 4px; font-size: 13px; border-radius: 3px;")
        main_layout.addWidget(self.lbl_clock)
        self._update_clock()  # 立即显示一次

    def _update_clock(self):
        self.lbl_clock.setText(f"🕒 当前时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _create_dash_panel(self, text, color="black", bg_color="white"):
        lbl = QLabel(text)
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setStyleSheet(f"""
            QLabel {{
                background-color: {bg_color};
                color: {color};
                border: 1px solid #CCCCCC;
                border-radius: 4px;
                padding: 10px;
                font-weight: bold;
                font-size: 14px;
            }}
        """)
        return lbl

    def _bind_events(self):
        # 绑定定时器处理UI的防抖/批处理刷新，解决单点高频上报造成的重绘卡顿
        self.refresh_timer = QTimer(self)
        self.refresh_timer.timeout.connect(self._on_refresh_timer)
        self.refresh_timer.start(500)

        # 每秒别时钟
        self.clock_timer = QTimer(self)
        self.clock_timer.timeout.connect(self._update_clock)
        self.clock_timer.start(1000)

    def _on_refresh_timer(self):
        if not hasattr(self.dm, 'dirty_nodes') or (not self.dm.dirty_nodes and getattr(self.dm, 'structure_changed', False) == False):
            return

        # 获取仪表盘统计数据
        total = len(self.dm.nodes)
        on_count = 0
        off_count = 0
        for n in self.dm.nodes.values():
            val = n.get('value')
            if val is True:
                on_count += 1
            elif val is False:
                off_count += 1

        self.lbl_dash_total.setText(f"总点位数: {total}")
        self.lbl_dash_on.setText(f"开启: {on_count}")
        self.lbl_dash_off.setText(f"关闭: {off_count}")
        
        # 判断是对可视区域做增量渲染，还是由于结构改变做了整体重新构建
        if getattr(self.dm, 'structure_changed', False):
            self.tab_monitor.on_refresh_clicked()
            self.dm.structure_changed = False
        elif hasattr(self.dm, 'dirty_nodes') and self.dm.dirty_nodes:
            # 委托子界面进行精确的行变动画重绘
            if hasattr(self.tab_monitor.model, 'update_nodes'):
                self.tab_monitor.model.update_nodes(self.dm.dirty_nodes)
                
        self.dm.dirty_nodes.clear()

    def closeEvent(self, event):
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
            # 断开 OPC 连接并等待完成
            if self.opc_engine.connected:
                try:
                    loop = asyncio.get_event_loop()
                    loop.run_until_complete(self.opc_engine.disconnect())
                except Exception as e:
                    global_logger.warning(f"Error during shutdown disconnect: {e}")
            event.accept()
            global_logger.info("Application shutdown by user.")
        else:
            if ok:
                 QMessageBox.critical(self, "错误", "密码错误，无法退出。")
            event.ignore()
