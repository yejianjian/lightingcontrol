try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox, QSpinBox, QCheckBox, QProgressDialog
    from PyQt5.QtCore import QTimer, Qt
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox, QSpinBox, QCheckBox, QProgressDialog
    from PySide6.QtCore import QTimer, Qt

import asyncio
import re
import base64
from utils.logger import global_logger

class TabSettings(QWidget):
    def __init__(self, opc_engine, data_manager):
        super().__init__()
        self.engine = opc_engine
        self.engine.on_connection_lost = self._handle_connection_lost
        self.dm = data_manager
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # OPC 服务联机配置区
        grp_conn = QGroupBox("OPC UA 服务器连接配置")
        form_conn = QFormLayout(grp_conn)

        # 从持久化配置中读取上次保存的连接参数
        saved_conn = self.dm.pm.data_store.get("connection", {})
        self.le_host = QLineEdit(saved_conn.get("host", "127.0.0.1"))
        self.le_port = QLineEdit(saved_conn.get("port", "48401"))
        self.le_user = QLineEdit(saved_conn.get("user", ""))
        self.le_pass = QLineEdit(self._decode_password(saved_conn.get("password", "")))
        self.le_pass.setEchoMode(QLineEdit.Password)
        self.le_ns_filter = QLineEdit(saved_conn.get("ns_filter", "ns=2;"))
        self.le_ns_filter.setToolTip("业务节点命名空间前缀，留空则不过滤")

        form_conn.addRow("DA 主机 IP (Host):", self.le_host)
        form_conn.addRow("服务端口 (Port):", self.le_port)
        form_conn.addRow("验证账户 (User):", self.le_user)
        form_conn.addRow("验证密码 (Password):", self.le_pass)
        form_conn.addRow("节点命名空间前缀:", self.le_ns_filter)

        # 按钮容器
        btn_layout = QHBoxLayout()
        self.btn_connect = QPushButton("连接并挂载服务器节点")
        self.btn_connect.setMinimumHeight(40)
        self.btn_connect.clicked.connect(self.on_connect_clicked)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_connect)
        
        form_conn.addRow("", btn_layout)

        layout.addWidget(grp_conn)

        # UI 设置区
        grp_ui = QGroupBox("界面设置")
        form_ui = QFormLayout(grp_ui)

        self.sb_refresh_interval = QSpinBox()
        self.sb_refresh_interval.setRange(100, 5000)
        self.sb_refresh_interval.setSuffix(" ms")
        self.sb_refresh_interval.setToolTip("UI 刷新间隔，建议 300-1000ms")
        saved_interval = self.dm.pm.data_store.get("refresh_interval", 500)
        self.sb_refresh_interval.setValue(saved_interval)
        self.sb_refresh_interval.valueChanged.connect(self._on_refresh_interval_changed)

        self.cb_auto_connect = QCheckBox("启动时自动连接")
        self.cb_auto_connect.setToolTip("程序启动时自动连接 OPC 服务器")
        self.cb_auto_connect.setChecked(self.dm.pm.data_store.get("auto_connect", False))
        self.cb_auto_connect.stateChanged.connect(self._on_auto_connect_changed)

        self.cb_close_to_tray = QCheckBox("关闭按钮最小化到托盘")
        self.cb_close_to_tray.setToolTip("点击关闭按钮时最小化到系统托盘，而不是退出程序")
        self.cb_close_to_tray.setChecked(self.dm.pm.data_store.get("close_to_tray", False))
        self.cb_close_to_tray.stateChanged.connect(self._on_close_to_tray_changed)

        self.cb_dark_mode = QCheckBox("深色模式")
        self.cb_dark_mode.setToolTip("切换深色/浅色界面主题")
        self.cb_dark_mode.setChecked(self.dm.pm.data_store.get("dark_mode", False))
        self.cb_dark_mode.stateChanged.connect(self._on_dark_mode_changed)

        self.le_username = QLineEdit()
        self.le_username.setPlaceholderText("输入用户名...")
        self.le_username.setText(self.dm.pm.data_store.get("username", "Admin"))
        self.le_username.editingFinished.connect(self._on_username_changed)

        form_ui.addRow("刷新间隔:", self.sb_refresh_interval)
        form_ui.addRow("", self.cb_auto_connect)
        form_ui.addRow("", self.cb_close_to_tray)
        form_ui.addRow("", self.cb_dark_mode)
        form_ui.addRow("用户名:", self.le_username)

        layout.addWidget(grp_ui)
        layout.addStretch()

    def _on_refresh_interval_changed(self, value):
        """刷新间隔改变时立即生效"""
        self.dm.pm.data_store["refresh_interval"] = value
        self.dm.pm.save()
        # 立即更新主窗口的刷新定时器
        main_win = self.window()
        if hasattr(main_win, 'refresh_timer'):
            main_win.refresh_timer.setInterval(value)

    def _on_auto_connect_changed(self, checked):
        """自动连接选项改变时保存"""
        self.dm.pm.data_store["auto_connect"] = bool(checked)
        self.dm.pm.save()

    def _on_close_to_tray_changed(self, checked):
        """关闭到托盘选项改变时保存"""
        self.dm.pm.data_store["close_to_tray"] = bool(checked)
        self.dm.pm.save()

    def _on_dark_mode_changed(self, checked):
        """深色模式切换"""
        self.dm.pm.data_store["dark_mode"] = bool(checked)
        self.dm.pm.save()
        main_win = self.window()
        if hasattr(main_win, '_apply_theme'):
            main_win._apply_theme(checked)
        global_logger.info(f"Theme switched to {'dark' if checked else 'light'} mode.")

    def _on_username_changed(self):
        """用户名修改"""
        username = self.le_username.text().strip() or "Admin"
        self.dm.pm.data_store["username"] = username
        self.dm.pm.save()
        # 更新主窗口问候语
        main_win = self.window()
        if main_win:
            # 直接查找顶层的问候语标签
            for child in main_win.findChildren(QLabel):
                if "你好" in child.text():
                    child.setText(f"你好，{username} 👋")
                    break

    def on_connect_clicked(self):
        if getattr(self, '_is_reconnecting', False):
            self._is_reconnecting = False
            self.btn_connect.setText("已请求停止自动重连... 正在断开")
            self.btn_connect.setEnabled(False)
            asyncio.create_task(self._disconnect())
            return
            
        if not self.engine.connected:
            self.engine.host = self.le_host.text().strip()
            # 主机地址合法性校验
            host = self.engine.host
            # 检查是否为合法 IP 或主机名
            ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
            hostname_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
            if not (re.match(ip_pattern, host) or re.match(hostname_pattern, host)):
                QMessageBox.warning(self, "地址错误", "请输入合法的 IP 地址或主机名。")
                return
            # 端口合法性校验
            port_str = self.le_port.text().strip() or "48401"
            try:
                port_val = int(port_str)
                if not (1 <= port_val <= 65535):
                    raise ValueError
            except ValueError:
                QMessageBox.warning(self, "端口错误", "请输入合法的端口号 (1-65535)。")
                return
            self.engine.port = port_val
            self.engine.url = f"opc.tcp://{self.engine.host}:{self.engine.port}/"
            self.engine.username = self.le_user.text().strip()
            self.engine.password = self.le_pass.text().strip()
            # 传入可配置的命名空间过滤前缀
            ns_filter = self.le_ns_filter.text().strip()
            # 命名空间格式校验（应为 "ns=X;" 格式，留空则不过滤）
            if ns_filter and not re.match(r'^ns=\d+;?$', ns_filter):
                QMessageBox.warning(self, "命名空间格式错误", "命名空间前缀格式应为 'ns=2;' 或 'ns=2'，留空则不过滤。")
                return
            self.engine.namespace_filter = ns_filter if ns_filter else None
            
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("正在连接中...")
            asyncio.create_task(self._connect_and_load())
        else:
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("正在断开中...")
            asyncio.create_task(self._disconnect())

    async def _connect_and_load(self):
        # 创建进度对话框
        progress = QProgressDialog("正在连接服务器...", "取消", 0, 100, self)
        progress.setWindowTitle("连接中")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        progress.setValue(10)
        progress.setLabelText(f"正在连接 {self.engine.url}...")

        try:
            global_logger.info(f"Connecting to {self.engine.url}...")
            await self.engine.connect()
            progress.setValue(40)
            progress.setLabelText("已连接，正在加载节点...")
            global_logger.info("Connected. Loading node tree...")

            # 连接成功后持久化连接参数
            self.dm.pm.data_store["connection"] = {
                "host": self.le_host.text().strip(),
                "port": self.le_port.text().strip(),
                "user": self.le_user.text().strip(),
                "password": self._encode_password(self.le_pass.text().strip()),
                "ns_filter": self.le_ns_filter.text().strip(),
            }
            self.dm.pm.save()

            progress.setValue(50)
            progress.setLabelText("正在获取节点列表...")

            # 抓取 OPC 节点树
            nodes = await self.engine.get_all_nodes()
            progress.setValue(70)
            progress.setLabelText(f"正在加载 {len(nodes)} 个节点...")
            global_logger.info(f"Grabbed {len(nodes)} valid control nodes.")
            
            progress.setValue(85)
            progress.setLabelText("正在初始化数据订阅...")

            # 打包塞给数据总线
            for n in nodes:
                self.dm.update_node(n['node_id'], n)

            progress.setValue(95)
            progress.setLabelText("正在启动订阅...")

            # 开始订阅并把回调接给数据总线
            await self.engine.start_subscription(self._on_sub_data)

            # 启动调度器（如果尚未运行）
            main_win = self.window()
            if hasattr(main_win, 'scheduler') and not main_win.scheduler.running:
                main_win.scheduler.start()

            progress.setValue(100)

            self.btn_connect.setText("断开服务器连接")
            self.btn_connect.setStyleSheet("color: #ef4444;")

            # 通知主界面连接成功更变模式指示
            if hasattr(self.window(), 'card_dash_mode'):
                self.window().card_dash_mode.value_label.setText(f"已连接: {self.engine.host}")
                self.window().card_dash_mode.value_label.setStyleSheet("color: #10b981;")

        except Exception as e:
            progress.close()
            global_logger.error(f"Connection flow failed: {e}", exc_info=True)
            QMessageBox.critical(self, "连接流失败", str(e))
            self.btn_connect.setText("连接并挂载服务器节点")
        finally:
            progress.close()
            self.btn_connect.setEnabled(True)

    def _on_sub_data(self, node_id, value, timestamp):
        # 接收到底层推送，交给DataManager中心处理合并
        # DataManager.update_node 已内置未知节点校验，此处无需额外检查
        self.dm.update_node(node_id, {"value": value, "timestamp": timestamp})

    def _handle_connection_lost(self):
        if getattr(self, '_is_reconnecting', False):
            return
            
        global_logger.warning("UI caught connection lost event. Initiating auto-reconnect sequence.")
        
        self._is_reconnecting = True
        
        def _update_ui_and_start_reconnect():
            if hasattr(self.window(), 'card_dash_mode'):
                self.window().card_dash_mode.value_label.setText("断开并重连中...")
                self.window().card_dash_mode.value_label.setStyleSheet("color: #ef4444;")
                
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("停止自动重连")
            self.btn_connect.setStyleSheet("color: #ef4444;")
            
            # 在主线程的事件循环中创建异步任务，避免跨线程 RuntimeError，并保存强引用防止被 GC
            self._reconnect_task = asyncio.create_task(self._auto_reconnect_loop())
        
        QTimer.singleShot(0, _update_ui_and_start_reconnect)
        
    async def _auto_reconnect_loop(self):
        try:
            global_logger.info(f"[_auto_reconnect_loop] Loop starting. connected={self.engine.connected}, is_reconnecting={getattr(self, '_is_reconnecting', False)}")
            while getattr(self, '_is_reconnecting', False) and not self.engine.connected:
                global_logger.info("[_auto_reconnect_loop] Waiting 15s to backoff before trying to reconnect...")
                await asyncio.sleep(15) # 安全退避避免 SYN Flood 防火墙封禁
                global_logger.info("[_auto_reconnect_loop] Woke up from 15s sleep.")
                if self.engine.connected or not getattr(self, '_is_reconnecting', False):
                    global_logger.info("[_auto_reconnect_loop] Condition changed, breaking loop.")
                    break
                    
                self.btn_connect.setText("正在尝试建立连接...")
                try:
                    global_logger.info("[_auto_reconnect_loop] Calling engine.disconnect() for clean up...")
                    await self.engine.disconnect()
                    global_logger.info("[_auto_reconnect_loop] engine.disconnect() finished.")
                except Exception as de:
                    global_logger.info(f"[_auto_reconnect_loop] engine.disconnect() failed with: {de}")
                
                try:
                    global_logger.info(f"[Auto-Reconnecting to {self.engine.url}...")
                    await self.engine.connect()
                    global_logger.info("[_auto_reconnect_loop] engine.connect() finished.")
                    nodes = await self.engine.get_all_nodes()
                    for n in nodes:
                        self.dm.update_node(n['node_id'], n)
                    await self.engine.start_subscription(self._on_sub_data)
                    
                    # 重连成功后确保调度器运行中
                    main_win = self.window()
                    if hasattr(main_win, 'scheduler') and not main_win.scheduler.running:
                        main_win.scheduler.start()
                    
                    self.btn_connect.setText("断开服务器连接")
                    self.btn_connect.setStyleSheet("color: #ef4444;")
                    if hasattr(self.window(), 'card_dash_mode'):
                        self.window().card_dash_mode.value_label.setText(f"已连接: {self.engine.host}")
                        self.window().card_dash_mode.value_label.setStyleSheet("color: #10b981;")
                    
                    self._is_reconnecting = False
                    break
                except Exception as e:
                    global_logger.error(f"Auto-reconnect failed: {e}")
                    if getattr(self, '_is_reconnecting', False):
                        self.btn_connect.setText("停止自动重连 (下次在15秒后...)")
                        if hasattr(self.window(), 'card_dash_mode'):
                             self.window().card_dash_mode.value_label.setText("定时重试中...")
        finally:
            if not self.engine.connected and not getattr(self, '_is_reconnecting', False):
                pass

    async def _disconnect(self):
        self._is_reconnecting = False
        await self.engine.disconnect()
        # 清理数据总线中失效的节点数据
        self.dm.clear_nodes()
        self.btn_connect.setText("连接并挂载服务器节点")
        self.btn_connect.setEnabled(True)
        self.btn_connect.setStyleSheet("")
        # 恢复 Dashboard 状态
        if hasattr(self.window(), 'card_dash_mode'):
            self.window().card_dash_mode.value_label.setText("待连接")
            self.window().card_dash_mode.value_label.setStyleSheet("color: #f59e0b;")
        global_logger.info("Disconnected from OPC Server.")

    @staticmethod
    def _encode_password(plain: str) -> str:
        """使用 base64 编码密码，避免明文存储"""
        if not plain:
            return ""
        return base64.b64encode(plain.encode('utf-8')).decode('ascii')

    @staticmethod
    def _decode_password(encoded: str) -> str:
        """解码 base64 密码，兼容旧版明文密码"""
        if not encoded:
            return ""
        try:
            return base64.b64decode(encoded.encode('ascii')).decode('utf-8')
        except Exception:
            # 兼容旧版未编码的明文密码
            return encoded
