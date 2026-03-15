try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox
    from PyQt5.QtCore import QTimer
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox
    from PySide6.QtCore import QTimer

import asyncio
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
        self.le_pass = QLineEdit(saved_conn.get("password", ""))
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
        self.btn_connect.setStyleSheet("font-weight: bold;")
        self.btn_connect.clicked.connect(self.on_connect_clicked)
        btn_layout.addStretch()
        btn_layout.addWidget(self.btn_connect)
        
        form_conn.addRow("", btn_layout)
        
        layout.addWidget(grp_conn)
        layout.addStretch()

    def on_connect_clicked(self):
        if getattr(self, '_is_reconnecting', False):
            self._is_reconnecting = False
            self.btn_connect.setText("已请求停止自动重连... 正在断开")
            self.btn_connect.setEnabled(False)
            asyncio.create_task(self._disconnect())
            return
            
        if not self.engine.connected:
            self.engine.host = self.le_host.text().strip()
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
            self.engine.namespace_filter = ns_filter if ns_filter else None
            
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("正在连接中...")
            asyncio.create_task(self._connect_and_load())
        else:
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("正在断开中...")
            asyncio.create_task(self._disconnect())

    async def _connect_and_load(self):
        try:
            global_logger.info(f"Connecting to {self.engine.url}...")
            await self.engine.connect()
            global_logger.info("Connected. Loading node tree...")
            
            # 连接成功后持久化连接参数
            self.dm.pm.data_store["connection"] = {
                "host": self.le_host.text().strip(),
                "port": self.le_port.text().strip(),
                "user": self.le_user.text().strip(),
                "password": self.le_pass.text().strip(),
                "ns_filter": self.le_ns_filter.text().strip(),
            }
            self.dm.pm.save()
            
            # 抓取 OPC 节点树
            nodes = await self.engine.get_all_nodes()
            global_logger.info(f"Grabbed {len(nodes)} valid control nodes.")
            
            # 打包塞给数据总线
            for n in nodes:
                self.dm.update_node(n['node_id'], n)
            
            # 开始订阅并把回调接给数据总线
            await self.engine.start_subscription(self._on_sub_data)
            
            # 启动调度器（如果尚未运行）
            main_win = self.window()
            if hasattr(main_win, 'scheduler') and not main_win.scheduler.running:
                main_win.scheduler.start()
            
            self.btn_connect.setText("断开服务器连接")
            self.btn_connect.setStyleSheet("font-weight: bold; color: red;")
            
            # 通知主界面连接成功更变模式指示
            if hasattr(self.window(), 'lbl_dash_mode'):
                self.window().lbl_dash_mode.setText(f"系统状态 (已连接: {self.engine.host})")
                self.window().lbl_dash_mode.setStyleSheet("background-color: #8C9EFF; color: white; border-radius: 4px; padding: 10px; font-weight: bold; font-size: 14px;")

        except Exception as e:
            global_logger.error(f"Connection flow failed: {e}", exc_info=True)
            QMessageBox.critical(self, "连接流失败", str(e))
            self.btn_connect.setText("连接并挂载服务器节点")
        finally:
            self.btn_connect.setEnabled(True)

    def _on_sub_data(self, node_id, value, timestamp):
        # 接收到底层推送，交给DataManager中心处理合并
        # DataManager.update_node 已内置未知节点校验，此处无需额外检查
        self.dm.update_node(node_id, {"value": value, "timestamp": timestamp})

    def _handle_connection_lost(self):
        global_logger.warning("UI caught connection lost event. Initiating auto-reconnect sequence.")
        
        self._is_reconnecting = True
        
        # 使用 QTimer.singleShot(0, ...) 确保 UI 操作和 asyncio 任务创建始终在主线程执行
        def _update_ui_and_start_reconnect():
            if hasattr(self.window(), 'lbl_dash_mode'):
                self.window().lbl_dash_mode.setText("系统状态 (连接断开，尝试重连中...)")
                self.window().lbl_dash_mode.setStyleSheet("background-color: #ffccc7; color: #cf1322; border: 1px solid #ffa39e; border-radius: 4px; padding: 10px; font-weight: bold; font-size: 14px;")
                
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("停止自动重连")
            self.btn_connect.setStyleSheet("font-weight: bold; color: red;")
            
            # 在主线程的事件循环中创建异步任务，避免跨线程 RuntimeError
            asyncio.create_task(self._auto_reconnect_loop())
        
        QTimer.singleShot(0, _update_ui_and_start_reconnect)
        
    async def _auto_reconnect_loop(self):
        try:
            while getattr(self, '_is_reconnecting', False) and not self.engine.connected:
                await asyncio.sleep(15) # 安全退避避免 SYN Flood 防火墙封禁
                if self.engine.connected or not getattr(self, '_is_reconnecting', False):
                    break
                    
                self.btn_connect.setText("正在尝试建立连接...")
                try:
                    await self.engine.disconnect()
                except Exception:
                    pass
                
                try:
                    global_logger.info(f"Auto-Reconnecting to {self.engine.url}...")
                    await self.engine.connect()
                    nodes = await self.engine.get_all_nodes()
                    for n in nodes:
                        self.dm.update_node(n['node_id'], n)
                    await self.engine.start_subscription(self._on_sub_data)
                    
                    # 重连成功后确保调度器运行中
                    main_win = self.window()
                    if hasattr(main_win, 'scheduler') and not main_win.scheduler.running:
                        main_win.scheduler.start()
                    
                    self.btn_connect.setText("断开服务器连接")
                    self.btn_connect.setStyleSheet("font-weight: bold; color: red;")
                    if hasattr(self.window(), 'lbl_dash_mode'):
                        self.window().lbl_dash_mode.setText(f"系统状态 (已连接: {self.engine.host})")
                        self.window().lbl_dash_mode.setStyleSheet("background-color: #8C9EFF; color: white; border-radius: 4px; padding: 10px; font-weight: bold; font-size: 14px;")
                    
                    self._is_reconnecting = False
                    break
                except Exception as e:
                    global_logger.error(f"Auto-reconnect failed: {e}")
                    if getattr(self, '_is_reconnecting', False):
                        self.btn_connect.setText("停止自动重连 (下次在15秒后...)")
                        if hasattr(self.window(), 'lbl_dash_mode'):
                             self.window().lbl_dash_mode.setText("系统状态 (重连失败，定时重试中...)")
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
        self.btn_connect.setStyleSheet("font-weight: bold;")
        # 恢复 Dashboard 状态
        if hasattr(self.window(), 'lbl_dash_mode'):
            self.window().lbl_dash_mode.setText("系统状态 (待连接)")
            self.window().lbl_dash_mode.setStyleSheet("background-color: #E0E0E0; color: gray; border: 1px solid #CCCCCC; border-radius: 4px; padding: 10px; font-weight: bold; font-size: 14px;")
        global_logger.info("Disconnected from OPC Server.")
