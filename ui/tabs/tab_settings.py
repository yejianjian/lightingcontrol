try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit, QFormLayout, QMessageBox, QGroupBox

import asyncio
from utils.logger import global_logger

class TabSettings(QWidget):
    def __init__(self, opc_engine, data_manager):
        super().__init__()
        self.engine = opc_engine
        self.dm = data_manager
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # OPC 服务联机配置区
        grp_conn = QGroupBox("OPC UA 服务器连接配置")
        form_conn = QFormLayout(grp_conn)

        self.le_host = QLineEdit("127.0.0.1")
        self.le_port = QLineEdit("48401")
        self.le_user = QLineEdit("admin")
        self.le_pass = QLineEdit("123456")
        self.le_pass.setEchoMode(QLineEdit.Password)

        form_conn.addRow("DA 主机 IP (Host):", self.le_host)
        form_conn.addRow("服务端口 (Port):", self.le_port)
        form_conn.addRow("验证账户 (User):", self.le_user)
        form_conn.addRow("验证密码 (Password):", self.le_pass)

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
            
            # 抓取 OPC 节点树
            nodes = await self.engine.get_all_nodes()
            global_logger.info(f"Grabbed {len(nodes)} valid control nodes.")
            
            # 打包塞给数据总线
            for n in nodes:
                self.dm.update_node(n['node_id'], n)
            
            # 开始订阅并把回调接给数据总线
            await self.engine.start_subscription(self._on_sub_data)
            
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
        # 接收到底层推送，交给DataManager中心处理合并 (顺带触发UI统计渲染)
        self.dm.update_node(node_id, {"value": value, "timestamp": timestamp})

    async def _disconnect(self):
        await self.engine.disconnect()
        self.btn_connect.setText("连接并挂载服务器节点")
        self.btn_connect.setEnabled(True)
        self.btn_connect.setStyleSheet("font-weight: bold;")
        # 恢复 Dashboard 状态
        if hasattr(self.window(), 'lbl_dash_mode'):
            self.window().lbl_dash_mode.setText("系统状态 (待连接)")
            self.window().lbl_dash_mode.setStyleSheet("background-color: #E0E0E0; color: gray; border: 1px solid #CCCCCC; border-radius: 4px; padding: 10px; font-weight: bold; font-size: 14px;")
        global_logger.info("Disconnected from OPC Server.")
