try:
    from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
                                 QFormLayout, QMessageBox, QGroupBox, QSpinBox, QCheckBox,
                                 QProgressDialog, QScrollArea, QFrame)
    from PyQt5.QtCore import QTimer, Qt
except ImportError:
    from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QLabel, QLineEdit,
                                   QFormLayout, QMessageBox, QGroupBox, QSpinBox, QCheckBox,
                                   QProgressDialog, QScrollArea, QFrame)
    from PySide6.QtCore import QTimer, Qt

import asyncio
import re
import time
from utils.logger import global_logger
from utils.security import encrypt_secret, decrypt_secret


class TabSettings(QWidget):
    """设置与连接页。

    重连职责划分：**引擎负责重连，本页只反映状态**。以前这里自己写了一个
    ``_auto_reconnect_loop``，它会在 ``get_all_nodes()`` 上无限期挂起（没有超时），
    一旦卡住就再也不会重试，也没有任何手段取消。现在改为订阅引擎的状态机回调，
    按钮文字与仪表盘状态完全由引擎状态驱动。
    """

    def __init__(self, opc_engine, data_manager):
        super().__init__()
        self.engine = opc_engine
        self.dm = data_manager

        saved = self.dm.pm.data_store

        # 先装配引擎回调，再读配置，避免状态回调在 UI 尚未建立时触发
        self.engine.on_connection_lost = self._handle_connection_lost
        self.engine.on_state_change = self._on_engine_state_change
        self.engine.on_nodes_recovered = self._on_nodes_recovered
        # 登记订阅回调：引擎在自动重连后会自行用它重建订阅
        self.engine.set_subscription_callback(self._on_sub_data)

        # 应用持久化的重连/保活策略
        self.engine.reconnect_base_delay = float(saved.get("reconnect_base_delay", self.engine.reconnect_base_delay))
        self.engine.reconnect_max_delay = float(saved.get("reconnect_max_delay", self.engine.reconnect_max_delay))
        self.engine.session_timeout_ms = int(saved.get("session_timeout_ms", self.engine.session_timeout_ms))

        # Task 强引用池：防止进行中的协程被 GC 回收
        self._connect_task = None
        self._disconnect_task = None
        # 用户是否通过 QProgressDialog 的「取消」按钮终止了本次连接：
        # 取消后连接流程的 finally 不得再把「已连接」投影到界面。
        self._connect_cancelled = False

        self._setup_ui()
        self._apply_state(self.engine.state, {"reason": "initial"})

        # 依据持久化开关决定引擎的自动重连总闸。
        # 注意 reconnect_forever() 是协程：直接调用只会创建协程对象而不执行
        # （并触发 "coroutine was never awaited"），必须交给事件循环调度。
        if self.cb_auto_reconnect.isChecked():
            self._schedule_reconnect_forever()
        else:
            self.engine.stop_reconnect()

    def _schedule_reconnect_forever(self):
        """在事件循环中安全地打开自动重连总闸（GUI 回调不能是协程）。"""
        # 冷启动时用户先勾选"自动重连"（或配置里持久化为 true）会走到这里：
        # 重连 supervisor 会直接调用 engine.connect() 发起首次连接，而这条路径
        # 不经过 _connect_and_load() 的表单注入。若此时引擎还是默认值
        # （localhost / 匿名），网关会以 BadUserAccessDenied 拒绝 ActivateSession，
        # 表现为"勾选自动重连后一直连不上，停止重连再手动连接却一次成功"。
        # 因此必须在打开总闸前把界面上的连接参数/账号密码注入引擎。
        if hasattr(self, "le_host"):
            self._apply_form_to_engine(interactive=False)
        # 先把总闸打开：事件循环未就绪或任务调度失败时，策略状态也必须正确，
        # 否则断线时会落到"待连接"，而界面上的复选框还勾着。
        self.engine._auto_reconnect = True
        try:
            loop = asyncio.get_event_loop()
            task = loop.create_task(self.engine.reconnect_forever())
            # 交给引擎登记：这样 stop_reconnect()/connect() 能取消这个"还在排队、
            # 尚未执行"的协程，避免它在用户已经点了停止之后才把总闸重新打开。
            self.engine._pending_reconnect_forever = task
            self._reconnect_forever_task = task
        except RuntimeError as e:
            global_logger.debug(f"Event loop not ready for reconnect supervisor ({e}); flag set only.")

    # ----------------------------------------------------------------------------------
    # UI
    # ----------------------------------------------------------------------------------

    def _setup_ui(self):
        # 设置页是长表单（连接 / 重连策略 / 界面 三组共 20+ 行）。整页套一层滚动区域，
        # 这样它的最小高度不再撑大整个窗口 —— 否则 768 高的屏幕上窗口会被强制拉到
        # 900+ 像素，用户全屏也看不到底部内容。宽度同样允许收缩。
        outer = QVBoxLayout(self)
        outer.setContentsMargins(0, 0, 0, 0)
        outer.setSpacing(0)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)
        scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        scroll.setVerticalScrollBarPolicy(Qt.ScrollBarAsNeeded)
        outer.addWidget(scroll)

        content = QWidget()
        scroll.setWidget(content)

        layout = QVBoxLayout(content)
        layout.setContentsMargins(6, 6, 6, 6)
        layout.setSpacing(6)

        # 协议相关的"服务器连接配置"区：UA 用 host/port/ns，DA 子类覆盖本方法。
        self._build_connection_group(layout)
        # 协议相关的额外配置区（DA 版用于"手动添加控制点位"；UA 版默认没有）
        self._build_extra_groups(layout)

        # 重连与保活策略区（24/7 现场可按网络质量调整）
        grp_recon = QGroupBox("重连与保活策略")
        form_recon = QFormLayout(grp_recon)

        self.sb_reconnect_base = QSpinBox()
        self.sb_reconnect_base.setRange(3, 600)
        self.sb_reconnect_base.setSuffix(" 秒")
        self.sb_reconnect_base.setToolTip("首次重连等待时间，之后按 2 倍指数退避")
        # 关键顺序：先 setValue，最后才 connect。QSpinBox.setValue 会发射 valueChanged，
        # 若先连接信号，初始化阶段就会把默认值"当成用户修改"写盘并打日志 —— 之前每条
        # "Reconnect policy updated" 在启动时刷了十几行，把真正的现场日志淹没了。
        self.sb_reconnect_base.setValue(int(self.engine.reconnect_base_delay))

        self.sb_reconnect_max = QSpinBox()
        self.sb_reconnect_max.setRange(10, 1800)
        self.sb_reconnect_max.setSuffix(" 秒")
        self.sb_reconnect_max.setToolTip("两次重连之间的最大等待时间上限")
        self.sb_reconnect_max.setValue(int(self.engine.reconnect_max_delay))

        self.sb_session_timeout = QSpinBox()
        self.sb_session_timeout.setRange(10, 3600)
        self.sb_session_timeout.setSuffix(" 秒")
        self.sb_session_timeout.setToolTip(
            "会话保活窗口。引擎内部看门狗间隔取其一半（上限 10 秒），\n"
            "数值越小断线发现越快，但网络心跳越频繁。建议 60 秒。"
        )
        self.sb_session_timeout.setValue(max(10, int(self.engine.session_timeout_ms / 1000)))

        for spin in (self.sb_reconnect_base, self.sb_reconnect_max, self.sb_session_timeout):
            spin.valueChanged.connect(self._on_reconnect_policy_changed)

        form_recon.addRow("首次重连等待:", self.sb_reconnect_base)
        form_recon.addRow("重连最大间隔:", self.sb_reconnect_max)
        form_recon.addRow("会话保活窗口:", self.sb_session_timeout)

        recon_hint = QLabel("断线后由引擎自动按指数退避重连（含随机抖动），无需人工干预；\n"
                            "重连期间定时任务会自动排队，恢复后补发。")
        recon_hint.setStyleSheet("color: #64748b; font-size: 12px;")
        recon_hint.setWordWrap(True)
        form_recon.addRow("", recon_hint)

        layout.addWidget(grp_recon)

        # UI 设置区
        grp_ui = QGroupBox("界面设置")
        form_ui = QFormLayout(grp_ui)

        self.sb_refresh_interval = QSpinBox()
        self.sb_refresh_interval.setRange(100, 5000)
        self.sb_refresh_interval.setSuffix(" ms")
        self.sb_refresh_interval.setToolTip("UI 刷新间隔，建议 300-1000ms")
        self.sb_refresh_interval.setValue(self.dm.pm.data_store.get("refresh_interval", 500))
        self.sb_refresh_interval.valueChanged.connect(self._on_refresh_interval_changed)

        self.cb_auto_connect = QCheckBox("启动时自动连接")
        self.cb_auto_connect.setToolTip("程序启动时自动连接 OPC 服务器")
        self.cb_auto_connect.setChecked(self.dm.pm.data_store.get("auto_connect", False))
        self.cb_auto_connect.stateChanged.connect(self._on_auto_connect_changed)

        self.cb_auto_reconnect = QCheckBox("断线后自动重连")
        self.cb_auto_reconnect.setToolTip("连接意外中断时由引擎自动重连（建议开启）")
        self.cb_auto_reconnect.setChecked(self.dm.pm.data_store.get("auto_reconnect", True))
        self.cb_auto_reconnect.stateChanged.connect(self._on_auto_reconnect_changed)

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

        self.le_exit_password = QLineEdit()
        self.le_exit_password.setEchoMode(QLineEdit.Password)
        self.le_exit_password.setPlaceholderText("系统退出安全锁密码，留空默认为 8888")
        self.le_exit_password.setText(self.dm.pm.data_store.get("exit_password", "8888"))
        self.le_exit_password.editingFinished.connect(self._on_exit_password_changed)

        form_ui.addRow("刷新间隔:", self.sb_refresh_interval)
        form_ui.addRow("", self.cb_auto_connect)
        form_ui.addRow("", self.cb_auto_reconnect)
        form_ui.addRow("", self.cb_close_to_tray)
        form_ui.addRow("", self.cb_dark_mode)
        form_ui.addRow("用户名:", self.le_username)
        form_ui.addRow("退出授权密码:", self.le_exit_password)

        layout.addWidget(grp_ui)
        layout.addStretch()

    def _build_extra_groups(self, layout):
        """构建协议相关的额外设置区（默认无）。

        OPC DA 版覆盖本方法，在连接配置区之后追加"手动添加控制点位"。
        """
        return

    def _build_connection_group(self, layout):
        """构建协议相关的服务器连接配置区（OPC UA 默认实现）。

        OPC DA 版设置页继承 TabSettings 并覆盖本方法、_apply_form_to_engine 与
        _persist_connection_params 即可复用状态机/按钮/重连策略等全部逻辑。
        """
        grp_conn = QGroupBox("OPC UA 服务器连接配置")
        form_conn = QFormLayout(grp_conn)

        saved_conn = self.dm.pm.data_store.get("connection", {})
        self.le_host = QLineEdit(saved_conn.get("host", "127.0.0.1"))
        self.le_port = QLineEdit(saved_conn.get("port", "48401"))
        self.le_user = QLineEdit(saved_conn.get("user", ""))
        self.le_pass = QLineEdit(decrypt_secret(saved_conn.get("password", "")))
        self.le_pass.setEchoMode(QLineEdit.Password)
        self.le_ns_filter = QLineEdit(saved_conn.get("ns_filter", "ns=2;"))
        self.le_ns_filter.setToolTip("业务节点命名空间前缀，留空则不过滤")

        # 窄窗口下保证输入框可读
        for le in (self.le_host, self.le_port, self.le_user, self.le_pass, self.le_ns_filter):
            le.setMinimumWidth(180)

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

        # 连接状态提示（含重试倒计时），让 24/7 值守人员一眼看出链路是否健康
        self.lbl_conn_status = QLabel("未连接")
        self.lbl_conn_status.setWordWrap(True)
        self.lbl_conn_status.setStyleSheet("color: #64748b;")
        form_conn.addRow("链路状态:", self.lbl_conn_status)

        layout.addWidget(grp_conn)
        return grp_conn

    # ----------------------------------------------------------------------------------
    # 设置项回调
    # ----------------------------------------------------------------------------------

    def _on_reconnect_policy_changed(self):
        base = float(self.sb_reconnect_base.value())
        maximum = float(self.sb_reconnect_max.value())
        if maximum < base:
            maximum = base
            self.sb_reconnect_max.setValue(int(maximum))
        self.engine.reconnect_base_delay = base
        self.engine.reconnect_max_delay = maximum
        self.engine.session_timeout_ms = int(self.sb_session_timeout.value()) * 1000
        store = self.dm.pm.data_store
        store["reconnect_base_delay"] = base
        store["reconnect_max_delay"] = maximum
        store["session_timeout_ms"] = self.engine.session_timeout_ms
        self.dm.pm.save()
        global_logger.info(
            f"Reconnect policy updated: base={base:.0f}s, max={maximum:.0f}s, "
            f"session_timeout={self.engine.session_timeout_ms}ms"
        )

    def _on_refresh_interval_changed(self, value):
        """刷新间隔改变时立即生效"""
        self.dm.pm.data_store["refresh_interval"] = value
        self.dm.pm.save()
        main_win = self.window()
        if hasattr(main_win, 'refresh_timer'):
            main_win.refresh_timer.setInterval(value)

    def _on_auto_connect_changed(self, checked):
        self.dm.pm.data_store["auto_connect"] = bool(checked)
        self.dm.pm.save()

    def _on_auto_reconnect_changed(self, checked):
        enabled = bool(checked)
        self.dm.pm.data_store["auto_reconnect"] = enabled
        self.dm.pm.save()
        if not enabled:
            self.engine.stop_reconnect()
            return
        # 勾选即视为用户意图开启：立刻把总闸打开（不依赖事件循环是否已就绪），
        # 这样"断线后自动重连"不会再出现"界面勾着、引擎却没开"的错位。
        self._schedule_reconnect_forever()

    def _sync_auto_reconnect_flag(self):
        """把复选框状态与引擎总闸对齐，返回复选框是否勾选。

        复选框是**唯一事实来源**：勾选项目必须真的生效，任何代码路径若把
        `_auto_reconnect` 关掉都要同步反映到复选框上，避免"界面勾着却不重连"或者
        "用户已点停止、下次断线又被自动拉起"这两种误导。
        """
        wanted = bool(self.cb_auto_reconnect.isChecked())
        if wanted != bool(self.engine._auto_reconnect):
            global_logger.warning(
                f"Auto-reconnect flag out of sync (checkbox={wanted}, engine={self.engine._auto_reconnect}); "
                f"aligning to the checkbox value."
            )
            self.engine._auto_reconnect = wanted
        return wanted

    def _set_auto_reconnect_preference(self, enabled):
        """统一设置"是否自动重连"：勾选状态、持久化配置、引擎总闸三者一致。

        通过按钮停止自动重连时也要取消勾选，否则设置页会声称"已开启自动重连"，
        而实际上引擎已被停止 —— 值守人员会被界面误导。
        """
        if self.cb_auto_reconnect.isChecked() != bool(enabled):
            # 触发 stateChanged -> _on_auto_reconnect_changed，由其完成三处同步
            self.cb_auto_reconnect.setChecked(bool(enabled))
            return
        if enabled:
            self._schedule_reconnect_forever()
        else:
            self.engine.stop_reconnect()
        self.dm.pm.data_store["auto_reconnect"] = bool(enabled)
        self.dm.pm.save()

    def _on_close_to_tray_changed(self, checked):
        self.dm.pm.data_store["close_to_tray"] = bool(checked)
        self.dm.pm.save()

    def _on_dark_mode_changed(self, checked):
        self.dm.pm.data_store["dark_mode"] = bool(checked)
        self.dm.pm.save()
        main_win = self.window()
        if hasattr(main_win, '_apply_theme'):
            main_win._apply_theme(checked)
        global_logger.info(f"Theme switched to {'dark' if checked else 'light'} mode.")

    def _on_username_changed(self):
        username = self.le_username.text().strip() or "Admin"
        self.dm.pm.data_store["username"] = username
        self.dm.pm.save()
        main_win = self.window()
        if main_win:
            for child in main_win.findChildren(QLabel):
                if "你好" in child.text():
                    child.setText(f"你好，{username} 👋")
                    break

    def _on_exit_password_changed(self):
        pwd = self.le_exit_password.text().strip() or "8888"
        self.dm.pm.data_store["exit_password"] = pwd
        self.dm.pm.save()
        global_logger.info("Exit authorization password updated.")

    # ----------------------------------------------------------------------------------
    # 连接 / 断开
    # ----------------------------------------------------------------------------------

    def start_auto_connect(self):
        """启动时的自动连接入口。

        与 :meth:`on_connect_clicked` 的区别：这里**只负责发起连接**，绝不解释为
        "用户想停止重连"。自动重连总闸由设置页构造时/复选框决定，不受本方法影响。
        """
        state = self.engine.state
        if state in ('connecting', 'loading', 'connected'):
            global_logger.info(f"Auto-connect skipped: already in state {state}.")
            return
        if not self._apply_form_to_engine(interactive=False):
            return
        global_logger.info(f"Auto-connect: connecting to {self.engine.url}...")
        self._set_connect_button("正在连接中...", enabled=False)
        self._connect_task = asyncio.create_task(self._connect_and_load())

    def on_connect_clicked(self):
        state = self.engine.state
        # 断线重连中点击 = 停止自动重连
        if state == 'reconnecting':
            global_logger.info("User requested to stop auto-reconnect via the button.")
            self._set_auto_reconnect_preference(False)
            self._disconnect_task = asyncio.create_task(self._disconnect(clear_nodes=False))
            return

        if state == 'connected':
            self._disconnect_task = asyncio.create_task(self._disconnect(clear_nodes=True))
            return

        # 连接/加载过程中点击 = 取消本次连接尝试（引擎通过世代号立即作废它）
        if state in ('connecting', 'loading'):
            global_logger.info(f"User cancelled the in-flight connect attempt (state={state}).")
            self._set_auto_reconnect_preference(False)
            self._disconnect_task = asyncio.create_task(self._disconnect(clear_nodes=False))
            return

        if state == 'disconnecting':
            global_logger.info("Ignoring connect request while disconnecting.")
            return

        # 首次连接：校验输入并注入引擎
        if not self._apply_form_to_engine():
            return

        self._set_connect_button("正在连接中...", enabled=False)
        self._connect_task = asyncio.create_task(self._connect_and_load())

    def _apply_form_to_engine(self, interactive=True):
        """校验界面输入并注入引擎，返回是否通过。

        :param interactive: True 时用弹窗提示错误（用户点击）；False 时仅记日志并返回
            False（启动自动连接等非交互场景，不能弹窗阻塞）。
        """
        def _fail(title, message):
            if interactive:
                QMessageBox.warning(self, title, message)
            else:
                global_logger.error(f"连接参数无效（{title}）: {message}")
            return False

        host = self.le_host.text().strip()
        ip_pattern = r'^(\d{1,3}\.){3}\d{1,3}$'
        hostname_pattern = r'^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$'
        if not (re.match(ip_pattern, host) or re.match(hostname_pattern, host)):
            return _fail("地址错误", "请输入合法的 IP 地址或主机名。")

        port_str = self.le_port.text().strip() or "48401"
        try:
            port_val = int(port_str)
            if not (1 <= port_val <= 65535):
                raise ValueError
        except ValueError:
            return _fail("端口错误", "请输入合法的端口号 (1-65535)。")

        ns_filter = self.le_ns_filter.text().strip()
        if ns_filter and not re.match(r'^ns=\d+;?$', ns_filter):
            return _fail("命名空间格式错误",
                         "命名空间前缀格式应为 'ns=2;' 或 'ns=2'，留空则不过滤。")

        self.engine.configure(
            host=host,
            port=port_val,
            username=self.le_user.text().strip(),
            password=self.le_pass.text().strip(),
            namespace_filter=ns_filter if ns_filter else None,
        )
        return True

    def _set_connect_button(self, text, enabled=True, color=""):
        self.btn_connect.setEnabled(enabled)
        self.btn_connect.setText(text)
        self.btn_connect.setStyleSheet(f"color: {color};" if color else "")

    def _persist_connection_params(self):
        self.dm.pm.data_store["connection"] = {
            "host": self.le_host.text().strip(),
            "port": self.le_port.text().strip(),
            "user": self.le_user.text().strip(),
            "password": encrypt_secret(self.le_pass.text().strip()),
            "ns_filter": self.le_ns_filter.text().strip(),
        }
        self.dm.pm.save()

    def _on_connect_progress_canceled(self):
        """QProgressDialog 的「取消」按钮：真正取消本次连接尝试。

        只 close() 对话框不会打断正在 await 的 connect()/bootstrap()，后台仍会
        连上并挂载点位。这里先取消连接任务，再用一个独立任务
        （stop_reconnect + disconnect）收尾 —— 收尾是异步的，不阻塞 UI 线程。
        """
        if self._connect_cancelled:
            # 同一轮连接只收尾一次（连点/关闭对话框可能重复触发 canceled）。
            return
        self._connect_cancelled = True
        global_logger.info("用户取消连接：正在取消本次连接尝试并释放已建立的会话...")
        task = self._connect_task
        if task is not None and not task.done():
            task.cancel()
        # 取消意图同时同步到自动重连复选框：收尾的 disconnect 会关闭重连总闸，
        # 若不复选位就会再次出现"界面勾着自动重连、引擎其实已关闭"的错位。
        self._set_auto_reconnect_preference(False)
        self._disconnect_task = asyncio.create_task(self._disconnect(clear_nodes=False))

    async def _connect_and_load(self):
        # 自包含：无论从哪个入口进来（点击连接 / 启动自动连接 / 后续调用），
        # 都先把界面上的连接参数注入引擎。否则在 on_connect_clicked 之外的路径上，
        # 引擎会用上一次的地址（甚至默认的 localhost:48401）去连，表现为"点了连接却
        # 连不上正确的服务器"。
        if not self._apply_form_to_engine(interactive=False):
            self._apply_state(self.engine.state, {"reason": "invalid connection parameters"})
            return

        progress = QProgressDialog("正在连接服务器...", "取消", 0, 100, self)
        progress.setWindowTitle("连接中")
        progress.setWindowModality(Qt.WindowModal)
        progress.setMinimumDuration(0)
        # 「取消」必须绑定槽：QProgressDialog 默认只把对话框关掉，不会打断
        # 正在 await 的 connect()/bootstrap()，用户会以为取消无效。
        progress.canceled.connect(self._on_connect_progress_canceled)
        progress.setValue(10)
        progress.setLabelText(f"正在连接 {self.engine.url}...")

        self._connect_cancelled = False
        try:
            global_logger.info(f"Connecting to {self.engine.url}...")
            await self.engine.connect()
            if self._connect_cancelled:
                # 底层 client.connect() 是同步阻塞调用，CancelledError 往往要等它
                # 返回后才送达；这里再确认一次取消意图，绝不继续装配点位/订阅。
                raise asyncio.CancelledError()
            progress.setValue(40)
            progress.setLabelText("已连接，正在加载节点...")
            global_logger.info("Connected. Loading node tree...")

            self._persist_connection_params()

            # 引擎负责"连接 + 加载点位 + 建立订阅"，UI 负责把点位发布到数据总线。
            # 这两步缺一不可：bootstrap() 只把点位放进 engine.nodes，如果不再灌进
            # DataManager，仪表盘与列表都会是 0 —— 这正是"连上了但一个点位都没有"的原因。
            await self.engine.bootstrap()
            # 点位发布由 engine 的 on_nodes_recovered 回调统一负责（手动连接与
            # 自动重连两条路径共用），此处不再重复调用，避免一次连接发布两遍。

            progress.setValue(100)
            self._start_scheduler()
            self._log_heartbeat_policy()
            global_logger.info("Initial connection flow completed successfully.")
        except asyncio.CancelledError:
            global_logger.info("Initial connection flow cancelled.")
        except Exception as e:
            if self._connect_cancelled:
                # 用户已取消：收尾 disconnect 会让引擎抛出 ConnectionSuperseded 之类
                # 的异常，这属于正常终止，不能再弹「连接流失败」吓值守人员。
                global_logger.info(f"Connection flow ended after user cancel: {e}")
            else:
                global_logger.error(f"Connection flow failed: {e}", exc_info=True)
                QMessageBox.critical(self, "连接流失败", str(e))
        finally:
            # 关键：QProgressDialog.close() 在 Qt 内部也会发出 canceled 信号
            # （closeEvent 中 emit），若不解绑，每次正常连接结束都会被当成
            # "用户取消"而立刻断开。只允许真正的用户点击/关闭对话框触发取消。
            try:
                progress.canceled.disconnect(self._on_connect_progress_canceled)
            except (TypeError, RuntimeError):
                pass
            progress.close()
            if self._connect_cancelled:
                # 用户已点「取消」：连接任务可能直到 engine 已经连上之后才收到
                # CancelledError，此处绝不能把 engine.state 的「已连接」投影到界面；
                # 收尾任务 _disconnect() 会在真正断开后自行刷新为「未连接」。
                global_logger.info("用户取消连接：连接流程已终止，界面保持未连接状态。")
            else:
                self._apply_state(self.engine.state, {"reason": "manual connect finished"})

    async def _disconnect(self, clear_nodes=True):
        try:
            self.engine.stop_reconnect()
            await self.engine.disconnect()
        except Exception as e:
            global_logger.error(f"Disconnect failed: {e}", exc_info=True)
        finally:
            if clear_nodes:
                # 用户主动断开：清空点位表，避免切换服务器后残留旧点位
                self.dm.clear_nodes()
            self._apply_state(self.engine.state, {"reason": "manual disconnect finished"})
            global_logger.info("Disconnected from OPC Server.")

    def _start_scheduler(self):
        main_win = self.window()
        if hasattr(main_win, 'scheduler') and not main_win.scheduler.running:
            main_win.scheduler.start()

    # ----------------------------------------------------------------------------------
    # 引擎回调
    # ----------------------------------------------------------------------------------

    def _on_sub_data(self, node_id, value, timestamp, quality=None):
        # 接收到底层推送，交给 DataManager 中心处理合并。
        # quality 必须同步进 DataManager：监控表用它实时显示/恢复「通信异常」，
        # 只在连接/重连时发布点位表会让坏质量状态卡住或永远不出现。
        payload = {"value": value, "timestamp": timestamp}
        if quality is not None:
            payload["quality"] = quality
        self.dm.update_node(node_id, payload)

    def _publish_nodes_to_bus(self):
        """把引擎缓存的点位表发布到 DataManager（UI 的唯一数据来源）。

        返回发布成功的点位数。engine.nodes 内的值是 bootstrap() 刚重新读取过的实时值，
        这里只是搬运，不再额外访问网络。
        """
        nodes = list(getattr(self.engine, "nodes", {}).values())
        if not nodes:
            self._log_empty_node_tree()
            return 0

        for cached in nodes:
            data = {k: v for k, v in cached.items() if k != 'node_obj'}
            self.dm.update_node(cached['node_id'], data)

        self.dm.recalculate_counts()
        self.dm.structure_changed = True
        global_logger.info(f"Published {len(nodes)} nodes to the data bus.")
        return len(nodes)

    def _log_empty_node_tree(self):
        """点位表为空时的提示（协议中立，子类可按协议调整语气）。

        原文案写死"检查 ns 命名空间前缀（UA 专用）"，DA 版删掉最后一个手动点位后
        空表完全正常，用 UA 的排查建议告警会误导现场。
        """
        global_logger.warning(
            "当前点位表为空（node tree is empty）：自动扫描为空且未配置手动点位，"
            "或点位刚被移除；请检查服务器地址空间/手动点位配置。"
        )

    def _handle_connection_lost(self):
        """引擎上报断线（每个连接生命周期只触发一次）。

        重连节奏由引擎的 supervisor 掌握，这里只做两件事：
        1. 让界面与引擎对"是否自动重连"保持一致；
        2. 若用户勾选了自动重连，确保 supervisor 真的在跑（兜底自愈）。
        """
        enabled = self._sync_auto_reconnect_flag()
        if enabled:
            task = getattr(self.engine, "_reconnect_task", None)
            if task is None or task.done():
                global_logger.warning(
                    "Connection lost with auto-reconnect enabled but no supervisor running; re-arming it."
                )
                self._schedule_reconnect_forever()
            else:
                global_logger.warning("Connection lost; auto-reconnect supervisor is already running.")
        else:
            global_logger.warning(
                "Connection lost but auto-reconnect is disabled (复选框未勾选)；"
                "将停留在待连接状态，请手动点击连接。"
            )

    def _on_nodes_recovered(self):
        """连接/重连成功、节点值已重新读取后的回调。"""
        # 引擎 bootstrap() 只把点位放进 engine.nodes，不会主动灌进 DataManager。
        # 正常手动连接由 _connect_and_load() 负责发布；但"冷启动勾选自动重连"或
        # 长时间断网后由 supervisor 自己恢复时，没有任何 UI 连接流程参与，
        # 若这里不发布，界面会出现"已连接但总点位数为 0"。发布操作幂等，可重复调用。
        if self.engine.nodes:
            self._publish_nodes_to_bus()
        self.dm.recalculate_counts()
        self._start_scheduler()
        # 确保自动重连总闸处于开启状态，这样下一次意外断线同样会被自动恢复
        if self._sync_auto_reconnect_flag():
            self._schedule_reconnect_forever()

    def update_heartbeat_status(self):
        """把"链路健康心跳"的存活状态显示到链路状态标签（不产生日志）。

        周期心跳成功记录是 DEBUG 级（按现场要求移出 INFO 分类），默认日志页看不到，
        容易被误判为"没有心跳检测"。这里用引擎的 _last_heartbeat_at 显示"心跳 Xs 前"，
        超时则变色告警，让值守人员在界面上就能确认心跳检测在运行。
        """
        if getattr(self.engine, "state", "") != "connected":
            return
        endpoint = getattr(self, "_connected_endpoint", "") or getattr(
            self.engine, "display_endpoint", "") or ""
        last = getattr(self.engine, "_last_heartbeat_at", 0.0)
        try:
            last = float(last or 0.0)
        except (TypeError, ValueError):
            last = 0.0
        if last <= 0.0:
            # 引擎未暴露心跳时间戳（如 UA 版）：只提示链路健康检测在运行
            self.lbl_conn_status.setText(f"已连接 {endpoint} | 链路健康检测运行中")
            self.lbl_conn_status.setStyleSheet("color: #10b981;")
            return
        age = max(0.0, time.monotonic() - last)
        try:
            interval = float(self.engine._heartbeat_interval())
        except Exception:
            interval = 10.0
        stale_after = max(interval * 3.0, 30.0)
        if age <= stale_after:
            self.lbl_conn_status.setText(
                "已连接 {} | 心跳正常（{:.0f}s 前，每 {:.0f}s 探测）".format(
                    endpoint, age, interval))
            self.lbl_conn_status.setStyleSheet("color: #10b981;")
        else:
            self.lbl_conn_status.setText(
                "已连接 {} | 心跳已 {:.0f}s 未更新（阈值 {:.0f}s），引擎将自动重连".format(
                    endpoint, age, stale_after))
            self.lbl_conn_status.setStyleSheet("color: #f59e0b;")

    def _log_heartbeat_policy(self):
        """连接成功后记录一行心跳策略说明（INFO，只一次，不是周期刷屏）。"""
        try:
            interval = float(self.engine._heartbeat_interval())
        except Exception:
            interval = 10.0
        global_logger.info(
            "链路健康心跳已启动：每约 {:.0f}s 探测一次；成功心跳记录为 DEBUG 级（每 6 次一条），"
            "如需在「实时日志」查看，请把级别切到 DEBUG。设置页链路状态会持续显示“心跳 Xs 前”。".format(
                interval))

    def _on_engine_state_change(self, state, detail):
        self._apply_state(state, detail)

    def _apply_state(self, state, detail=None):
        detail = detail or {}

        main_win = self.window()
        dash = getattr(main_win, 'card_dash_mode', None)

        if state == 'connected':
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("断开服务器连接")
            self.btn_connect.setStyleSheet("color: #ef4444;")
            endpoint = getattr(self.engine, "display_endpoint", None) or f"{self.engine.host}:{self.engine.port}"
            self._connected_endpoint = endpoint
            self.lbl_conn_status.setText(f"已连接 {endpoint}")
            self.lbl_conn_status.setStyleSheet("color: #10b981;")
            if dash:
                dash.value_label.setText(f"已连接: {self.engine.host}")
                dash.value_label.setStyleSheet("color: #10b981;")

        elif state == 'loading':
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("停止连接")
            self.btn_connect.setStyleSheet("color: #f59e0b;")
            self.lbl_conn_status.setText("已建立通道，正在加载节点/订阅...")
            self.lbl_conn_status.setStyleSheet("color: #f59e0b;")
            if dash:
                dash.value_label.setText("正在加载节点...")
                dash.value_label.setStyleSheet("color: #f59e0b;")

        elif state == 'connecting':
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("取消连接")
            self.btn_connect.setStyleSheet("color: #f59e0b;")
            self.lbl_conn_status.setText("正在建立通信连接...")
            self.lbl_conn_status.setStyleSheet("color: #f59e0b;")
            if dash:
                dash.value_label.setText("正在连接...")
                dash.value_label.setStyleSheet("color: #f59e0b;")

        elif state == 'reconnecting':
            attempt = detail.get("attempt", self.engine.reconnect_attempt)
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("停止自动重连")
            self.btn_connect.setStyleSheet("color: #ef4444;")
            self.lbl_conn_status.setText(
                f"链路中断，引擎自动重连中（第 {attempt} 次）— 原因: {detail.get('reason', '未知')}"
            )
            self.lbl_conn_status.setStyleSheet("color: #ef4444;")
            if dash:
                dash.value_label.setText("断开并重连中...")
                dash.value_label.setStyleSheet("color: #ef4444;")

        elif state == 'disconnecting':
            self.btn_connect.setEnabled(False)
            self.btn_connect.setText("正在断开中...")
            self.lbl_conn_status.setText("正在释放会话与连接资源...")
            self.lbl_conn_status.setStyleSheet("color: #f59e0b;")

        else:  # disconnected
            self.btn_connect.setEnabled(True)
            self.btn_connect.setText("连接并挂载服务器节点")
            self.btn_connect.setStyleSheet("")
            self._connected_endpoint = ""
            self.lbl_conn_status.setText("未连接")
            self.lbl_conn_status.setStyleSheet("color: #64748b;")
            if dash:
                dash.value_label.setText("待连接")
                dash.value_label.setStyleSheet("color: #f59e0b;")
