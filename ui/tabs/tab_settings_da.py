"""OPC DA 版"设置与连接"页。

继承 UA 版 TabSettings，只替换与协议相关的三处：

1. _build_connection_group()：DCOM 主机、可编辑的 ProgID 下拉框（带"刷新服务器
   列表"按钮，调用 opcda.client.list_servers）、更新周期（ms）、浏览深度；
2. _apply_form_to_engine()：校验并把参数注入 OpcDaClientEngine.configure()；
3. _persist_connection_params()：连接成功后把参数写入 data_store["da_connection"]。

重连策略、界面设置、状态机投影、按钮行为全部复用父类，保证 UA / DA 两版设置页
的行为一致（"断线后自动重连"、"连接/停止重连"按钮语义等）。

COM/枚举说明：list_servers() 可能在远程 DCOM 主机上阻塞数秒，因此放进 daemon
后台线程执行，绝不阻塞 Qt 主线程（主线程还负责 COM 消息泵与订阅回调）。
"""

try:
    from PyQt5.QtWidgets import (
        QGroupBox, QFormLayout, QLineEdit, QComboBox, QPushButton, QLabel,
        QSpinBox, QHBoxLayout, QMessageBox, QListWidget, QListWidgetItem,
    )
    from PyQt5.QtCore import QObject, Qt, pyqtSignal
except ImportError:
    from PySide6.QtWidgets import (
        QGroupBox, QFormLayout, QLineEdit, QComboBox, QPushButton, QLabel,
        QSpinBox, QHBoxLayout, QMessageBox, QListWidget, QListWidgetItem,
    )
    from PySide6.QtCore import QObject, Qt, Signal as pyqtSignal

import asyncio
import re
import threading

from ui.tabs.tab_settings import TabSettings
from utils.logger import global_logger


# 主机名/IP 校验（与 UA 版一致）
_IP_PATTERN = re.compile(r"^(\d{1,3}\.){3}\d{1,3}$")
_HOSTNAME_PATTERN = re.compile(
    r"^[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?"
    r"(\.[a-zA-Z0-9]([a-zA-Z0-9\-]{0,61}[a-zA-Z0-9])?)*$"
)
# OPC ProgID 形如 Matrikon.OPC.Simulation.1 / Kepware.KEPServerEX.V6；
# 部分服务器只登记了 CLSID，因此同时接受 {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX}。
# 普通 ProgID 允许空格、点、下划线、连字符（例如 "Rockwell Automation.Tag.1"）。
_PROGID_PATTERN = re.compile(
    r"^(?:\{[0-9A-Fa-f]{8}(?:-[0-9A-Fa-f]{4}){3}-[0-9A-Fa-f]{12}\}"
    r"|[A-Za-z0-9_\- ]+(?:\.[A-Za-z0-9_\- ]+)+)$"
)

# 枚举任务保活集合：使用普通 daemon 线程而不是 QThread。
# 远程 DCOM / Remote Registry 枚举可能阻塞数十秒；QThread 若在窗口/解释器销毁
# 时仍持有运行中的 Qt 线程，会触发 "QThread: Destroyed while thread is still
# running"（Qt 直接 abort，无法被 Python try/except 捕获）。daemon 线程不持有
# Qt 线程对象，进程退出时由 OS 回收，从根上消除该崩溃路径。
_RUNNING_WORKERS = set()


class _ServerListWorker(QObject):
    """后台枚举 OPC DA 服务器列表（daemon 线程，不阻塞 GUI 线程）。"""

    finished_list = pyqtSignal(list, str)
    finished = pyqtSignal()

    def __init__(self, host, parent=None):
        super().__init__(parent)
        self.host = host
        self._thread = None

    def isRunning(self):
        thread = self._thread
        return bool(thread is not None and thread.is_alive())

    def _release_keepalive_on_gui(self):
        """在 GUI 线程释放保活引用（由 finished 信号排队触发）。"""
        _RUNNING_WORKERS.discard(self)

    def start(self):
        """启动枚举线程；两个 Qt 信号会自动以队列方式回到 GUI 线程。"""
        if self.isRunning():
            return
        _RUNNING_WORKERS.add(self)
        # 线程结束后由 GUI 线程释放保活引用并 deleteLater。QObject 归属 GUI 线程，
        # 绝不能在 daemon 线程里 discard 掉最后一个强引用（否则可能在非所属线程
        # 析构 QObject，与排队中的 deleteLater 竞争）。即使解释器已退出、队列未
        # 处理，daemon 线程也不会阻止进程结束。
        self.finished.connect(self._release_keepalive_on_gui)
        self.finished.connect(self.deleteLater)
        self._thread = threading.Thread(
            target=self._run, name="opcda-server-enum", daemon=True)
        self._thread.start()

    def _run(self):
        try:
            # 延迟导入：opcda.client 依赖 comtypes（仅 Windows）。
            # list_servers 契约要求任何失败返回 []、不抛异常；这里仍兜一层，
            # 以便后端缺失/被裁剪时给出可读错误。
            from opcda.client import list_servers

            servers = list_servers(self.host) or []
            payload = ([str(name) for name in servers], "")
        except Exception as exc:  # pragma: no cover - 依赖真实 COM 环境
            payload = ([], "{}: {}".format(type(exc).__name__, exc))
        try:
            self.finished_list.emit(*payload)
        except Exception:  # pragma: no cover - 接收方已销毁
            pass
        try:
            self.finished.emit()
        except Exception:  # pragma: no cover
            pass
        # 注意：不在这里 _RUNNING_WORKERS.discard(self)。保活引用必须由 GUI 线程
        # 的 _release_keepalive_on_gui() 释放，避免在后台线程析构 QObject。


class DaTabSettings(TabSettings):
    """OPC DA 版设置页。"""

    def __init__(self, opc_engine, data_manager):
        self._server_worker = None
        self._manual_tasks = set()
        # 手动点位在线挂载/删除任务进行中标志：防止快速连击并发创建多个 live 任务。
        self._manual_busy = False
        # 手动点位必须在父类构造（可能调度自动连接）之前从配置读入，
        # 并尽早注入引擎，否则冷启动自动连接的第一轮 bootstrap 会漏挂。
        self._manual_entries = self._normalize_manual_entries(
            data_manager.pm.data_store.get("da_manual_nodes", []))
        super().__init__(opc_engine, data_manager)
        try:
            self.engine.set_manual_items(self._manual_entries)
        except Exception as e:  # pragma: no cover - 防御性
            global_logger.warning("Failed to inject manual OPC DA points: {}".format(e))
        self._refresh_manual_points_list()

    # ----------------------------------------------------------------------------------
    # 连接配置区（覆盖父类）
    # ----------------------------------------------------------------------------------

    def _build_connection_group(self, layout):
        """构建 OPC DA 连接配置区（DCOM 主机 / ProgID / 更新周期 / 浏览深度）。"""
        grp_conn = QGroupBox("OPC DA 服务器连接配置（COM / DCOM）")
        form_conn = QFormLayout(grp_conn)

        saved_conn = self.dm.pm.data_store.get("da_connection", {}) or {}

        # DCOM 主机：本机可填 localhost / 127.0.0.1，远程填主机名或 IP
        self.le_da_host = QLineEdit(saved_conn.get("host", "localhost"))
        self.le_da_host.setMinimumWidth(180)
        self.le_da_host.setToolTip(
            "OPC DA 服务器的 DCOM 主机。\n"
            "本机服务器：localhost 或 127.0.0.1；远程服务器：主机名 / IP（需要 DCOM 权限配置）。"
        )

        # ProgID：可编辑下拉框 + 刷新按钮
        self.cb_prog_id = QComboBox()
        self.cb_prog_id.setEditable(True)
        self.cb_prog_id.setMinimumWidth(240)
        self.cb_prog_id.setToolTip(
            "OPC DA 服务器的 ProgID，例如 Matrikon.OPC.Simulation.1。\n"
            "可从下拉列表选择，也可以直接输入；点击右侧按钮刷新可用的服务器列表。"
        )
        saved_prog_id = saved_conn.get("prog_id") or getattr(
            self.engine, "prog_id", "Matrikon.OPC.Simulation.1"
        )
        saved_servers = list(self.dm.pm.data_store.get("da_servers", []) or [])
        seen = set()
        for name in [saved_prog_id] + saved_servers:
            if name and name not in seen:
                seen.add(name)
                self.cb_prog_id.addItem(name)
        self.cb_prog_id.setCurrentText(saved_prog_id)

        self.btn_refresh_servers = QPushButton("刷新服务器列表")
        self.btn_refresh_servers.setToolTip(
            "通过 OPCEnum / 注册表枚举指定 DCOM 主机上的 OPC DA 服务器。\n"
            "远程枚举需要目标机器启用 OPCEnum 与 DCOM 权限。"
        )
        self.btn_refresh_servers.clicked.connect(self._on_refresh_servers)

        prog_row = QHBoxLayout()
        prog_row.setContentsMargins(0, 0, 0, 0)
        prog_row.setSpacing(6)
        prog_row.addWidget(self.cb_prog_id, 1)
        prog_row.addWidget(self.btn_refresh_servers)

        # 枚举状态（数量 / 失败原因），刷新按钮的反馈不依赖模态弹窗
        self.lbl_server_status = QLabel()
        self.lbl_server_status.setWordWrap(True)
        self.lbl_server_status.setStyleSheet("color: #64748b;")
        if saved_servers:
            self.lbl_server_status.setText(
                "已保存 {} 个枚举结果；点击\"刷新服务器列表\"重新枚举，也可直接手工输入 ProgID。"
                .format(len(saved_servers))
            )
        else:
            self.lbl_server_status.setText(
                "点击\"刷新服务器列表\"枚举目标主机上的 OPC DA 服务器；失败时可直接手工输入 ProgID。"
            )

        # 更新周期：订阅 / SyncRead 轮询的采样间隔
        self.sb_update_rate = QSpinBox()
        self.sb_update_rate.setRange(100, 60000)
        self.sb_update_rate.setSingleStep(100)
        self.sb_update_rate.setSuffix(" ms")
        self.sb_update_rate.setToolTip(
            "点位刷新周期（订阅或降级轮询的采样间隔）。\n"
            "500ms 适合灯光控制；网络较差时可放宽到 1000-2000ms。"
        )
        self.sb_update_rate.setValue(
            int(saved_conn.get("update_rate_ms", getattr(self.engine, "update_rate_ms", 500)))
        )
        # 关键顺序：先 setValue 再 connect，避免构造时把持久化值当成用户修改而误触发。
        # 已连接时用户改更新周期必须立即生效，否则要重启程序才能应用。
        self.sb_update_rate.valueChanged.connect(self._on_update_rate_changed)

        # 浏览深度：地址空间递归层数
        self.sb_browse_depth = QSpinBox()
        self.sb_browse_depth.setRange(1, 12)
        self.sb_browse_depth.setToolTip(
            "地址空间浏览的最大递归深度。点位层级较深（如 Device.Channel.Tag）时可调大。"
        )
        self.sb_browse_depth.setValue(
            int(saved_conn.get("browse_max_depth", getattr(self.engine, "browse_max_depth", 3)))
        )

        form_conn.addRow("DCOM 主机 (Host):", self.le_da_host)
        form_conn.addRow("OPC Server ProgID:", prog_row)
        form_conn.addRow("服务器列表:", self.lbl_server_status)
        form_conn.addRow("更新周期:", self.sb_update_rate)
        form_conn.addRow("浏览深度:", self.sb_browse_depth)

        # 按钮容器（与 UA 版保持一致的位置与交互）
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

    def _on_update_rate_changed(self, value):
        """更新周期热更新：已连接时立即调整底层订阅/轮询的采样间隔。

        离线时只改界面值即可（连接成功后 _persist_connection_params 会落盘）；
        24/7 现场改完周期不应要求重启程序，因此已连接时调用 engine.configure()
        让引擎把新周期下发给底层 client。
        """
        requested = int(value)
        if not getattr(self.engine, "connected", False):
            return
        try:
            self.engine.configure(update_rate_ms=requested)
        except Exception as e:
            global_logger.error("热更新 OPC DA 更新周期失败: {}".format(e), exc_info=True)
            self.lbl_conn_status.setText("更新周期应用失败：{}".format(e))
            return
        # 服务器可能拒绝/钳制请求值（SetState 返回 RevisedUpdateRate）；持久化与
        # 提示必须以引擎实际生效值为准，不能把输入值当成已生效值。
        try:
            effective = int(getattr(self.engine, "update_rate_ms", requested) or requested)
        except (TypeError, ValueError):
            effective = requested
        conn = dict(self.dm.pm.data_store.get("da_connection", {}) or {})
        conn["update_rate_ms"] = effective
        self.dm.pm.data_store["da_connection"] = conn
        self.dm.pm.save()
        endpoint = getattr(self.engine, "display_endpoint", "") or ""
        prefix = "已连接 {} | ".format(endpoint) if endpoint else ""
        if effective == requested:
            self.lbl_conn_status.setText("{}更新周期已应用 {}ms".format(prefix, effective))
        else:
            self.lbl_conn_status.setText(
                "{}更新周期已应用 {}ms（请求 {}ms，服务器已确认）".format(
                    prefix, effective, requested))
        global_logger.info(
            "OPC DA 更新周期热更新：请求 {}ms，实际生效 {}ms".format(requested, effective))


    # ----------------------------------------------------------------------------------
    # 手动添加控制点位（自动扫描之外的补充）
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _normalize_manual_entries(raw):
        """兼容字符串/字典两种历史写法，去空去重。"""
        entries = []
        seen = set()
        for entry in raw or []:
            if isinstance(entry, str):
                item_id = entry.strip()
                name = item_id
            elif isinstance(entry, dict):
                item_id = str(entry.get("item_id") or entry.get("id") or "").strip()
                name = str(entry.get("name") or item_id).strip() or item_id
            else:
                continue
            if not item_id or item_id in seen:
                continue
            seen.add(item_id)
            entries.append({"item_id": item_id, "name": name})
        return entries

    def _build_extra_groups(self, layout):
        """在 DA 连接配置区之后追加"手动添加控制点位"区。"""
        grp_manual = QGroupBox("手动添加控制点位（自动扫描之外的补充）")
        form_manual = QFormLayout(grp_manual)

        hint = QLabel(
            "自动扫描会遍历服务器地址空间；如果某个点位没有被扫描到（FLAT 命名空间、"
            "浏览受限的分支、私有标签等），可在这里输入 OPC ItemID 单独挂载。\n"
            "手动点位会保存到配置，并在每次连接/重连时自动 AddItems 和订阅。")
        hint.setWordWrap(True)
        hint.setStyleSheet("color: #64748b; font-size: 12px;")
        form_manual.addRow("", hint)

        self.le_manual_item_id = QLineEdit()
        self.le_manual_item_id.setMinimumWidth(240)
        self.le_manual_item_id.setPlaceholderText("例如 Random.Int1 / Device.Channel.Tag")
        self.le_manual_item_id.setToolTip("OPC DA 的完整 ItemID（区分大小写，通常用点分隔层级）。")
        self.le_manual_item_id.returnPressed.connect(self._on_add_manual_point)

        self.btn_manual_add = QPushButton("添加点位")
        self.btn_manual_add.setMinimumHeight(32)
        self.btn_manual_add.clicked.connect(self._on_add_manual_point)

        id_row = QHBoxLayout()
        id_row.setContentsMargins(0, 0, 0, 0)
        id_row.setSpacing(6)
        id_row.addWidget(self.le_manual_item_id, 1)
        id_row.addWidget(self.btn_manual_add)
        form_manual.addRow("OPC ItemID:", id_row)

        self.le_manual_name = QLineEdit()
        self.le_manual_name.setPlaceholderText("可选；留空则使用 ItemID 作为显示名")
        form_manual.addRow("显示名称:", self.le_manual_name)

        self.list_manual_points = QListWidget()
        self.list_manual_points.setMinimumHeight(96)
        self.list_manual_points.setToolTip("已保存的手动点位；点击选中后可按下方按钮删除。")
        form_manual.addRow("已保存:", self.list_manual_points)

        self.btn_manual_remove = QPushButton("删除选中")
        self.btn_manual_remove.clicked.connect(self._on_remove_manual_point)

        btn_row = QHBoxLayout()
        btn_row.setContentsMargins(0, 0, 0, 0)
        btn_row.addStretch()
        btn_row.addWidget(self.btn_manual_remove)
        form_manual.addRow("", btn_row)

        self.lbl_manual_status = QLabel("手动点位会保存，并在每次连接时自动挂载和订阅。")
        self.lbl_manual_status.setWordWrap(True)
        self.lbl_manual_status.setStyleSheet("color: #64748b;")
        form_manual.addRow("状态:", self.lbl_manual_status)

        layout.addWidget(grp_manual)
        self._refresh_manual_points_list()
        return grp_manual

    def _refresh_manual_points_list(self):
        widget = getattr(self, "list_manual_points", None)
        if widget is None:
            return
        widget.clear()
        for entry in self._manual_entries:
            item_id = entry["item_id"]
            name = entry.get("name") or item_id
            text = name if name == item_id else "{}  [{}]".format(name, item_id)
            item = QListWidgetItem(text)
            item.setData(Qt.UserRole, item_id)
            item.setToolTip("ItemID: {}".format(item_id))
            widget.addItem(item)

    def _save_manual_points(self):
        self.dm.pm.data_store["da_manual_nodes"] = [dict(entry) for entry in self._manual_entries]
        self.dm.pm.save()

    def _spawn_manual_task(self, coro, on_done=None):
        """把手动点位的异步挂载/删除交给事件循环，并保留强引用避免被 GC。

        :param on_done: 任务结束（成功 / 失败 / 取消）后的回调。用于恢复按钮可用
            状态；原先只 discard 强引用，异常路径没有机会恢复 UI。
        :return: 创建成功的 Task；事件循环未就绪时返回 None。
        """
        try:
            loop = asyncio.get_event_loop()
            task = loop.create_task(coro)
        except RuntimeError as e:
            global_logger.warning("Event loop not ready for manual point task: {}".format(e))
            try:
                coro.close()
            except Exception:
                pass
            return None
        self._manual_tasks.add(task)

        def _finish(t):
            self._manual_tasks.discard(t)
            if on_done is not None:
                try:
                    on_done(t)
                except Exception as e:  # pragma: no cover - 防御性
                    global_logger.warning("Manual point done-callback failed: {}".format(e))

        task.add_done_callback(_finish)
        return task

    def _set_manual_busy(self, busy):
        """手动点位 live 任务进行中禁用「添加点位 / 删除选中」两个按钮。"""
        self._manual_busy = bool(busy)
        for name in ("btn_manual_add", "btn_manual_remove"):
            btn = getattr(self, name, None)
            if btn is not None:
                btn.setEnabled(not self._manual_busy)

    def _on_manual_task_done(self, task=None):
        """手动点位 live 任务结束（含异常/取消）后恢复按钮。"""
        self._set_manual_busy(False)

    def _on_add_manual_point(self):
        if self._manual_busy:
            # 上一个挂载/删除任务还没结束：忽略连击，避免并发创建多个 live 任务。
            return
        item_id = self.le_manual_item_id.text().strip()
        if not item_id:
            QMessageBox.warning(self, "ItemID 为空", "请输入 OPC DA 的完整 ItemID，例如 Random.Int1。")
            return
        if any(entry["item_id"] == item_id for entry in self._manual_entries):
            QMessageBox.information(self, "已存在", "该 ItemID 已经在手动点位列表中。")
            return
        name = self.le_manual_name.text().strip() or item_id
        self._manual_entries.append({"item_id": item_id, "name": name})
        self.engine.set_manual_items(self._manual_entries)
        self._save_manual_points()
        self._refresh_manual_points_list()
        self.le_manual_item_id.clear()
        self.le_manual_name.clear()

        if self.engine.connected:
            self.lbl_manual_status.setText("正在向服务器挂载 {} ...".format(item_id))
            self._set_manual_busy(True)
            task = self._spawn_manual_task(
                self._add_manual_point_live(item_id, name), self._on_manual_task_done)
            if task is None:  # 事件循环未就绪：没有任务可等，立刻恢复按钮
                self._set_manual_busy(False)
        else:
            # 离线添加只改配置，不联网、不置忙，保持原有行为。
            self.lbl_manual_status.setText("已保存；连接成功后会自动挂载 {}".format(item_id))
        global_logger.info("Manual OPC DA point saved: {} ({})".format(item_id, name))

    async def _add_manual_point_live(self, item_id, name):
        try:
            node = await self.engine.add_manual_node(item_id, name)
            self._publish_nodes_to_bus()
            self._refresh_manual_points_list()
            self.lbl_manual_status.setText(
                "已挂载 {}：类型={}，当前值={!r}".format(
                    item_id, node.get("type"), node.get("value")))
            global_logger.info(
                "[MANUAL] OPC DA point live-mounted: {} ({})".format(item_id, name))
        except Exception as e:
            global_logger.error(
                "Manual OPC DA point mount failed for {}: {}".format(item_id, e), exc_info=True)
            self.lbl_manual_status.setText(
                "挂载失败：{}（已保存，可删除后重试）".format(e))

    def _on_remove_manual_point(self):
        if self._manual_busy:
            # 与添加共用同一个 busy 标志：连击时不会并发创建多个 RemoveItems 任务。
            return
        selected = self.list_manual_points.selectedItems()
        if not selected:
            QMessageBox.information(self, "未选择点位", "请先在上方列表中选择要删除的手动点位。")
            return
        item_ids = [item.data(Qt.UserRole) for item in selected]
        self._manual_entries = [
            entry for entry in self._manual_entries if entry["item_id"] not in item_ids]
        self.engine.set_manual_items(self._manual_entries)
        self._save_manual_points()
        self._refresh_manual_points_list()
        for item_id in item_ids:
            self.dm.remove_node(item_id)
        # 即使当前未连接，也要让引擎从 nodes 中移除（断线期间删除的点位不能在
        # 下次连接时被 AddItems 复活）；引擎内部会自动判断是否需要 RemoveItems。
        self.lbl_manual_status.setText("正在移除 {} ...".format(", ".join(item_ids)))
        self._set_manual_busy(True)
        task = self._spawn_manual_task(
            self._remove_manual_points_live(item_ids), self._on_manual_task_done)
        if task is None:  # 事件循环未就绪：立刻恢复按钮
            self._set_manual_busy(False)
        global_logger.info("Manual OPC DA point(s) removed from config: {}".format(item_ids))

    async def _remove_manual_points_live(self, item_ids):
        failed = []
        for item_id in item_ids:
            try:
                await self.engine.remove_manual_node(item_id)
            except Exception as e:
                failed.append("{}: {}".format(item_id, e))
                global_logger.warning(
                    "Manual OPC DA point remove failed for {}: {}".format(item_id, e))
        self._publish_nodes_to_bus()
        if failed:
            self.lbl_manual_status.setText("部分点位移除失败：{}".format("; ".join(failed)))
        else:
            self.lbl_manual_status.setText("已从服务器移除：{}".format(", ".join(item_ids)))

    # ----------------------------------------------------------------------------------
    # 服务器列表刷新
    # ----------------------------------------------------------------------------------

    def _on_refresh_servers(self):
        host = self.le_da_host.text().strip() or "localhost"
        if not (self._is_valid_host(host)):
            self._fail("地址错误", "请输入合法的 IP 地址或主机名。")
            return

        if self._server_worker is not None and self._server_worker.isRunning():
            return

        self.btn_refresh_servers.setEnabled(False)
        self.btn_refresh_servers.setText("正在枚举...")
        self.lbl_server_status.setStyleSheet("color: #f59e0b;")
        self.lbl_server_status.setText(
            "正在枚举主机 {} 上的 OPC DA 服务器（本机 OPCEnum/注册表，远程 OPCEnum/Remote Registry）..."
            .format(host))
        global_logger.info("Enumerating OPC DA servers on host {} ...".format(host))

        # parent=None + 模块级保活集合：线程对象活到 run() 返回。关闭主窗口时
        # Qt 不会随父对象销毁仍在枚举远程 DCOM/Remote Registry 的线程，从而避免
        # "QThread: Destroyed while thread is still running" 崩溃。
        worker = _ServerListWorker(host)
        worker.finished_list.connect(self._on_server_list_ready)
        worker.finished.connect(self._on_server_worker_finished)
        self._server_worker = worker
        self._server_scan_host = host  # 结果回显用，避免依赖 worker 生命周期
        _RUNNING_WORKERS.add(worker)
        worker.start()

    def _on_server_list_ready(self, servers, error):
        current = self.cb_prog_id.currentText().strip()
        # 用本次实际扫描的主机回显结果：枚举期间用户可能已改输入框，若再读控件
        # 会出现"提示说 A 主机、实际扫的是 B 主机"的误导。
        worker = self._server_worker
        host = (worker.host if worker is not None
                else getattr(self, "_server_scan_host", "") or "(未知主机)")
        if servers:
            existing = {self.cb_prog_id.itemText(i) for i in range(self.cb_prog_id.count())}
            for name in servers:
                if name not in existing:
                    self.cb_prog_id.addItem(name)
            # 保留用户当前输入；若为空则选第一个枚举到的服务器
            if current:
                self.cb_prog_id.setCurrentText(current)
            else:
                self.cb_prog_id.setCurrentIndex(0)
            self.dm.pm.data_store["da_servers"] = list(servers)
            self.dm.pm.save()
            preview = "、".join(servers[:5])
            if len(servers) > 5:
                preview += " 等"
            self.lbl_server_status.setStyleSheet("color: #10b981;")
            self.lbl_server_status.setText(
                "已从 {} 枚举到 {} 个 OPC DA 服务器：{}".format(
                    host, len(servers), preview))
            global_logger.info(
                "OPC DA server enumeration found {} server(s): {}".format(
                    len(servers), ", ".join(servers[:10]))
            )
        else:
            detail = str(error).strip() if error else "枚举结果为空"
            self.lbl_server_status.setStyleSheet("color: #ef4444;")
            self.lbl_server_status.setText(
                "未从 {} 枚举到服务器（{}）；仍可直接手工输入 ProgID。".format(
                    host, detail))
            global_logger.warning(
                "No OPC DA server found on the target host ({}): {}. "
                "Check OPCEnum/OPC.ServerList、Remote Registry、DCOM 权限与服务器是否已启动。"
                .format(host, detail)
            )
            self._show_info(
                "未发现服务器",
                "未枚举到 OPC DA 服务器。\n\n"
                "请检查：\n"
                "1. 目标机器已安装 OPC Core Components（OPCEnum / OPC.ServerList）；\n"
                "2. 远程枚举需要目标服务器安装 OPC Core Components（OPCEnum/OPC.ServerList）"
                "或启用 Remote Registry 服务；\n"
                "3. 服务器 ProgID 已注册（本机可在组件服务中查看）；\n"
                "4. 远程连接时 DCOM 权限/防火墙已放行；\n"
                "5. 也可以直接在 ProgID 输入框中手工填写。"
                + ("\n\n枚举错误：{}".format(detail) if error else ""),
            )

    def _on_server_worker_finished(self):
        """枚举线程结束：从保活集合移除、安排销毁，并恢复"刷新服务器列表"按钮。"""
        worker = self._server_worker
        self._server_worker = None
        if worker is not None:
            _RUNNING_WORKERS.discard(worker)
            try:
                worker.deleteLater()
            except RuntimeError:  # 对象已随窗口销毁
                pass
        try:
            self.btn_refresh_servers.setEnabled(True)
            self.btn_refresh_servers.setText("刷新服务器列表")
        except RuntimeError:  # 退出阶段控件已销毁：忽略
            pass

    def shutdown_background_tasks(self):
        """由顶层窗口关闭流程调用：解绑枚举回调并取消手动点位任务。

        QTabWidget 子页面通常收不到主窗口的 closeEvent，因此不能依赖
        DaTabSettings.closeEvent；由 MainWindow.closeEvent 显式调用本方法，
        避免枚举/手动点位回调进入已销毁的界面。
        """
        worker = self._server_worker
        self._server_worker = None
        if worker is not None:
            for signal, slot in (
                    (worker.finished_list, self._on_server_list_ready),
                    (worker.finished, self._on_server_worker_finished)):
                try:
                    signal.disconnect(slot)
                except (TypeError, RuntimeError):
                    pass
        for task in list(self._manual_tasks):
            try:
                task.cancel()
            except Exception:
                pass
        self._manual_tasks.clear()

    def closeEvent(self, event):
        """关闭设置页时解绑枚举结果信号，绝不同步等待后台线程。"""
        worker = self._server_worker
        if worker is not None:
            try:
                worker.finished_list.disconnect(self._on_server_list_ready)
            except (TypeError, RuntimeError):
                pass
        super().closeEvent(event)

    def _log_empty_node_tree(self):
        """DA 版点位表为空多数是正常状态（例如刚删掉最后一个手动点位）。

        父类给的是 WARNING 并要求排查"命名空间前缀（UA 专用）"，对 DA 会误导现场；
        这里降为 info，只提示点位数空的真实原因与可继续的操作。
        """
        global_logger.info(
            "当前没有点位：自动扫描为空且未配置手动点位，或点位刚被移除；"
            "DA 版删除手动点位后空表属于正常，可继续添加点位或刷新服务器列表。"
        )

    # ----------------------------------------------------------------------------------
    # 表单校验 / 注入 / 持久化（覆盖父类）
    # ----------------------------------------------------------------------------------

    def _apply_form_to_engine(self, interactive=True):
        """校验 DA 连接参数并注入引擎，返回是否通过。"""
        host = self.le_da_host.text().strip()
        if not self._is_valid_host(host):
            return self._fail("地址错误", "请输入合法的 IP 地址或主机名。", interactive=interactive)

        prog_id = self.cb_prog_id.currentText().strip()
        if not prog_id or not _PROGID_PATTERN.match(prog_id):
            return self._fail(
                "ProgID 错误",
                "请输入合法的 OPC Server ProgID（例如 Matrikon.OPC.Simulation.1），"
                "或 {XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX} 形式的 CLSID。",
                interactive=interactive,
            )

        try:
            update_rate_ms = int(self.sb_update_rate.value())
            browse_max_depth = int(self.sb_browse_depth.value())
        except (TypeError, ValueError):
            return self._fail(
                "参数错误", "更新周期/浏览深度必须是整数。", interactive=interactive
            )

        self.engine.configure(
            host=host,
            prog_id=prog_id,
            update_rate_ms=update_rate_ms,
            browse_max_depth=browse_max_depth,
            manual_items=self._manual_entries,
        )
        return True

    def _persist_connection_params(self):
        """连接成功后持久化 DA 连接参数（键名 da_connection）。"""
        self.dm.pm.data_store["da_connection"] = {
            "host": self.le_da_host.text().strip(),
            "prog_id": self.cb_prog_id.currentText().strip(),
            "update_rate_ms": int(self.sb_update_rate.value()),
            "browse_max_depth": int(self.sb_browse_depth.value()),
        }
        self.dm.pm.save()

    # ----------------------------------------------------------------------------------
    # 辅助
    # ----------------------------------------------------------------------------------

    @staticmethod
    def _is_valid_host(host):
        return bool(host) and bool(
            _IP_PATTERN.match(host) or _HOSTNAME_PATTERN.match(host)
        )

    def _fail(self, title, message, interactive=True):
        if interactive:
            QMessageBox.warning(self, title, message)
        else:
            global_logger.error("连接参数无效（{}）: {}".format(title, message))
        return False

    def _show_info(self, title, message):
        # 只在用户主动点击"刷新服务器列表"后弹窗，不会在自动连接流程中阻塞
        QMessageBox.information(self, title, message)

    def _schedule_reconnect_forever(self):
        """打开自治重连总闸前，先把设置页上的 DA 参数注入引擎。

        父类只在存在 le_host（UA 版控件）时注入；DA 版的控件名不同，因此在这里
        显式注入，避免"冷启动勾选自动重连时引擎用默认 localhost / Matrikon 去连，
        而界面上的目标其实是另一台服务器"的错位。
        """
        if hasattr(self, "le_da_host"):
            self._apply_form_to_engine(interactive=False)
        super()._schedule_reconnect_forever()
