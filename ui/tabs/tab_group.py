try:
    from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                                 QListWidget, QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QListWidgetItem, QTimeEdit, QTableWidget, QTableWidgetItem)
    from PyQt5.QtCore import Qt, QTime
except ImportError:
    from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                                 QListWidget, QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QListWidgetItem, QTimeEdit, QTableWidget, QTableWidgetItem)
    from PySide6.QtCore import Qt, QTime
    
import uuid
import asyncio
from utils.logger import global_logger
from ui.tabs.tab_monitor import MonitorTableModel

class TabGroup(QWidget):
    def __init__(self, data_manager, opc_engine):
        super().__init__()
        self.dm = data_manager
        self.engine = opc_engine
        self._setup_ui()

    def _setup_ui(self):
        main_layout = QHBoxLayout(self)
        
        # ===============================
        # 左侧：分组列表管理
        # ===============================
        left_layout = QVBoxLayout()
        grp_group = QGroupBox("分组管理")
        grp_group_layout = QVBoxLayout(grp_group)
        
        btn_layout = QHBoxLayout()
        self.btn_add_group = QPushButton("新建分组")
        self.btn_rename_group = QPushButton("重命名")
        self.btn_del_group = QPushButton("删除分组")
        self.btn_del_group.setStyleSheet("QPushButton {background-color: #ff4d4f; color: white;} QPushButton:hover {background-color: #ff7875;} QPushButton:pressed {background-color: #d9363e;}")
        
        btn_layout.addWidget(self.btn_add_group)
        btn_layout.addWidget(self.btn_rename_group)
        btn_layout.addWidget(self.btn_del_group)
        
        self.list_groups = QListWidget()
        
        # 批量控制区（底部）
        batch_layout = QVBoxLayout()
        batch_layout.addWidget(QLabel("【勾选组】批量控制:"))
        keys_layout = QHBoxLayout()
        self.btn_batch_on = QPushButton("⚡ 批量全开")
        self.btn_batch_on.setStyleSheet(
            "QPushButton {background-color: #52c41a; color: white; height: 40px; font-weight: bold; border-radius: 4px;}"
            "QPushButton:hover {background-color: #73d13d;}"
            "QPushButton:pressed {background-color: #389e0d;}"
        )
        self.btn_batch_off = QPushButton("💡 批量全关")
        self.btn_batch_off.setStyleSheet(
            "QPushButton {background-color: #f5222d; color: white; height: 40px; font-weight: bold; border-radius: 4px;}"
            "QPushButton:hover {background-color: #ff7875;}"
            "QPushButton:pressed {background-color: #d9363e;}"
        )
        keys_layout.addWidget(self.btn_batch_on)
        keys_layout.addWidget(self.btn_batch_off)
        batch_layout.addLayout(keys_layout)
        
        grp_group_layout.addLayout(btn_layout)
        grp_group_layout.addWidget(self.list_groups)
        grp_group_layout.addLayout(batch_layout)
        
        left_layout.addWidget(grp_group)
        
        # ===============================
        # 右侧：组成员管理 & 定时调度
        # ===============================
        right_layout = QVBoxLayout()
        
        # 右上：成员穿梭框
        grp_members = QGroupBox("组成员管理 (选中分组后配置)")
        members_layout = QHBoxLayout(grp_members)
        
        # 左穿梭（选中的组成员）
        box_in = QVBoxLayout()
        box_in.addWidget(QLabel("组内控制点 (点击移出):"))
        self.tb_in = QTableView()
        box_in.addWidget(self.tb_in)
        
        # 右穿梭（可供添加的成员）
        box_out = QVBoxLayout()
        header_out = QHBoxLayout()
        header_out.addWidget(QLabel("可添加控制点 (点击加入):"))
        header_out.addStretch()
        header_out.addWidget(QLabel("筛选类型:"))
        self.cb_type = QComboBox()
        self.cb_type.addItems(["全部类型", "Boolean", "UInt", "Int", "Real", "String"])
        self.cb_type.currentTextChanged.connect(self.refresh_members)
        header_out.addWidget(self.cb_type)
        
        header_out.addWidget(QLabel(" 搜索:"))
        self.le_search = QLineEdit()
        self.le_search.setPlaceholderText("搜ID或别名...")
        self.le_search.textChanged.connect(self.refresh_members)
        header_out.addWidget(self.le_search)
        
        box_out.addLayout(header_out)
        
        self.tb_out = QTableView()
        box_out.addWidget(self.tb_out)
        
        members_layout.addLayout(box_in)
        members_layout.addLayout(box_out)
        
        # 右下：定时调度
        grp_schedule = QGroupBox("自动定时控制 (对勾选的多个组生效)")
        sched_layout = QVBoxLayout(grp_schedule)
        
        ctrl_layout = QHBoxLayout()
        ctrl_layout.addWidget(QLabel("时间:"))
        self.time_edit = QTimeEdit()
        self.time_edit.setDisplayFormat("HH:mm")
        self.time_edit.setTime(QTime.currentTime())
        ctrl_layout.addWidget(self.time_edit)
        
        ctrl_layout.addWidget(QLabel(" 动作:"))
        self.cb_action = QComboBox()
        self.cb_action.addItems(["开启", "关闭"])
        ctrl_layout.addWidget(self.cb_action)
        
        self.btn_add_sched = QPushButton("增加计划")
        self.btn_del_sched = QPushButton("删除选中")
        self.btn_del_sched.setStyleSheet("QPushButton {color: #ff4d4f;} QPushButton:hover {color: #ff7875;}")
        ctrl_layout.addStretch()
        ctrl_layout.addWidget(self.btn_add_sched)
        ctrl_layout.addWidget(self.btn_del_sched)
        sched_layout.addLayout(ctrl_layout)
        
        self.table_sched = QTableWidget()
        self.table_sched.setColumnCount(5)
        self.table_sched.setHorizontalHeaderLabels(["ID", "目标分组", "触发时间", "动作指令", "状态"])
        header_sched = self.table_sched.horizontalHeader()
        header_sched.setSectionResizeMode(QHeaderView.Interactive)
        header_sched.setStretchLastSection(True)
        self.table_sched.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table_sched.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.table_sched.hideColumn(0) # 隐藏内部ID
        sched_layout.addWidget(self.table_sched)
        
        right_layout.addWidget(grp_members, 6) # 占比6
        right_layout.addWidget(grp_schedule, 4) # 占比4
        
        # 装入全局
        main_layout.addLayout(left_layout, 3) # 左边占3
        main_layout.addLayout(right_layout, 7) # 右边占7
        
        self.model_in = MonitorTableModel(self.dm)
        self.model_out = MonitorTableModel(self.dm)
        self.tb_in.setModel(self.model_in)
        self.tb_out.setModel(self.model_out)
        
        self.tb_in.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.tb_out.setSelectionBehavior(QAbstractItemView.SelectRows)
        
        self.tb_in.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.tb_in.horizontalHeader().setStretchLastSection(True)
        self.tb_out.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.tb_out.horizontalHeader().setStretchLastSection(True)
        
        # 隐藏控制操作列
        self.tb_in.hideColumn(5)
        self.tb_out.hideColumn(5)
        
        self.tb_in.doubleClicked.connect(self.on_move_out)
        self.tb_out.doubleClicked.connect(self.on_move_in)
        
        # 绑定左侧群组信号
        self.btn_add_group.clicked.connect(self.on_add_group)
        self.btn_del_group.clicked.connect(self.on_del_group)
        self.btn_rename_group.clicked.connect(self.on_rename_group)
        self.list_groups.currentItemChanged.connect(self.on_group_selection_changed)
        
        self.btn_batch_on.clicked.connect(lambda: self.on_batch_control(True))
        self.btn_batch_off.clicked.connect(lambda: self.on_batch_control(False))
        
        self.btn_add_sched.clicked.connect(self.on_add_schedule)
        self.btn_del_sched.clicked.connect(self.on_del_schedule)
        
        self.refresh_groups_list()
        self.refresh_schedules()

    def refresh_groups_list(self):
        self.list_groups.clear()
        groups = self.dm.pm.get_groups()
        for g_name in groups.keys():
            item = QListWidgetItem(g_name)
            item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
            item.setCheckState(Qt.Unchecked)
            self.list_groups.addItem(item)
            
    def on_add_group(self):
        text, ok = QInputDialog.getText(self, "新建分组", "请输入分组名称:")
        if ok and text.strip():
            if self.dm.pm.add_group(text.strip()):
                self.refresh_groups_list()
            else:
                QMessageBox.warning(self, "错误", "分组名称已存在或非法。")
                
    def on_del_group(self):
        curr = self.list_groups.currentItem()
        if not curr: return
        name = curr.text()
        
        rep = QMessageBox.question(self, "确认", f"确定要删除分组 {name} 吗？\n(仅解散分组，不影响控制节点)", QMessageBox.Yes | QMessageBox.No)
        if rep == QMessageBox.Yes:
            self.dm.pm.delete_group(name)
            self.refresh_groups_list()
            # 清空右侧选定状态TODO

    def on_rename_group(self):
        curr = self.list_groups.currentItem()
        if not curr: return
        old_name = curr.text()
        
        text, ok = QInputDialog.getText(self, "重命名", "请输入新分组名称:", text=old_name)
        if ok and text.strip() and text.strip() != old_name:
            if self.dm.pm.rename_group(old_name, text.strip()):
                self.refresh_groups_list()
            else:
                 QMessageBox.warning(self, "错误", "操作失败，可能目标名称已存在。")
                 
    def on_group_selection_changed(self, current, previous):
        if not current: 
            self.model_in.beginResetModel()
            self.model_in._data_cache = []
            self.model_in.endResetModel()
            self.model_out.beginResetModel()
            self.model_out._data_cache = []
            self.model_out.endResetModel()
            return
        
        self.refresh_members()
        
    def refresh_members(self):
        curr = self.list_groups.currentItem()
        if not curr: return
        group_name = curr.text()
        
        # 从本地配置拿组员 ID 清单
        member_ids = self.dm.pm.get_groups().get(group_name, [])
        all_nodes = self.dm.get_node_list()
        
        in_nodes = [n for n in all_nodes if n.get('node_id') in member_ids]
        
        # 执行给out_node准备的过滤逻辑
        tf_lower = getattr(self, 'cb_type', None) and self.cb_type.currentText().lower() or "全部类型"
        kw = getattr(self, 'le_search', None) and self.le_search.text().strip().lower() or ""
        type_map = {
            "boolean": ["bool", "boolean"],
            "uint": ["uint", "byte"], 
            "int": ["int", "sbyte"],
            "real": ["float", "double", "real"],
            "string": ["string", "str", "localizedtext"]
        }
        allowed = type_map.get(tf_lower, [tf_lower])
        
        out_nodes = []
        for n in all_nodes:
            if n.get('node_id') in member_ids: continue
            
            n_type = str(n.get('type', '')).lower()
            if tf_lower != "全部类型":
                matched = False
                for t in allowed:
                    if t in n_type:
                        if tf_lower == 'int' and 'uint' in n_type:
                            continue
                        matched = True
                        break
                if not matched: continue
                
            if kw:
                alias = str(n.get('alias', n.get('name', ''))).lower()
                nid_str = str(n.get('node_id', '')).lower()
                if kw not in alias and kw not in nid_str:
                    continue
            out_nodes.append(n)
        
        self.model_in.beginResetModel()
        self.model_in._data_cache = in_nodes
        self.model_in.endResetModel()
        
        self.model_out.beginResetModel()
        self.model_out._data_cache = out_nodes
        self.model_out.endResetModel()

    def on_move_in(self, index):
        curr = self.list_groups.currentItem()
        if not curr or not index.isValid(): return
        group_name = curr.text()
        
        node = self.model_out._data_cache[index.row()]
        nid = node.get('node_id')
        
        members = self.dm.pm.get_groups().get(group_name, [])
        if nid not in members:
            members.append(nid)
            self.dm.pm.update_group_members(group_name, members)
            self.refresh_members()

    def on_move_out(self, index):
        curr = self.list_groups.currentItem()
        if not curr or not index.isValid(): return
        group_name = curr.text()
        
        node = self.model_in._data_cache[index.row()]
        nid = node.get('node_id')
        
        members = self.dm.pm.get_groups().get(group_name, [])
        if nid in members:
            members.remove(nid)
            self.dm.pm.update_group_members(group_name, members)
            self.refresh_members()

    def on_batch_control(self, action_val: bool):
        checked_groups = []
        for i in range(self.list_groups.count()):
            item = self.list_groups.item(i)
            if item.checkState() == Qt.Checked:
                checked_groups.append(item.text())
                
        if not checked_groups:
            # 向下兼容：如果没勾选，但选中了一行，也放行
            curr = self.list_groups.currentItem()
            if curr:
                checked_groups.append(curr.text())
            else:
                QMessageBox.warning(self, "未勾选组", "请先在左侧通过复选框勾选要控的分组。")
                return
            
        if not self.engine.connected:
            QMessageBox.warning(self, "连接中断", "OPC 服务掉线，请前往设置重连。")
            return
            
        combined_member_ids = set()
        for group_name in checked_groups:
            members = self.dm.pm.get_groups().get(group_name, [])
            combined_member_ids.update(members)
        
        if not combined_member_ids:
            QMessageBox.information(self, "空组", "当前指定的分组内没有任何控制点。")
            return
            
        # 并发发送写入指令前作好全量日志记录
        action_name = '全开' if action_val else '全关'
        global_logger.info(f"==> 用户点击了批量 {action_name} 操作 | 受影响的分组: {', '.join(checked_groups)} | 受影响节点数: {len(combined_member_ids)}")
        
        def _task_err_callback(t):
            exc = t.exception()
            if exc:
                global_logger.error(f"批量控制写入失败: {exc}")

        for nid in combined_member_ids:
            task = asyncio.create_task(self.engine.write_node_value(nid, action_val))
            task.add_done_callback(_task_err_callback)

    def refresh_schedules(self):
        self.table_sched.setRowCount(0)
        schedules = self.dm.pm.get_schedules()
        for s in schedules:
            row = self.table_sched.rowCount()
            self.table_sched.insertRow(row)
            
            self.table_sched.setItem(row, 0, QTableWidgetItem(s.get("id", "")))
            self.table_sched.setItem(row, 1, QTableWidgetItem(s.get("group", "")))
            self.table_sched.setItem(row, 2, QTableWidgetItem(s.get("time", "")))
            
            action_text = "开启" if s.get("action") else "关闭"
            self.table_sched.setItem(row, 3, QTableWidgetItem(action_text))
            
            is_enabled = s.get("enabled", True)
            btn_status = QPushButton("🟢 已开启" if is_enabled else "🔴 已关闭")
            if is_enabled:
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #52c41a; border-radius: 3px; padding: 2px; } QPushButton:hover { background-color: #73d13d; }")
            else:
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #8c8c8c; border-radius: 3px; padding: 2px; } QPushButton:hover { background-color: #a6a6a6; }")
                
            # 闭包绑定当前id和前状态
            btn_status.clicked.connect(lambda chk, _id=s.get("id"), curr=is_enabled: self.on_toggle_schedule(_id, curr))
            
            self.table_sched.setCellWidget(row, 4, btn_status)
            
    def on_toggle_schedule(self, sched_id, current_status):
        new_status = not current_status
        self.dm.pm.update_schedule(sched_id, {"enabled": new_status})
        global_logger.info(f"调度计划 {sched_id} 用户操作变更为 -> [{'可用' if new_status else '停用'}]")
        self.refresh_schedules()

    def on_add_schedule(self):
        checked_groups = []
        for i in range(self.list_groups.count()):
            item = self.list_groups.item(i)
            if item.checkState() == Qt.Checked:
                checked_groups.append(item.text())
        
        if not checked_groups:
            curr = self.list_groups.currentItem()
            if curr:
                checked_groups.append(curr.text())
            else:
                QMessageBox.warning(self, "提示", "请在左侧至少勾选一个分组，时间计划必须挂靠在分组上。")
                return
                
        time_str = self.time_edit.time().toString("HH:mm")
        is_on = self.cb_action.currentText() == "开启"
        
        for g_name in checked_groups:
            sched_dict = {
                "id": str(uuid.uuid4())[:8],
                "group": g_name,
                "time": time_str,
                "action": is_on,
                "enabled": True
            }
            self.dm.pm.add_schedule(sched_dict)
            
        self.refresh_schedules()

    def on_del_schedule(self):
        selected = self.table_sched.selectedItems()
        if not selected: return
        
        row = selected[0].row()
        sched_id = self.table_sched.item(row, 0).text()
        
        self.dm.pm.delete_schedule(sched_id)
        self.refresh_schedules()
