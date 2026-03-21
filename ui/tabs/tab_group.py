try:
    from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                                 QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QTimeEdit, QTableWidget, QTableWidgetItem, QFileDialog, QCheckBox, QTreeWidget, QTreeWidgetItem)
    from PyQt5.QtCore import Qt, QTime
except ImportError:
    from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton, 
                                 QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QTimeEdit, QTableWidget, QTableWidgetItem, QFileDialog, QCheckBox, QTreeWidget, QTreeWidgetItem)
    from PySide6.QtCore import Qt, QTime
    
import uuid
import asyncio
import re
import pandas as pd
from utils.logger import global_logger
from utils.filter_helper import filter_nodes
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
        self.btn_add_group = QPushButton("新建一级分组")
        self.btn_add_sub = QPushButton("新建子分组")
        self.btn_rename_group = QPushButton("重命名")
        self.btn_del_group = QPushButton("删除分组")
        self.btn_del_group.setStyleSheet("QPushButton {background-color: #ff4d4f; color: white;} QPushButton:hover {background-color: #ff7875;} QPushButton:pressed {background-color: #d9363e;}")
        
        btn_layout_io = QHBoxLayout()
        self.btn_export_group = QPushButton("导出配置")
        self.btn_import_group = QPushButton("导入配置")
        btn_layout_io.addWidget(self.btn_export_group)
        btn_layout_io.addWidget(self.btn_import_group)
        
        btn_layout.addWidget(self.btn_add_group)
        btn_layout.addWidget(self.btn_add_sub)
        btn_layout.addWidget(self.btn_rename_group)
        btn_layout.addWidget(self.btn_del_group)
        
        self.tree_groups = QTreeWidget()
        self.tree_groups.setHeaderLabel("照明分组架构 (三级)")
        
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
        grp_group_layout.addLayout(btn_layout_io)
        grp_group_layout.addWidget(self.tree_groups)
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
        
        # 星期复选框列
        week_layout = QHBoxLayout()
        week_layout.addWidget(QLabel(" 重复:"))
        self.week_checkboxes = []
        days = ["一", "二", "三", "四", "五", "六", "日"]
        for i, day in enumerate(days):
            cb = QCheckBox(day)
            cb.setChecked(True) # 默认全选
            cb.setMinimumWidth(40)
            self.week_checkboxes.append(cb)
            week_layout.addWidget(cb)
        week_layout.addStretch()
        sched_layout.addLayout(week_layout)
        
        self.table_sched = QTableWidget()
        self.table_sched.setColumnCount(6)
        self.table_sched.setHorizontalHeaderLabels(["ID", "目标分组", "触发时间", "重复日期", "动作指令", "状态"])
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
        self.btn_add_group.clicked.connect(lambda: self.on_add_group(is_sub=False))
        self.btn_add_sub.clicked.connect(lambda: self.on_add_group(is_sub=True))
        self.btn_del_group.clicked.connect(self.on_del_group)
        self.btn_rename_group.clicked.connect(self.on_rename_group)
        self.btn_export_group.clicked.connect(self.on_export_groups)
        self.btn_import_group.clicked.connect(self.on_import_groups)
        self.tree_groups.currentItemChanged.connect(self.on_group_selection_changed)
        
        self.btn_batch_on.clicked.connect(lambda: self.on_batch_control(True))
        self.btn_batch_off.clicked.connect(lambda: self.on_batch_control(False))
        
        self.btn_add_sched.clicked.connect(self.on_add_schedule)
        self.btn_del_sched.clicked.connect(self.on_del_schedule)
        
        self.refresh_groups_list()
        self.refresh_schedules()

    def refresh_groups_list(self):
        """重新构建树形分组结构"""
        selected_id = None
        curr = self.tree_groups.currentItem()
        if curr:
            selected_id = curr.data(0, Qt.UserRole)
            
        self.tree_groups.clear()
        groups = self.dm.pm.get_groups()
        
        # 构建 ID 到 item 的映射
        items_map = {}
        
        # 第一轮：创建所有顶级节点
        for g in groups:
            if g.get("parent_id") is None:
                item = QTreeWidgetItem(self.tree_groups)
                item.setText(0, g["name"])
                item.setData(0, Qt.UserRole, g["id"])
                item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                item.setCheckState(0, Qt.Unchecked)
                items_map[g["id"]] = item
        
        # 多轮尝试（支持嵌套，虽然逻辑限制了三级）
        for _ in range(5):
            added_any = False
            for g in groups:
                if g["id"] in items_map: continue
                pid = g.get("parent_id")
                if pid in items_map:
                    parent_item = items_map[pid]
                    item = QTreeWidgetItem(parent_item)
                    item.setText(0, g["name"])
                    item.setData(0, Qt.UserRole, g["id"])
                    item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                    item.setCheckState(0, Qt.Unchecked)
                    items_map[g["id"]] = item
                    added_any = True
            if not added_any: break
            
        self.tree_groups.expandAll()
        
        # 恢复选中
        if selected_id:
            for i in range(self.tree_groups.topLevelItemCount()):
                item = self.tree_groups.topLevelItem(i)
                if self._find_and_select_item(item, selected_id):
                    break

    def _find_and_select_item(self, item, target_id):
        if item.data(0, Qt.UserRole) == target_id:
            self.tree_groups.setCurrentItem(item)
            return True
        for i in range(item.childCount()):
            if self._find_and_select_item(item.child(i), target_id):
                return True
        return False
            
    def on_add_group(self, is_sub=False):
        parent_id = None
        current_depth = 0
        
        if is_sub:
            curr = self.tree_groups.currentItem()
            if not curr:
                QMessageBox.warning(self, "提示", "请先选中一个父分组。")
                return
            parent_id = curr.data(0, Qt.UserRole)
            # 计算深度
            temp = curr
            while temp:
                current_depth += 1
                temp = temp.parent()
            
            if current_depth >= 3:
                QMessageBox.warning(self, "深度限制", "分组深度最高支持 3 级（例如：区域 > 楼层 > 房间）。")
                return
        
        title = "新建子分组" if is_sub else "新建一级分组"
        text, ok = QInputDialog.getText(self, title, "请输入分组名称:")
        if ok and text.strip():
            self.dm.pm.add_group(text.strip(), parent_id)
            self.refresh_groups_list()
                
    def on_del_group(self):
        curr = self.tree_groups.currentItem()
        if not curr: return
        group_id = curr.data(0, Qt.UserRole)
        name = curr.text(0)
        
        rep = QMessageBox.question(self, "确认", f"确定要删除分组 {name} 及其所有子分组吗？\n(仅解散分组，不影响控制节点)", QMessageBox.Yes | QMessageBox.No)
        if rep == QMessageBox.Yes:
            self.dm.pm.delete_group(group_id)
            self.refresh_groups_list()

    def on_rename_group(self):
        curr = self.tree_groups.currentItem()
        if not curr: return
        group_id = curr.data(0, Qt.UserRole)
        old_name = curr.text(0)
        
        text, ok = QInputDialog.getText(self, "重命名", "请输入新分组名称:", text=old_name)
        if ok and text.strip() and text.strip() != old_name:
            if self.dm.pm.rename_group(group_id, text.strip()):
                self.refresh_groups_list()

    def on_export_groups(self):
        groups = self.dm.pm.get_groups()
        if not groups:
            QMessageBox.information(self, "提示", "目前没有任何分组可供导出。")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出分组配置", "lighting_groups_export.xlsx", "Excel Files (*.xlsx)"
        )
        if not file_path:
            return
            
        try:
            export_rows = []
            for g in groups:
                g_name = g["name"]
                for full_id in g.get("nodes", []):
                    # [V1.2.0 修订] 仅导出标识符的具体值内容，不含 ns=2;s= 前缀
                    match = re.search(r';[isgb]=(.+)', full_id)
                    short_id = match.group(1) if match else full_id
                    export_rows.append({
                        "分组名称": g_name,
                        "节点标识": short_id
                    })
            
            if not export_rows:
                # 如果有空组但也想导出列名
                df = pd.DataFrame(columns=["分组名称", "节点标识"])
            else:
                df = pd.DataFrame(export_rows)
                
            df.to_excel(file_path, index=False)
            QMessageBox.information(self, "导出成功", f"成功导出 {len(groups)} 个分组配置到 Excel。")
        except Exception as e:
            global_logger.error(f"Failed to export Excel: {e}")
            QMessageBox.critical(self, "异常", f"导出失败: {e}")

    def on_import_groups(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "导入分组配置", "", "Excel Files (*.xlsx)"
        )
        if not file_path:
            return
            
        try:
            df = pd.read_excel(file_path)
            if df.empty or "分组名称" not in df.columns or "节点标识" not in df.columns:
                raise ValueError("Excel 格式不正确，必须包含 '分组名称' 和 '节点标识' 列。")

            # 获取当前系统中所有全量 ID 映射
            all_nodes = self.dm.get_node_list()
            id_map = {} # short_id/string_id -> full_id
            for node in all_nodes:
                full_id = node.get('node_id', '')
                match = re.search(r';[isgb]=(.+)', full_id)
                if match:
                    id_map[match.group(1)] = full_id
                else:
                    id_map[full_id] = full_id # 兜底逻辑

            imported_data = {} # group -> [full_ids]
            for _, row in df.iterrows():
                g_name = str(row["分组名称"]).strip()
                raw_id = str(row["节点标识"]).strip()
                if not g_name or not raw_id: continue
                clean_id = raw_id[:-2] if raw_id.endswith('.0') else raw_id
                full_id = id_map.get(clean_id) or id_map.get(raw_id)
                if full_id:
                    if g_name not in imported_data:
                        imported_data[g_name] = []
                    imported_data[g_name].append(full_id)

            current_groups = self.dm.pm.get_groups() # 这是一个列表了
            imported_count = 0
            
            # 由于目前导入暂不处理树形层级（全部作为一级处理），为了向后兼容
            for g_name, member_list in imported_data.items():
                target_g = None
                for g in current_groups:
                    if g["name"] == g_name:
                        target_g = g
                        break
                        
                if target_g:
                    # 并集去重合并 — 直接修改内存中的分组数据，最后统一保存
                    merged_members = list(set(target_g.get("nodes", []) + member_list))
                    target_g["nodes"] = merged_members
                else:
                    new_id = str(uuid.uuid4())
                    new_group = {
                        "id": new_id,
                        "name": g_name,
                        "parent_id": None,
                        "nodes": member_list
                    }
                    self.dm.pm.data_store["groups"].append(new_group)
                imported_count += 1
                
            # 批量操作完成后一次性持久化，避免 N 次磁盘 IO
            self.dm.pm.save()
            self.refresh_groups_list()
            QMessageBox.information(self, "导入成功", f"成功从 Excel 合并了 {imported_count} 个分组逻辑。")
        except Exception as e:
            global_logger.error(f"Failed to import Excel: {e}")
            QMessageBox.critical(self, "导入失败", f"Excel 读取或合并出错: {e}")
                 
                 
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
        curr = self.tree_groups.currentItem()
        if not curr: return
        group_id = curr.data(0, Qt.UserRole)
        
        group_obj = self.dm.pm.get_group_by_id(group_id)
        if not group_obj: return
        
        # 成员穿梭框仅针对「当前特定层级」的分组，不包含子孙
        member_ids = set(group_obj.get("nodes", []))
        all_nodes = self.dm.get_node_list()
        
        in_nodes = [n for n in all_nodes if n.get('node_id') in member_ids]
        
        # 使用公共筛选函数过滤可添加的节点
        tf = self.cb_type.currentText()
        kw = self.le_search.text().strip()
        
        non_member_nodes = [n for n in all_nodes if n.get('node_id') not in member_ids]
        out_nodes = filter_nodes(non_member_nodes, keyword=kw, type_filter=tf)
        
        self.model_in.beginResetModel()
        self.model_in._data_cache = in_nodes
        self.model_in.endResetModel()
        
        self.model_out.beginResetModel()
        self.model_out._data_cache = out_nodes
        self.model_out.endResetModel()

    def on_move_in(self, index):
        curr = self.tree_groups.currentItem()
        if not curr or not index.isValid(): return
        group_id = curr.data(0, Qt.UserRole)
        
        try:
            node = self.model_out._data_cache[index.row()]
        except IndexError:
            return
        nid = node.get('node_id')
        
        group_obj = self.dm.pm.get_group_by_id(group_id)
        if group_obj:
            members = group_obj.get("nodes", [])
            if nid not in members:
                members.append(nid)
                self.dm.pm.update_group_members(group_id, members)
                self.refresh_members()

    def on_move_out(self, index):
        curr = self.tree_groups.currentItem()
        if not curr or not index.isValid(): return
        group_id = curr.data(0, Qt.UserRole)
        
        try:
            node = self.model_in._data_cache[index.row()]
        except IndexError:
            return
        nid = node.get('node_id')
        
        group_obj = self.dm.pm.get_group_by_id(group_id)
        if group_obj:
            members = group_obj.get("nodes", [])
            if nid in members:
                members.remove(nid)
                self.dm.pm.update_group_members(group_id, members)
                self.refresh_members()

    def _get_checked_group_ids(self):
        ids = []
        def collect_checked(item):
            if item.checkState(0) == Qt.Checked:
                ids.append(item.data(0, Qt.UserRole))
            for i in range(item.childCount()):
                collect_checked(item.child(i))
        
        for i in range(self.tree_groups.topLevelItemCount()):
            collect_checked(self.tree_groups.topLevelItem(i))
        return ids

    def on_batch_control(self, action_val: bool):
        checked_group_ids = self._get_checked_group_ids()
                
        if not checked_group_ids:
            # 向下兼容：如果没勾选，但选中了一行，也放行
            curr = self.tree_groups.currentItem()
            if curr:
                checked_group_ids.append(curr.data(0, Qt.UserRole))
            else:
                QMessageBox.warning(self, "未勾选组", "请先在左侧通过复选框勾选要控的分组。")
                return
            
        if not self.engine.connected:
            QMessageBox.warning(self, "连接中断", "OPC 服务掉线，请前往设置重连。")
            return
            
        combined_member_ids = set()
        display_names = []
        for group_id in checked_group_ids:
            # 递归获取成员
            members = self.dm.pm.get_group_nodes_recursive(group_id)
            combined_member_ids.update(members)
            
            g_obj = self.dm.pm.get_group_by_id(group_id)
            if g_obj: display_names.append(g_obj["name"])
        
        if not combined_member_ids:
            QMessageBox.information(self, "空组", "当前指定的分组（及其子组）内没有任何控制点。")
            return
            
        # 并发发送写入指令前作好全量日志记录
        action_name = '全开' if action_val else '全关'
        global_logger.info(f"==> 用户点击了批量 {action_name} 操作 | 受影响的分组: {', '.join(display_names)} | 受影响总节点数: {len(combined_member_ids)}")
        
        # 方案B：使用 WriteList 批量写入，大幅减少 RTT
        async def _batch_write():
            member_list = list(combined_member_ids)
            batch_size = 100  # 增大批次，减少网络往返
            for i in range(0, len(member_list), batch_size):
                batch = member_list[i:i+batch_size]
                display_names = [self.dm.get_alias_by_node_id(nid) for nid in batch]
                success_count, fail_count = await self.engine.write_values_batch(batch, action_val, display_names)
                if fail_count > 0:
                    global_logger.error(f"批量写入部分失败: {success_count} 成功, {fail_count} 失败")

        asyncio.create_task(_batch_write())

    def refresh_schedules(self):
        self.table_sched.setRowCount(0)
        schedules = self.dm.pm.get_schedules()
        for s in schedules:
            row = self.table_sched.rowCount()
            self.table_sched.insertRow(row)
            
            self.table_sched.setItem(row, 0, QTableWidgetItem(s.get("id", "")))
            
            # 查找分组名称
            g_id = s.get("group_id")
            g_obj = self.dm.pm.get_group_by_id(g_id)
            g_name = g_obj.get("name") if g_obj else f"未知分组({g_id})"
            
            self.table_sched.setItem(row, 1, QTableWidgetItem(g_name))
            self.table_sched.setItem(row, 2, QTableWidgetItem(s.get("time", "")))
            
            # 显示日期
            week_data = s.get("weekdays")
            if week_data is None:
                week_text = "每天"
            elif not week_data:
                week_text = "永不"
            elif len(week_data) == 7:
                week_text = "每天"
            else:
                days_map = {0:"一", 1:"二", 2:"三", 3:"四", 4:"五", 5:"六", 6:"日"}
                week_text = ",".join([days_map[d] for d in sorted(week_data)])
            
            self.table_sched.setItem(row, 3, QTableWidgetItem(week_text))
            
            action_text = "开启" if s.get("action") else "关闭"
            self.table_sched.setItem(row, 4, QTableWidgetItem(action_text))
            
            is_enabled = s.get("enabled", True)
            btn_status = QPushButton("🟢 已开启" if is_enabled else "🔴 已关闭")
            if is_enabled:
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #52c41a; border-radius: 3px; padding: 2px; } QPushButton:hover { background-color: #73d13d; }")
            else:
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #8c8c8c; border-radius: 3px; padding: 2px; } QPushButton:hover { background-color: #a6a6a6; }")
                
            # 闭包绑定当前id和前状态
            btn_status.clicked.connect(lambda chk, _id=s.get("id"), curr=is_enabled: self.on_toggle_schedule(_id, curr))
            
            self.table_sched.setCellWidget(row, 5, btn_status)
            
    def on_toggle_schedule(self, sched_id, current_status):
        new_status = not current_status
        self.dm.pm.update_schedule(sched_id, {"enabled": new_status})
        global_logger.info(f"调度计划 {sched_id} 用户操作变更为 -> [{'可用' if new_status else '停用'}]")
        self.refresh_schedules()

    def on_add_schedule(self):
        checked_group_ids = self._get_checked_group_ids()
        
        if not checked_group_ids:
            curr = self.tree_groups.currentItem()
            if curr:
                checked_group_ids.append(curr.data(0, Qt.UserRole))
            else:
                QMessageBox.warning(self, "提示", "请在左侧至少勾选一个分组，时间计划必须挂靠在分组上。")
                return
                
        time_str = self.time_edit.time().toString("HH:mm")
        is_on = self.cb_action.currentText() == "开启"
        
        # 获取勾选的日期
        weekdays = []
        for i, cb in enumerate(self.week_checkboxes):
            if cb.isChecked():
                weekdays.append(i)
        
        sched_list = []
        for g_id in checked_group_ids:
            sched_dict = {
                "id": str(uuid.uuid4()),
                "group_id": g_id,
                "time": time_str,
                "weekdays": weekdays,
                "action": is_on,
                "enabled": True
            }
            sched_list.append(sched_dict)
        
        # 批量添加，一次性持久化，避免 N 次磁盘 IO
        self.dm.pm.batch_add_schedules(sched_list)
            
        self.refresh_schedules()

    def on_del_schedule(self):
        selected = self.table_sched.selectedItems()
        if not selected: return
        
        row = selected[0].row()
        sched_id = self.table_sched.item(row, 0).text()
        
        self.dm.pm.delete_schedule(sched_id)
        self.refresh_schedules()
