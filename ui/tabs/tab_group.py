try:
    from PyQt5.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                                 QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QTimeEdit, QTableWidget, QTableWidgetItem, QFileDialog, QCheckBox, QTreeWidget, QTreeWidgetItem, QSplitter, QDialog)
    from PyQt5.QtCore import Qt, QTime, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
    from PyQt5.QtGui import QColor, QFont
except ImportError:
    from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
                                 QGroupBox, QTableView, QHeaderView, QLabel, QComboBox, QInputDialog, QMessageBox, QAbstractItemView, QLineEdit, QTimeEdit, QTableWidget, QTableWidgetItem, QFileDialog, QCheckBox, QTreeWidget, QTreeWidgetItem, QSplitter, QDialog)
    from PySide6.QtCore import Qt, QTime, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
    from PySide6.QtGui import QColor, QFont
    
import uuid
import asyncio
import re
import pandas as pd
from utils.logger import global_logger
from utils.filter_helper import filter_nodes

class SearchProxyModel(QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self.keyword = ""
        self.type_filter = "全部类型"
    
    def set_filter(self, keyword, type_filter):
        self.keyword = keyword.lower()
        self.type_filter = type_filter
        self.invalidateFilter()
        
    def filterAcceptsRow(self, source_row, source_parent):
        model = self.sourceModel()
        if not model: return False
        
        # Keyword matches Name (col 0) or NodeID (col 2)
        match_kw = True
        if self.keyword:
            name_idx = model.index(source_row, 0, source_parent)
            node_idx = model.index(source_row, 2, source_parent)
            name_val = str(model.data(name_idx) or "").lower()
            node_val = str(model.data(node_idx) or "").lower()
            match_kw = (self.keyword in name_val) or (self.keyword in node_val)
            
        # Type matches (col 3)
        match_type = True
        if self.type_filter != "全部类型":
            type_idx = model.index(source_row, 3, source_parent)
            type_val = str(model.data(type_idx) or "")
            match_type = (self.type_filter in type_val)
            
        return match_kw and match_type

class GroupMemberTableModel(QAbstractTableModel):
    """组内/组外控制点表格模型，仅显示4列：别名、当前状态、Node ID、数据类型"""
    def __init__(self, data_manager):
        super().__init__()
        self.dm = data_manager
        self._data_cache = []
        self._headers = ["别名", "当前状态", "Node ID", "数据类型"]

    def set_data(self, nodes):
        self.beginResetModel()
        self._data_cache = nodes
        self.endResetModel()

    def update_nodes(self, dirty_ids):
        if not dirty_ids or not self._data_cache:
            return
            
        if not hasattr(self, '_row_map') or len(self._row_map) != len(self._data_cache):
            self._row_map = {n.get('node_id'): i for i, n in enumerate(self._data_cache)}
            
        for nid in dirty_ids:
            if nid in self._row_map:
                row = self._row_map[nid]
                # column 1 indicates Current Status
                self.dataChanged.emit(self.index(row, 1), self.index(row, 1))

    def rowCount(self, parent=QModelIndex()):
        return len(self._data_cache)

    def columnCount(self, parent=QModelIndex()):
        return len(self._headers)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or not (0 <= index.row() < len(self._data_cache)):
            return None

        node = self._data_cache[index.row()]
        col = index.column()

        if role == Qt.DisplayRole:
            if col == 0: return node.get('alias', node.get('name', ''))
            if col == 1:
                val = node.get('value')
                if isinstance(val, bool): return "开启" if val else "关闭"
                return str(val) if val is not None else "等待更新"
            if col == 2: return node.get('node_id', '')
            if col == 3: return node.get('type', '')
        elif role == Qt.ForegroundRole:
            if col == 1:
                val = node.get('value')
                if isinstance(val, bool):
                    return QColor("red") if not val else QColor("green")
        elif role == Qt.FontRole:
            if col == 1:
                font = QFont()
                font.setBold(True)
                return font
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignCenter
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._headers[section]
        return None


class TabGroup(QWidget):
    def __init__(self, data_manager, opc_engine):
        super().__init__()
        self.dm = data_manager
        self.engine = opc_engine
        self._setup_ui()

    def _setup_ui(self):
        # 使用 QSplitter 实现左右可调节布局
        main_splitter = QSplitter(Qt.Horizontal)

        # ===============================
        # 左侧：分组列表管理
        # ===============================
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        grp_group = QGroupBox("分组管理")
        grp_group_layout = QVBoxLayout(grp_group)

        btn_layout = QHBoxLayout()
        self.btn_add_group = QPushButton("新建一级分组")
        self.btn_add_sub = QPushButton("新建子分组")
        self.btn_rename_group = QPushButton("重命名")
        self.btn_del_group = QPushButton("删除分组")
        self.btn_del_group.setStyleSheet("QPushButton {background-color: #fee2e2; color: #ef4444; border: 1px solid #fca5a5; border-radius: 6px;} QPushButton:hover {background-color: #fecaca;}")

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
            "QPushButton {background-color: #10b981; color: white; height: 40px; font-weight: bold; border-radius: 6px; border: none;}"
            "QPushButton:hover {background-color: #34d399;}"
            "QPushButton:pressed {background-color: #059669;}"
        )
        self.btn_batch_off = QPushButton("💡 批量全关")
        self.btn_batch_off.setStyleSheet(
            "QPushButton {background-color: #ef4444; color: white; height: 40px; font-weight: bold; border-radius: 6px; border: none;}"
            "QPushButton:hover {background-color: #f87171;}"
            "QPushButton:pressed {background-color: #dc2626;}"
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
        # 右侧：组成员管理 & 定时调度（使用 QSplitter 上下可调节）
        # ===============================
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)

        # 上下分割器：成员管理 / 定时调度
        vert_splitter = QSplitter(Qt.Vertical)

        # 上部：成员穿梭框
        grp_members = QGroupBox("组成员管理 (选中分组后配置)")
        members_layout = QVBoxLayout(grp_members)

        # 左穿梭（选中的组成员）- 带筛选
        box_in = QVBoxLayout()
        header_in = QHBoxLayout()
        header_in.addWidget(QLabel("组内控制点 (点击移出):"))
        header_in.addStretch()
        header_in.addWidget(QLabel("类型:"))
        self.cb_type_in = QComboBox()
        self.cb_type_in.addItems(["全部类型", "Boolean", "UInt", "Int", "Real", "String"])
        self.cb_type_in.currentTextChanged.connect(lambda _: self._update_in_filter())
        header_in.addWidget(self.cb_type_in)
        header_in.addWidget(QLabel(" 搜索:"))
        self.le_search_in = QLineEdit()
        self.le_search_in.setPlaceholderText("搜ID或别名...")
        self.le_search_in.textChanged.connect(lambda _: self._update_in_filter())
        header_in.addWidget(self.le_search_in)
        box_in.addLayout(header_in)

        self.tb_in = QTableView()
        self.tb_in.setSelectionBehavior(QAbstractItemView.SelectRows)
        box_in.addWidget(self.tb_in)

        # 右穿梭（可供添加的成员）- 带筛选
        box_out = QVBoxLayout()
        header_out = QHBoxLayout()
        header_out.addWidget(QLabel("可添加控制点 (点击加入):"))
        header_out.addStretch()
        header_out.addWidget(QLabel("类型:"))
        self.cb_type_out = QComboBox()
        self.cb_type_out.addItems(["全部类型", "Boolean", "UInt", "Int", "Real", "String"])
        self.cb_type_out.currentTextChanged.connect(lambda _: self._update_out_filter())
        header_out.addWidget(self.cb_type_out)

        header_out.addWidget(QLabel(" 搜索:"))
        self.le_search_out = QLineEdit()
        self.le_search_out.setPlaceholderText("搜ID或别名...")
        self.le_search_out.textChanged.connect(lambda _: self._update_out_filter())
        header_out.addWidget(self.le_search_out)

        box_out.addLayout(header_out)

        self.tb_out = QTableView()
        self.tb_out.setSelectionBehavior(QAbstractItemView.SelectRows)
        box_out.addWidget(self.tb_out)

        # 左右分割器
        horiz_splitter = QSplitter(Qt.Horizontal)
        horiz_splitter.addWidget(self._make_widget_from_layout(box_in))
        horiz_splitter.addWidget(self._make_widget_from_layout(box_out))
        horiz_splitter.setStretchFactor(0, 1)
        horiz_splitter.setStretchFactor(1, 1)

        members_layout.addWidget(horiz_splitter)

        # 下部：定时调度
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
        self.btn_del_sched.setStyleSheet("QPushButton {color: #ef4444; background-color: transparent;} QPushButton:hover {background-color: #fee2e2;}")
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
            cb.setChecked(True)  # 默认全选
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
        self.table_sched.hideColumn(0)  # 隐藏内部ID
        sched_layout.addWidget(self.table_sched)

        vert_splitter.addWidget(grp_members)
        vert_splitter.addWidget(grp_schedule)
        vert_splitter.setStretchFactor(0, 6)
        vert_splitter.setStretchFactor(1, 4)

        right_layout.addWidget(vert_splitter)

        # 组装左右分割器
        main_splitter.addWidget(left_widget)
        main_splitter.addWidget(right_widget)
        main_splitter.setStretchFactor(0, 3)
        main_splitter.setStretchFactor(1, 7)

        # 设置主窗口布局
        container = QWidget()
        container_layout = QHBoxLayout(container)
        container_layout.setContentsMargins(0, 0, 0, 0)
        container_layout.addWidget(main_splitter)
        self.setLayout(container_layout)

        # 使用新的 GroupMemberTableModel
        self.model_in = GroupMemberTableModel(self.dm)
        self.model_out = GroupMemberTableModel(self.dm)
        
        self.proxy_in = SearchProxyModel()
        self.proxy_in.setSourceModel(self.model_in)
        self.proxy_out = SearchProxyModel()
        self.proxy_out.setSourceModel(self.model_out)
        
        self.tb_in.setModel(self.proxy_in)
        self.tb_out.setModel(self.proxy_out)

        self.tb_in.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.tb_in.horizontalHeader().setStretchLastSection(True)
        self.tb_out.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.tb_out.horizontalHeader().setStretchLastSection(True)

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
        self.table_sched.cellDoubleClicked.connect(self.on_schedule_cell_double_clicked)

        self.refresh_groups_list()
        self.refresh_schedules()

    def _make_widget_from_layout(self, layout):
        """将布局转换为 widget"""
        w = QWidget()
        w.setLayout(layout)
        return w

    def _update_in_filter(self):
        self.proxy_in.set_filter(self.le_search_in.text(), self.cb_type_in.currentText())

    def _update_out_filter(self):
        self.proxy_out.set_filter(self.le_search_out.text(), self.cb_type_out.currentText())

    def _get_schedule_group_name(self, sched):
        """从调度对象获取分组名称"""
        if not sched:
            return ""
        g_obj = self.dm.pm.get_group_by_id(sched.get("group_id"))
        return g_obj.get("name") if g_obj else sched.get("group_id", "")

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
            global_logger.info(f"{'新建子分组' if is_sub else '新建一级分组'}: '{text.strip()}'")
            self.refresh_groups_list()
                
    def on_del_group(self):
        curr = self.tree_groups.currentItem()
        if not curr: return
        group_id = curr.data(0, Qt.UserRole)
        name = curr.text(0)

        # 获取子分组数量
        child_count = curr.childCount()
        child_info = f"（包含 {child_count} 个子分组）" if child_count > 0 else ""

        rep = QMessageBox.question(
            self, "确认删除分组",
            f"确定要删除分组 【{name}】{child_info} 吗？\n\n"
            f"此操作将同时：\n"
            f"  • 删除该分组及其所有子分组\n"
            f"  • 删除该分组关联的所有调度计划\n"
            f"  • 控制节点本身不受影响\n\n"
            f"此操作不可撤销！",
            QMessageBox.Yes | QMessageBox.No
        )
        if rep == QMessageBox.Yes:
            self.dm.pm.delete_group(group_id)
            global_logger.info(f"删除分组: '{name}'")
            self.refresh_groups_list()
            self.refresh_schedules()

    def on_rename_group(self):
        curr = self.tree_groups.currentItem()
        if not curr: return
        group_id = curr.data(0, Qt.UserRole)
        old_name = curr.text(0)

        text, ok = QInputDialog.getText(self, "重命名", "请输入新分组名称:", text=old_name)
        if ok and text.strip() and text.strip() != old_name:
            if self.dm.pm.rename_group(group_id, text.strip()):
                global_logger.info(f"分组 '{old_name}' 已重命名为 '{text.strip()}'")
                self.refresh_groups_list()
                self.refresh_schedules()

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
            # 构建 id -> name 映射用于查找父分组名称
            id_to_name = {g["id"]: g["name"] for g in groups}
            
            export_rows = []
            for g in groups:
                g_name = g["name"]
                parent_name = id_to_name.get(g.get("parent_id"), "") if g.get("parent_id") else ""
                
                nodes = g.get("nodes", [])
                if nodes:
                    for full_id in nodes:
                        # [V1.2.0 修订] 仅导出标识符的具体值内容，不含 ns=2;s= 前缀
                        match = re.search(r';[isgb]=(.+)', full_id)
                        short_id = match.group(1) if match else full_id
                        export_rows.append({
                            "分组名称": g_name,
                            "父分组名称": parent_name,
                            "节点标识": short_id
                        })
                else:
                    # 导出空分组（仅保留层级关系）
                    export_rows.append({
                        "分组名称": g_name,
                        "父分组名称": parent_name,
                        "节点标识": ""
                    })
            
            if not export_rows:
                df = pd.DataFrame(columns=["分组名称", "父分组名称", "节点标识"])
            else:
                df = pd.DataFrame(export_rows)
                
            df.to_excel(file_path, index=False)
            QMessageBox.information(self, "导出成功", f"成功导出 {len(groups)} 个分组配置到 Excel（包含层级关系）。")
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

            has_parent_col = "父分组名称" in df.columns

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

            # 第一步：收集分组名称 -> (父分组名称, [节点列表])
            imported_data = {}  # group_name -> {"parent": parent_name, "nodes": [full_ids]}
            for _, row in df.iterrows():
                g_name = str(row["分组名称"]).strip()
                raw_id = str(row["节点标识"]).strip()
                parent_name = str(row["父分组名称"]).strip() if has_parent_col else ""
                # 清理 nan
                if parent_name.lower() == 'nan':
                    parent_name = ""
                if not g_name or g_name.lower() == 'nan':
                    continue

                if g_name not in imported_data:
                    imported_data[g_name] = {"parent": parent_name, "nodes": []}

                if raw_id and raw_id.lower() != 'nan':
                    clean_id = raw_id[:-2] if raw_id.endswith('.0') else raw_id
                    full_id = id_map.get(clean_id) or id_map.get(raw_id)
                    if full_id:
                        imported_data[g_name]["nodes"].append(full_id)

            current_groups = self.dm.pm.get_groups()
            # 构建名称 -> id 的映射（用于查找已存在分组和父分组）
            name_to_id = {g["name"]: g["id"] for g in current_groups}
            imported_count = 0

            # 第二步：创建或合并分组，保留层级关系
            for g_name, info in imported_data.items():
                parent_name = info["parent"]
                member_list = info["nodes"]

                # 查找父分组 ID
                parent_id = name_to_id.get(parent_name) if parent_name else None

                target_g = None
                for g in current_groups:
                    if g["name"] == g_name:
                        target_g = g
                        break

                if target_g:
                    # 并集去重合并
                    merged_members = list(set(target_g.get("nodes", []) + member_list))
                    target_g["nodes"] = merged_members
                    # 如果原来无父分组但导入有父分组，更新层级
                    if parent_id and not target_g.get("parent_id"):
                        target_g["parent_id"] = parent_id
                else:
                    new_id = str(uuid.uuid4())
                    new_group = {
                        "id": new_id,
                        "name": g_name,
                        "parent_id": parent_id,
                        "nodes": member_list
                    }
                    self.dm.pm.data_store["groups"].append(new_group)
                    name_to_id[g_name] = new_id  # 更新映射以供后续组引用
                imported_count += 1

            # 批量操作完成后一次性持久化
            self.dm.pm.save()
            self.refresh_groups_list()
            QMessageBox.information(self, "导入成功", f"成功从 Excel 合并了 {imported_count} 个分组逻辑。")
        except Exception as e:
            global_logger.error(f"Failed to import Excel: {e}")
            QMessageBox.critical(self, "导入失败", f"Excel 读取或合并出错: {e}")
                 
                 
    def on_group_selection_changed(self, current, previous):
        if not current:
            self.model_in.set_data([])
            self.model_out.set_data([])
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

        self.model_in.set_data(in_nodes)
        
        non_member_nodes = [n for n in all_nodes if n.get('node_id') not in member_ids]
        self.model_out.set_data(non_member_nodes)
        
        # 顺便刷新一遍过滤以处理数据源变更
        self._update_in_filter()
        self._update_out_filter()

    def on_move_in(self, index):
        curr = self.tree_groups.currentItem()
        if not curr or not index.isValid(): return
        group_id = curr.data(0, Qt.UserRole)
        
        src_index = self.proxy_out.mapToSource(index)
        
        try:
            node = self.model_out._data_cache[src_index.row()]
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
        
        src_index = self.proxy_in.mapToSource(index)
        
        try:
            node = self.model_in._data_cache[src_index.row()]
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

        # 确认对话框
        action_name = '全开' if action_val else '全关'
        confirmed = QMessageBox.question(
            self, "确认批量操作",
            f"确定要对以下分组执行【{action_name}】操作吗？\n\n"
            f"分组: {', '.join(display_names)}\n"
            f"影响节点数: {len(combined_member_ids)}\n\n"
            f"此操作将{'开启' if action_val else '关闭'}所有相关联的点位，请确认。",
            QMessageBox.Yes | QMessageBox.No
        )
        if confirmed != QMessageBox.Yes:
            return

        # 并发发送写入指令前作好全量日志记录
        global_logger.info(f"==> 用户确认了批量 {action_name} 操作 | 受影响的分组: {', '.join(display_names)} | 受影响总节点数: {len(combined_member_ids)}")

        # 方案B：使用 WriteList 批量写入，大幅减少 RTT
        async def _batch_write():
            member_list = list(combined_member_ids)
            batch_size = 100  # 增大批次，减少网络往返
            for i in range(0, len(member_list), batch_size):
                batch = member_list[i:i+batch_size]
                node_aliases = [self.dm.get_alias_by_node_id(nid) for nid in batch]
                success_count, fail_count = await self.engine.write_values_batch(batch, action_val, node_aliases)
                if fail_count > 0:
                    global_logger.error(f"批量写入部分失败: {success_count} 成功, {fail_count} 失败")

        asyncio.create_task(_batch_write())

    def refresh_schedules(self):
        # 保存滚动位置
        scroll_pos = self.table_sched.verticalScrollBar().value()

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
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #10b981; border-radius: 6px; border: none; padding: 4px; } QPushButton:hover { background-color: #34d399; }")
            else:
                btn_status.setStyleSheet("QPushButton { color: white; background-color: #94a3b8; border-radius: 6px; border: none; padding: 4px; } QPushButton:hover { background-color: #cbd5e1; }")

            # 闭包绑定当前id和前状态
            btn_status.clicked.connect(lambda chk, _id=s.get("id"), curr=is_enabled: self.on_toggle_schedule(_id, curr))

            self.table_sched.setCellWidget(row, 5, btn_status)

        # 恢复滚动位置
        self.table_sched.verticalScrollBar().setValue(scroll_pos)
            
    def on_toggle_schedule(self, sched_id, current_status):
        new_status = not current_status
        self.dm.pm.update_schedule(sched_id, {"enabled": new_status})
        sched = self.dm.pm.get_schedule_by_id(sched_id)
        group_name = self._get_schedule_group_name(sched)
        global_logger.info(f"调度计划 '{group_name}' (ID: {sched_id}) 变更为 -> [{'可用' if new_status else '停用'}]")
        self.refresh_schedules()

    def on_schedule_cell_double_clicked(self, row, col):
        """双击调度表单元格编辑时间和动作"""
        # 只允许编辑列2（触发时间）和列4（动作指令）
        if col not in (2, 4):
            return

        sched_id = self.table_sched.item(row, 0).text()
        if not sched_id:
            return

        if col == 2:
            # 编辑时间：弹出时间选择对话框
            current_time_str = self.table_sched.item(row, 2).text()
            time_obj = QTime.fromString(current_time_str, "HH:mm")
            if not time_obj.isValid():
                time_obj = QTime.currentTime()

            time_edit = QTimeEdit(time_obj)
            time_edit.setDisplayFormat("HH:mm")
            time_edit.setMinimumWidth(100)

            dialog = QDialog(self)
            dialog.setWindowTitle("修改触发时间")
            layout = QVBoxLayout(dialog)
            layout.addWidget(time_edit)
            btn_box = QHBoxLayout()
            btn_ok = QPushButton("确定")
            btn_cancel = QPushButton("取消")
            btn_ok.clicked.connect(dialog.accept)
            btn_cancel.clicked.connect(dialog.reject)
            btn_box.addStretch()
            btn_box.addWidget(btn_ok)
            btn_box.addWidget(btn_cancel)
            layout.addLayout(btn_box)

            if dialog.exec_() == QDialog.Accepted:
                new_time_str = time_edit.time().toString("HH:mm")
                self.dm.pm.update_schedule(sched_id, {"time": new_time_str})
                sched = self.dm.pm.get_schedule_by_id(sched_id)
                group_name = self._get_schedule_group_name(sched)
                global_logger.info(f"调度计划 '{group_name}' (ID: {sched_id}) 时间修改为 {new_time_str}")
                self.refresh_schedules()

        elif col == 4:
            # 编辑动作：切换开启/关闭
            current_action_text = self.table_sched.item(row, 4).text()
            current_action = (current_action_text == "开启")

            # 弹出选择对话框
            items = ["开启", "关闭"]
            current_idx = 0 if current_action else 1
            item, ok = QInputDialog.getItem(self, "修改动作指令", "选择动作:", items, current_idx, False)
            if ok and item:
                new_action = (item == "开启")
                self.dm.pm.update_schedule(sched_id, {"action": new_action})
                sched = self.dm.pm.get_schedule_by_id(sched_id)
                group_name = self._get_schedule_group_name(sched)
                global_logger.info(f"调度计划 '{group_name}' (ID: {sched_id}) 动作修改为 {'开启' if new_action else '关闭'}")
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
        group_names = []
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
            g_obj = self.dm.pm.get_group_by_id(g_id)
            group_names.append(g_obj.get("name") if g_obj else g_id)

        # 批量添加，一次性持久化，避免 N 次磁盘 IO
        self.dm.pm.batch_add_schedules(sched_list)
        global_logger.info(f"新增调度计划: 分组 {group_names}, 时间 {time_str}, 动作 {'开启' if is_on else '关闭'}, 重复 {[i+1 for i in weekdays] if weekdays else '每天'}")
        self.refresh_schedules()

    def on_del_schedule(self):
        selected = self.table_sched.selectedItems()
        if not selected: return

        row = selected[0].row()
        sched_id = self.table_sched.item(row, 0).text()
        sched = self.dm.pm.get_schedule_by_id(sched_id)
        group_name = self._get_schedule_group_name(sched)

        self.dm.pm.delete_schedule(sched_id)
        global_logger.info(f"删除调度计划: 分组 '{group_name}' (ID: {sched_id})")
        self.refresh_schedules()
