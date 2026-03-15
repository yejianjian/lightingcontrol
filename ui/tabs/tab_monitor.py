try:
    from PyQt5.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableView, QLineEdit, QHeaderView, QLabel, QComboBox, QMessageBox, QFileDialog
    from PyQt5.QtCore import Qt, QAbstractTableModel, QModelIndex
    from PyQt5.QtGui import QColor, QFont
except ImportError:
    from PySide6.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableView, QLineEdit, QHeaderView, QLabel, QComboBox, QMessageBox, QFileDialog
    from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex
    from PySide6.QtGui import QColor, QFont

from utils.excel_parser import import_aliases_from_excel
from utils.filter_helper import filter_nodes

class MonitorTableModel(QAbstractTableModel):
    def __init__(self, data_manager):
        super().__init__()
        self.dm = data_manager
        self._data_cache = []
        self._headers = ["Node ID", "备注名(别名)", "数据类型", "当前状态/数", "更新时间", "控制操作"]
        self._last_keyword = ""
        self._last_type_filter = "全部数据类型"

    def refresh_data(self, keyword="", type_filter="全部数据类型"):
        self.beginResetModel()
        all_nodes = self.dm.get_node_list()
        
        self._data_cache = filter_nodes(all_nodes, keyword=keyword, type_filter=type_filter)
        self._row_map = {n.get('node_id'): i for i, n in enumerate(self._data_cache)}
        self._last_keyword = keyword
        self._last_type_filter = type_filter
        self.endResetModel()

    def update_nodes(self, dirty_ids):
        if not dirty_ids or not self._data_cache:
            return
        
        # 兜底确保映射存在
        if not hasattr(self, '_row_map') or len(self._row_map) != len(self._data_cache):
            self._row_map = {n.get('node_id'): i for i, n in enumerate(self._data_cache)}
            
        for nid in dirty_ids:
            if nid in self._row_map:
                row = self._row_map[nid]
                self.dataChanged.emit(self.index(row, 3), self.index(row, 4))

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
            if col == 0: return node.get('node_id', '')
            if col == 1: return node.get('alias', node.get('name', ''))
            if col == 2: return node.get('type', '')
            if col == 3: 
                val = node.get('value')
                if isinstance(val, bool): return "开启" if val else "关闭"
                return str(val) if val is not None else "等待更新"
            if col == 4: return node.get('timestamp', '')
            
        elif role == Qt.EditRole and col == 1:
            return node.get('alias', node.get('name', ''))
            
        elif role == Qt.ForegroundRole:
            if col == 3:
                val = node.get('value')
                if isinstance(val, bool):
                     return QColor("red") if not val else QColor("green")
            
        elif role == Qt.FontRole:
            if col == 3:
                font = QFont()
                font.setBold(True)
                return font
                
        elif role == Qt.TextAlignmentRole:
            return Qt.AlignCenter
            
        return None

    def flags(self, index):
        default_flags = super().flags(index)
        if index.column() == 1:
            return default_flags | Qt.ItemIsEditable
        return default_flags

    def setData(self, index, value, role=Qt.EditRole):
        if index.isValid() and role == Qt.EditRole and index.column() == 1:
            node = self._data_cache[index.row()]
            new_alias = str(value).strip()
            self.dm.set_alias(node.get('node_id'), new_alias)
            # 直接更新缓存中的别名，不做全量重刷以保留筛选状态
            node['alias'] = new_alias
            self.dataChanged.emit(index, index)
            return True
        return False

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._headers[section]
        return None

class TabMonitor(QWidget):
    def __init__(self, data_manager, engine):
        super().__init__()
        self.dm = data_manager
        self.engine = engine
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # 顶部工具栏
        tool_layout = QHBoxLayout()
        btn_import = QPushButton("导入 Excel 映射别名")
        btn_import.clicked.connect(self.on_import_clicked)
        btn_export = QPushButton("导出所有点位")
        btn_export.clicked.connect(self.on_export_clicked)
        btn_refresh = QPushButton("刷新列表")
        btn_refresh.clicked.connect(self.on_refresh_clicked)
        
        lbl_type = QLabel("点位类型:")
        self.cb_type = QComboBox()
        self.cb_type.addItems(["全部数据类型", "Boolean", "UInt", "Int", "Real", "String"])
        self.cb_type.currentTextChanged.connect(self.on_filter_changed)
        
        lbl_search = QLabel("搜索:")
        self.le_search = QLineEdit()
        self.le_search.setPlaceholderText("输入 Node ID 或别名进行筛选...")
        self.le_search.textChanged.connect(self.on_filter_changed)
        
        tool_layout.addWidget(btn_import)
        tool_layout.addWidget(btn_export)
        tool_layout.addWidget(btn_refresh)
        tool_layout.addStretch()
        tool_layout.addWidget(lbl_type)
        tool_layout.addWidget(self.cb_type)
        tool_layout.addWidget(lbl_search)
        tool_layout.addWidget(self.le_search)
        
        layout.addLayout(tool_layout)

        # 数据表格
        self.table_view = QTableView()
        self.model = MonitorTableModel(self.dm)
        self.table_view.setModel(self.model)
        header = self.table_view.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.Interactive)
        header.setStretchLastSection(True)
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setMouseTracking(True)  # 开启以支持悬停事件

        # 挂载控制流代理
        from ui.components.action_delegate import ActionButtonDelegate
        self.delegate = ActionButtonDelegate(self.table_view, self.dm, self.engine)
        self.table_view.setItemDelegateForColumn(5, self.delegate)
        
        layout.addWidget(self.table_view)

    def on_refresh_clicked(self):
        self.on_filter_changed()

    def on_filter_changed(self, *args):
        kw = self.le_search.text().strip()
        tf = self.cb_type.currentText()
        self.model.refresh_data(keyword=kw, type_filter=tf)
        
    def on_import_clicked(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择点表配置 Excel 文件", "", "Excel Files (*.xlsx *.xls)"
        )
        if file_path:
            count, msg = import_aliases_from_excel(file_path, self.dm)
            if count > 0:
                QMessageBox.information(self, "导入成功", msg)
                self.on_refresh_clicked()  # 刷一下界面使得别名显示出来
            else:
                QMessageBox.warning(self, "导入失败或为空", msg)

    def on_export_clicked(self):
        all_nodes = self.dm.get_node_list()
        if not all_nodes:
            QMessageBox.information(self, "无数据", "当前没有任何点位数据，请先连接服务器。")
            return

        file_path, _ = QFileDialog.getSaveFileName(
            self, "导出点位列表", "点位列表.xlsx", "Excel Files (*.xlsx)"
        )
        if not file_path:
            return

        try:
            import pandas as pd
            import re
            rows = []
            for n in all_nodes:
                full_id = n.get('node_id', '')
                # [V1.2.0 修订] 仅导出标识符的具体值内容，不含 ns=2;s= 前缀
                match = re.search(r';[isgb]=(.+)', full_id)
                short_id = match.group(1) if match else full_id
                
                val = n.get('value')
                if isinstance(val, bool):
                    val_str = "开启" if val else "关闭"
                else:
                    val_str = str(val) if val is not None else ""

                rows.append({
                    "Node ID": short_id,
                    "备注名/别名": n.get('alias', n.get('name', '')),
                    "数据类型": n.get('type', ''),
                    "当前状态/数据": val_str
                })
            df = pd.DataFrame(rows)
            df.to_excel(file_path, index=False)
            QMessageBox.information(self, "导出成功", f"已将 {len(rows)} 个点位导出到:\n{file_path}")
        except Exception as e:
            QMessageBox.critical(self, "导出失败", str(e))
