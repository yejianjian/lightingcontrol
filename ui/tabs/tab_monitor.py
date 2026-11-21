try:
    from PyQt5.QtWidgets import (
        QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableView,
        QLineEdit, QHeaderView, QLabel, QComboBox, QMessageBox,
        QFileDialog, QAbstractItemView, QApplication
    )
    from PyQt5.QtCore import Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
    from PyQt5.QtGui import QColor, QFont
except ImportError:
    from PySide6.QtWidgets import (
        QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QTableView,
        QLineEdit, QHeaderView, QLabel, QComboBox, QMessageBox,
        QFileDialog, QAbstractItemView, QApplication
    )
    from PySide6.QtCore import Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel
    from PySide6.QtGui import QColor, QFont

import asyncio
import re
import pandas as pd
from utils.logger import global_logger
from utils.excel_parser import import_aliases_from_excel
from utils.filter_helper import TYPE_MAP


class MonitorSearchProxyModel(QSortFilterProxyModel):
    """
    监控表格专用的搜索与类型过滤代理模型
    避免每次用户敲击键盘时重建整张数据表与重置滚动条
    """
    def __init__(self, parent=None):
        super().__init__(parent)
        self.keyword = ""
        self.type_filter = "全部数据类型"
        self.setDynamicSortFilter(True)

    def set_filter(self, keyword: str, type_filter: str):
        self.keyword = keyword.strip().lower()
        self.type_filter = type_filter
        self.invalidateFilter()

    def filterAcceptsRow(self, source_row: int, source_parent: QModelIndex) -> bool:
        model = self.sourceModel()
        if not model or not hasattr(model, '_data_cache'):
            return False

        if not (0 <= source_row < len(model._data_cache)):
            return False

        node = model._data_cache[source_row]

        # 1. 数据类型过滤
        if "全部" not in self.type_filter:
            tf_lower = self.type_filter.lower()
            allowed = TYPE_MAP.get(tf_lower, [tf_lower])
            is_int_filter = (tf_lower == 'int')
            n_type = str(node.get('type', '')).lower()
            matched = False
            for t in allowed:
                if t in n_type:
                    if is_int_filter and 'uint' in n_type:
                        continue
                    matched = True
                    break
            if not matched:
                return False

        # 2. 关键词模糊过滤 (匹配别名、原始名或 Node ID)
        if self.keyword:
            alias = str(node.get('alias', node.get('name', ''))).lower()
            node_id = str(node.get('node_id', '')).lower()
            if self.keyword not in alias and self.keyword not in node_id:
                return False

        return True


class MonitorTableModel(QAbstractTableModel):
    def __init__(self, data_manager):
        super().__init__()
        self.dm = data_manager
        self._data_cache = []
        self._headers = ["Node ID", "备注名(别名)", "数据类型", "当前状态/数", "更新时间", "控制操作"]
        self._row_map = {}

    def refresh_data(self):
        """全量同步数据总线快照（仅在数据架构变化或手动刷新时调用）"""
        self.beginResetModel()
        self._data_cache = self.dm.get_node_list()
        self._row_map = {n.get('node_id'): i for i, n in enumerate(self._data_cache)}
        self.endResetModel()

    def update_nodes(self, dirty_ids):
        """局部增量通知脏节点，避免整表重绘刷新"""
        if not dirty_ids or not self._data_cache:
            return
        
        # 兜底确保映射存在
        if len(self._row_map) != len(self._data_cache):
            self._row_map = {n.get('node_id'): i for i, n in enumerate(self._data_cache)}
            
        for nid in dirty_ids:
            if nid in self._row_map:
                row = self._row_map[nid]
                # 同步最新的节点字典属性到本地缓存
                with self.dm._dirty_lock:
                    latest = self.dm.nodes.get(nid)
                    if latest:
                        self._data_cache[row].update(latest)
                # 通知别名、状态和时间列更新
                self.dataChanged.emit(self.index(row, 1), self.index(row, 4))

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
            # 直接更新缓存中的别名
            node['alias'] = new_alias
            self.dataChanged.emit(index, index)
            return True
        return False

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole and orientation == Qt.Horizontal:
            return self._headers[section]
        return None


def _export_worker(file_path: str, all_nodes: list) -> int:
    """在后台线程执行的 Excel 导出任务"""
    rows = []
    for n in all_nodes:
        full_id = n.get('node_id', '')
        # 仅导出标识符的具体值内容，不含 ns=2;s= 前缀
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
    return len(rows)


class TabMonitor(QWidget):
    def __init__(self, data_manager, engine):
        super().__init__()
        self.dm = data_manager
        self.engine = engine
        self._background_tasks = set()  # 强引用保活池，防止执行中的导出导入协程被垃圾回收
        self._setup_ui()

    def _spawn_background_task(self, coro):
        """保活启动后台异步任务"""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        return task

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(2, 4, 2, 2)
        layout.setSpacing(4)

        # 顶部工具栏
        tool_layout = QHBoxLayout()
        self.btn_import = QPushButton("导入 Excel 映射别名")
        self.btn_import.clicked.connect(self.on_import_clicked)
        self.btn_export = QPushButton("导出所有点位")
        self.btn_export.clicked.connect(self.on_export_clicked)
        self.btn_refresh = QPushButton("刷新列表")
        self.btn_refresh.clicked.connect(self.on_refresh_clicked)
        
        lbl_type = QLabel("点位类型:")
        self.cb_type = QComboBox()
        self.cb_type.addItems(["全部数据类型", "Boolean", "UInt", "Int", "Real", "String"])
        self.cb_type.currentTextChanged.connect(self.on_filter_changed)
        
        lbl_search = QLabel("搜索:")
        self.le_search = QLineEdit()
        self.le_search.setPlaceholderText("输入 Node ID 或别名进行筛选...")
        self.le_search.setMaximumWidth(260)  # 最大化时避免搜索框无限拉长
        self.le_search.textChanged.connect(self.on_filter_changed)
        
        tool_layout.addWidget(self.btn_import)
        tool_layout.addWidget(self.btn_export)
        tool_layout.addWidget(self.btn_refresh)
        tool_layout.addStretch()
        tool_layout.addWidget(lbl_type)
        tool_layout.addWidget(self.cb_type)
        tool_layout.addWidget(lbl_search)
        tool_layout.addWidget(self.le_search)
        
        layout.addLayout(tool_layout)

        # 数据表格与代理模型装配
        self.table_view = QTableView()
        self.model = MonitorTableModel(self.dm)
        self.proxy_model = MonitorSearchProxyModel(self)
        self.proxy_model.setSourceModel(self.model)
        self.table_view.setModel(self.proxy_model)

        header = self.table_view.horizontalHeader()
        header.setStretchLastSection(False)
        # 列宽策略：Node ID 交互可调、别名列吸收弹性空间、控制列固定加宽保证按钮可点
        header.setSectionResizeMode(0, QHeaderView.Interactive)
        header.resizeSection(0, 220)
        header.setSectionResizeMode(1, QHeaderView.Stretch)  # 备注名(别名)
        header.setSectionResizeMode(2, QHeaderView.Interactive)
        header.resizeSection(2, 90)   # 数据类型
        header.setSectionResizeMode(3, QHeaderView.Interactive)
        header.resizeSection(3, 100)  # 当前状态
        header.setSectionResizeMode(4, QHeaderView.Interactive)
        header.resizeSection(4, 150)  # 更新时间
        header.setSectionResizeMode(5, QHeaderView.Fixed)
        header.resizeSection(5, 190)  # 控制操作：开启/关闭双按钮
        self.table_view.setHorizontalScrollMode(QAbstractItemView.ScrollPerPixel)
        self.table_view.verticalHeader().setDefaultSectionSize(32)  # 加大行高便于点击
        self.table_view.setSelectionBehavior(QTableView.SelectRows)
        self.table_view.setAlternatingRowColors(True)
        self.table_view.setMouseTracking(True)  # 开启以支持悬停事件

        # 挂载控制流代理
        from ui.components.action_delegate import ActionButtonDelegate
        self.delegate = ActionButtonDelegate(self.table_view, self.dm, self.engine)
        self.table_view.setItemDelegateForColumn(5, self.delegate)
        
        layout.addWidget(self.table_view)

    def on_refresh_clicked(self):
        self.model.refresh_data()
        self.on_filter_changed()

    def on_filter_changed(self, *args):
        kw = self.le_search.text().strip()
        tf = self.cb_type.currentText()
        self.proxy_model.set_filter(keyword=kw, type_filter=tf)
        
    def on_import_clicked(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "选择点表配置 Excel 文件", "", "Excel Files (*.xlsx *.xls)"
        )
        if not file_path:
            return

        # 异步后台执行 Excel 解析，防止界面无响应并保留强引用
        self._spawn_background_task(self._do_async_import(file_path))

    async def _do_async_import(self, file_path: str):
        self.btn_import.setEnabled(False)
        self.btn_import.setText("正在导入...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            loop = asyncio.get_event_loop()
            count, msg = await loop.run_in_executor(None, import_aliases_from_excel, file_path, self.dm)
            if count > 0:
                QMessageBox.information(self, "导入成功", msg)
                self.on_refresh_clicked()
            else:
                QMessageBox.warning(self, "导入失败或为空", msg)
        except Exception as e:
            global_logger.error(f"Async import failed: {e}")
            QMessageBox.critical(self, "导入异常", str(e))
        finally:
            QApplication.restoreOverrideCursor()
            self.btn_import.setEnabled(True)
            self.btn_import.setText("导入 Excel 映射别名")

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

        self._spawn_background_task(self._do_async_export(file_path, all_nodes))

    async def _do_async_export(self, file_path: str, all_nodes: list):
        self.btn_export.setEnabled(False)
        self.btn_export.setText("正在导出...")
        QApplication.setOverrideCursor(Qt.WaitCursor)
        try:
            loop = asyncio.get_event_loop()
            count = await loop.run_in_executor(None, _export_worker, file_path, all_nodes)
            QMessageBox.information(self, "导出成功", f"已将 {count} 个点位导出到:\n{file_path}")
        except Exception as e:
            global_logger.error(f"Async export failed: {e}")
            QMessageBox.critical(self, "导出失败", str(e))
        finally:
            QApplication.restoreOverrideCursor()
            self.btn_export.setEnabled(True)
            self.btn_export.setText("导出所有点位")
