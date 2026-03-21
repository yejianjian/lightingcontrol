try:
    from PyQt5.QtWidgets import QStyledItemDelegate, QPushButton, QStyle, QApplication, QStyleOptionButton, QMessageBox
    from PyQt5.QtCore import Qt, pyqtSignal, QEvent, QPoint
    from PyQt5.QtGui import QCursor
except ImportError:
    from PySide6.QtWidgets import QStyledItemDelegate, QPushButton, QStyle, QApplication, QStyleOptionButton, QMessageBox
    from PySide6.QtCore import Qt, Signal as pyqtSignal, QEvent, QPoint
    from PySide6.QtGui import QCursor

import asyncio
from utils.logger import global_logger

class ActionButtonDelegate(QStyledItemDelegate):
    """
    自绘表格控制按钮委托器
    在单元格内绘制 "干启 设备" 等控制按钮，并捕获点击事件
    """
    clicked = pyqtSignal(str, bool)

    def __init__(self, parent=None, data_manager=None, opc_engine=None):
        super().__init__(parent)
        self.dm = data_manager
        self.opc_engine = opc_engine
        self._hover_pos = QPoint(-1, -1)
        self._hover_index = None

    def paint(self, painter, option, index):
        if index.column() != 5: # 假设最后一列为控制列
            super().paint(painter, option, index)
            return

        model = index.model()
        if not model or not hasattr(model, '_data_cache'):
            return
            
        try:
            node = model._data_cache[index.row()]
        except IndexError:
            return

        dtype = str(node.get('type', '')).lower()
        if 'bool' not in dtype and 'boolean' not in dtype:
            # 对于非布尔类型，不再绘制按键，只画一个减号表示不可开关
            painter.drawText(option.rect, Qt.AlignCenter, "-")
            return
        
        # 为了复刻效果图，画两个按钮：【开启】 【关闭】
        btn1_rect = option.rect.adjusted(2, 2, -option.rect.width()//2 - 2, -2)
        btn2_rect = option.rect.adjusted(option.rect.width()//2 + 2, 2, -2, -2)
        
        # 判断鼠标是否悬停在按钮上
        is_hover1 = (self._hover_index == index) and btn1_rect.contains(self._hover_pos)
        is_hover2 = (self._hover_index == index) and btn2_rect.contains(self._hover_pos)

        opt_btn1 = QStyleOptionButton()
        opt_btn1.rect = btn1_rect
        opt_btn1.text = "开启"
        opt_btn1.state = QStyle.State_Enabled | QStyle.State_Active
        if is_hover1:
            opt_btn1.state |= QStyle.State_MouseOver

        opt_btn2 = QStyleOptionButton()
        opt_btn2.rect = btn2_rect
        opt_btn2.text = "关闭"
        opt_btn2.state = QStyle.State_Enabled | QStyle.State_Active
        if is_hover2:
            opt_btn2.state |= QStyle.State_MouseOver

        # 拿当前的QStyle画出来
        QApplication.style().drawControl(QStyle.CE_PushButton, opt_btn1, painter)
        QApplication.style().drawControl(QStyle.CE_PushButton, opt_btn2, painter)

    def editorEvent(self, event, model, option, index):
        if index.column() == 5:
            # 捕获鼠标移动用于处理悬停事件
            if event.type() == QEvent.MouseMove:
                self._hover_pos = event.pos()
                self._hover_index = index
                if self.parent() and hasattr(self.parent(), 'viewport'):
                    self.parent().viewport().update()
                return True
                
            # 也可以处理鼠标离开事件
            if event.type() in (QEvent.Leave, QEvent.FocusOut):
                self._hover_pos = QPoint(-1, -1)
                self._hover_index = None
                if self.parent() and hasattr(self.parent(), 'viewport'):
                    self.parent().viewport().update()
                    
            if event.type() == QEvent.MouseButtonRelease:
                if not hasattr(model, '_data_cache'): return False
                
                try:
                    node = model._data_cache[index.row()]
                except IndexError:
                    return False
                    
                dtype = str(node.get('type', '')).lower()
                if 'bool' not in dtype and 'boolean' not in dtype:
                    return False # 非布尔不再阻拦处理
                    
                node_id = node.get('node_id')
                
                click_pos = event.pos()
                btn1_rect = option.rect.adjusted(2, 2, -option.rect.width()//2 - 2, -2)
                btn2_rect = option.rect.adjusted(option.rect.width()//2 + 2, 2, -2, -2)
                
                if btn1_rect.contains(click_pos):
                    self._dispatch_write(node_id, True, "开启")
                    return True
                elif btn2_rect.contains(click_pos):
                    self._dispatch_write(node_id, False, "关闭")
                    return True

        return super().editorEvent(event, model, option, index)
        
    def _dispatch_write(self, node_id, target_val, action_name):
        # 获取别名用于日志显示
        alias = self.dm.get_alias_by_node_id(node_id)
        global_logger.info(f"==== UI单点操作触发 ==== -> [点位: {alias}] | 指令发送: 【{action_name}】")

        if not self.opc_engine.connected:
            QMessageBox.warning(self.parent(), "错误", "目前未连接到 OPC UA 服务器。")
            return

        # 操作反馈：改变光标状态
        QApplication.setOverrideCursor(Qt.WaitCursor)
        # 发起写入异步任务
        task = asyncio.create_task(self.opc_engine.write_node_value(node_id, target_val))

        # 挂载回调恢复光标，确保无论如何都恢复光标
        def on_done(t):
            try:
                QApplication.restoreOverrideCursor()
            except Exception:
                pass  # 防止 widget 已销毁时出错
        task.add_done_callback(on_done)
