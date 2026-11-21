"""主界面尺寸回归测试。

背景：主界面曾经的最小尺寸被内容撑到 1365x928 —— 在 1366x768 / 1024x768 的现场屏上
即使全屏也看不到底部内容（用户实际反馈的问题）。这类退化不会被功能测试发现，
因此这里用 Qt 自身的最小尺寸计算把它钉住。

覆盖：
* 窗口最小尺寸必须小于 1024x768 屏幕的可用区域（留出任务栏与标题栏）。
* 每个页签在目标视口尺寸下不能把交互控件挤出可视区（设置页允许纵向滚动）。
* 设置页必须真的可滚动，且不需要横向滚动。
"""

import os
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 必须在导入任何 Qt 模块之前设置：Windows 上若沿用默认平台插件，无桌面会话时
# QApplication 构造会直接卡住。用 setdefault 会被外部已有的值覆盖，这里强制指定。
os.environ["QT_QPA_PLATFORM"] = "offscreen"

try:
    from PyQt5.QtWidgets import (
        QApplication, QWidget, QScrollArea, QPushButton, QLineEdit, QComboBox,
        QSpinBox, QCheckBox, QTimeEdit,
    )
    from PyQt5.QtCore import QPoint
    QT_AVAILABLE = True
except ImportError:
    try:
        from PySide6.QtWidgets import (
            QApplication, QWidget, QScrollArea, QPushButton, QLineEdit, QComboBox,
            QSpinBox, QCheckBox, QTimeEdit,
        )
        from PySide6.QtCore import QPoint
        QT_AVAILABLE = True
    except ImportError:
        QT_AVAILABLE = False

# 现场最常见的两种屏幕；窗口还要给标题栏与任务栏留出空间
SMALL_SCREEN = (1024, 768)
LARGE_SCREEN = (1366, 768)
WINDOW_CHROME_HEIGHT = 45      # 标题栏 + 任务栏
CHROME_RESERVED = 130          # 页头 + 仪表盘 + 底部时钟

INTERACTIVE = (QPushButton, QLineEdit, QComboBox, QSpinBox, QCheckBox, QTimeEdit)


@unittest.skipUnless(QT_AVAILABLE, "PyQt5/PySide6 不可用")
class TestMainWindowFitsScreens(unittest.TestCase):
    app = None

    @classmethod
    def setUpClass(cls):
        cls.app = QApplication.instance() or QApplication([])

    def setUp(self):
        # 隔离配置目录：绝不触碰仓库里的 data/lighting_config.json
        self._tmp = tempfile.TemporaryDirectory(prefix="lc_ui_")
        self._base_patch = patch("utils.persistence._get_base_dir", return_value=self._tmp.name)
        self._base_patch.start()
        from ui.main_window import MainWindow
        self.window = MainWindow()
        self.window.show()
        self.app.processEvents()

    def tearDown(self):
        try:
            # closeEvent 会弹出退出密码对话框（模态，会挂住测试）；置 _force_close
            # 只绕过"关闭到托盘"分支，仍需密码 —— 因此直接隐藏并销毁，不触发 close。
            self.window.hide()
            self.window.deleteLater()
            self.app.processEvents()
        finally:
            self._base_patch.stop()
            self._tmp.cleanup()

    def test_window_minimum_fits_smallest_screen(self):
        """窗口最小尺寸必须能塞进 1024x768（含标题栏与任务栏）。"""
        w, h = SMALL_SCREEN
        min_w = max(self.window.minimumWidth(), self.window.minimumSizeHint().width())
        min_h = max(self.window.minimumHeight(), self.window.minimumSizeHint().height())
        self.assertLessEqual(
            min_w, w - 20,
            f"窗口最小宽度 {min_w}px 超过屏幕宽度 {w}px，用户全屏也看不到右侧内容"
        )
        self.assertLessEqual(
            min_h, h - WINDOW_CHROME_HEIGHT,
            f"窗口最小高度 {min_h}px 超过屏幕可用高度 {h - WINDOW_CHROME_HEIGHT}px，"
            f"用户全屏也看不到底部内容"
        )

    def test_initial_size_does_not_exceed_screen(self):
        """默认打开时的尺寸不应超过 1366x768 的可用区域。"""
        w, h = LARGE_SCREEN
        self.assertLessEqual(self.window.width(), w)
        self.assertLessEqual(self.window.height(), h - WINDOW_CHROME_HEIGHT)

    def _per_tab_viewport(self, screen):
        w, h = screen
        return w - 10, h - WINDOW_CHROME_HEIGHT - CHROME_RESERVED

    def test_tabs_fit_target_viewport(self):
        """每个页签在目标视口下不得把交互控件挤出可视区。"""
        for name in ("tab_monitor", "tab_group", "tab_logs"):
            tab = getattr(self.window, name)
            view_w, view_h = self._per_tab_viewport(SMALL_SCREEN)
            tab.setParent(None)
            tab.resize(view_w, view_h)
            tab.show()
            self.app.processEvents()
            self.app.processEvents()

            outside = []
            for child in tab.findChildren(QWidget):
                if not isinstance(child, INTERACTIVE) or child.isHidden():
                    continue
                pos = child.mapTo(tab, QPoint(0, 0))
                if (pos.x() < 0 or pos.y() < 0
                        or pos.x() > tab.width() - 20 or pos.y() > tab.height() - 20):
                    text = ""
                    try:
                        text = child.text()
                    except Exception:
                        pass
                    outside.append(f"{type(child).__name__}({text[:16]}) at ({pos.x()},{pos.y()})")
            self.assertEqual(
                outside, [],
                f"{name} 在 {view_w}x{view_h} 视口下有控件不可见: {outside[:5]}"
            )

    def test_settings_tab_is_vertically_scrollable_only(self):
        """设置页靠滚动容纳长表单：纵向可滚，横向不需要滚。"""
        tab = self.window.tab_settings
        view_w, view_h = self._per_tab_viewport(SMALL_SCREEN)
        tab.setParent(None)
        tab.resize(view_w, view_h)
        tab.show()
        self.app.processEvents()
        self.app.processEvents()

        areas = tab.findChildren(QScrollArea)
        self.assertTrue(areas, "设置页缺少滚动区域，长表单会把窗口最小高度撑爆")
        scroll = areas[0]
        self.assertGreater(
            scroll.verticalScrollBar().maximum(), 0,
            "设置页内容未超出视口，滚动条不可用（布局可能被压缩到不可读）"
        )
        self.assertEqual(
            scroll.horizontalScrollBar().maximum(), 0,
            "设置页出现横向滚动条，说明控件宽度超出了视口"
        )


if __name__ == "__main__":
    unittest.main()
