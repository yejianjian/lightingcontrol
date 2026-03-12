import sys
import os

try:
    from PyQt5.QtGui import QImage, QPainter, QColor
    from PyQt5.QtCore import Qt
    from PyQt5.QtWidgets import QApplication
except ImportError:
    from PySide6.QtGui import QImage, QPainter, QColor
    from PySide6.QtCore import Qt
    from PySide6.QtWidgets import QApplication

def create_logo():
    app = QApplication(sys.argv)
    
    size = 256
    img = QImage(size, size, QImage.Format_ARGB32)
    img.fill(Qt.transparent)

    painter = QPainter(img)
    painter.setRenderHint(QPainter.Antialiasing)

    # 绘制科技感深色底
    painter.setBrush(QColor("#1e293b")) 
    painter.setPen(Qt.NoPen)
    painter.drawEllipse(10, 10, size-20, size-20)

    # 绘制明亮灯泡/控制核芯
    painter.setBrush(QColor("#10b981")) 
    painter.drawEllipse(70, 50, 116, 116)

    # 绘制底座
    painter.setBrush(QColor("#94a3b8")) 
    painter.drawRect(100, 150, 56, 40)
    painter.drawRect(110, 190, 36, 20)

    painter.end()
    
    out_path = os.path.join(os.path.dirname(__file__), "lighting_logo.png")
    ico_path = os.path.join(os.path.dirname(__file__), "lighting_logo.ico")
    
    img.save(out_path)
    img.save(ico_path)
    
    print(f"Logo saved to {out_path} & {ico_path}")

if __name__ == "__main__":
    create_logo()
