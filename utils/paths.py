"""
应用路径工具模块 — 统一管理 frozen/dev 环境下的基础路径
"""
import os
import sys


def get_base_path() -> str:
    """获取应用基础路径：打包后为临时解压目录(_MEIPASS)，开发时为项目根目录"""
    if getattr(sys, 'frozen', False):
        return sys._MEIPASS
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
