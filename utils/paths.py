"""应用路径工具模块 — 统一管理 frozen/dev 环境下的基础路径。

必须区分两类路径，混用会直接导致 24/7 现场的稳定性事故：

* **可写数据目录**（客户端证书/私钥、`server_cert_0.der`、`data/lighting_config.json`、
  `logs/`）：必须放在可执行文件**旁边**，重启后依然存在。若写到
  PyInstaller 的 ``sys._MEIPASS``（单文件模式下的临时解压目录），程序退出即被删除，
  结果就是每次启动都重新生成 2048 位客户端证书 —— 服务端信任列表随之失配、
  握手被拒；`server_cert_0.der` 的缓存也永远无法生效。
* **只读资源目录**（`ui/style.qss`、logo）：随包分发，位置由 ``_MEIPASS`` 决定。
"""
import os
import sys


def get_app_path() -> str:
    """可写数据目录：打包后为可执行文件所在目录，开发时为项目根目录。"""
    if getattr(sys, 'frozen', False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_resource_path() -> str:
    """只读资源目录：打包后为临时解压目录(_MEIPASS)，开发时为项目根目录。"""
    if getattr(sys, 'frozen', False):
        return getattr(sys, '_MEIPASS', get_app_path())
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def get_base_path() -> str:
    """向后兼容别名：语义为"可写数据目录"。

    历史上本函数既被用于读资源也被用于写证书，导致打包后证书无法持久化。
    新代码请显式使用 :func:`get_app_path`（可写）或 :func:`get_resource_path`（只读）。
    """
    return get_app_path()
