# -*- mode: python ; coding: utf-8 -*-
# OPC DA 版单文件打包配置（Windows Server 2012 目标）。
# 入口 main_da.py（DaMainWindow + OpcDaClientEngine + DaTabSettings）。
# EXE() 里直接传入 a.binaries / a.datas 即为 onefile 模式；数据文件运行时解压到
# sys._MEIPASS（只读资源），可写数据（配置、日志）由 utils.paths.get_app_path()
# 放在 exe 同级目录，保证重启后仍然存在。
import os

# 数据文件（与 UA 版一致）
datas = []
os_path = os.path.join(os.getcwd(), 'ui', 'style.qss')
if os.path.exists(os_path):
    datas.append(('ui/style.qss', 'ui'))
data_json_src = os.path.join(os.getcwd(), 'data', 'lighting_config.json')
if os.path.exists(data_json_src):
    datas.append(('data/lighting_config.json', 'data'))
icon_png = os.path.join(os.getcwd(), 'lighting_logo.png')
if os.path.exists(icon_png):
    datas.append(('lighting_logo.png', '.'))
icon_ico = os.path.join(os.getcwd(), 'lighting_logo.ico')
if os.path.exists(icon_ico):
    datas.append(('lighting_logo.ico', '.'))

# Windows Server 2012 需要的 DirectX 编译库
binaries = []
d3d_path = r'C:\Windows\System32\d3dcompiler_47.dll'
if os.path.exists(d3d_path):
    binaries.append((d3d_path, '.'))

def _collect_comtypes_submodules():
    """枚举已安装 comtypes 的全部子模块，交给 hiddenimports 打包。

    comtypes 大量使用动态 import / 延迟导入（client._generate、automation 的
    VARIANT/BSTR、connectionpoints 的 IConnectionPoint 等），PyInstaller 的静态
    分析看不到，必须在 hiddenimports 里显式列出，否则运行时会
    ModuleNotFoundError: No module named 'comtypes.xxx'。
    """
    names = []
    try:
        import comtypes
    except Exception:
        return names
    base = os.path.dirname(comtypes.__file__)
    for root, dirs, files in os.walk(base):
        dirs[:] = [d for d in dirs if d not in ('__pycache__', 'test', 'tests')]
        for filename in files:
            if not filename.endswith('.py'):
                continue
            rel = os.path.relpath(os.path.join(root, filename), base)
            if rel == '__init__.py':
                continue
            module = 'comtypes.' + rel[:-3].replace(os.sep, '.')
            if module.endswith('.__init__'):
                module = module[:-len('.__init__')]
            names.append(module)
    return names


hiddenimports = [
    'qasync',
    'PyQt5', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 'PyQt5.sip',
    'ctypes', 'ctypes.wintypes',
    'utils.security',
    # OPC DA 低层模块（含引擎的延迟导入路径，静态分析可能看不到）
    'opcda', 'opcda.com', 'opcda.client', 'opcda.engine',
    # comtypes：必须整包打包，否则 OPC DA 客户端在打包版里无法创建 COM 对象
    'comtypes', 'comtypes.client', 'comtypes.automation', 'comtypes.typeinfo',
    'comtypes.connectionpoints', 'comtypes.GUID', 'comtypes.hresult',
    'comtypes._comobject', 'comtypes._safearray', 'comtypes.safearray',
    'comtypes.messageloop', 'comtypes.errorinfo', 'comtypes.persist',
    'comtypes.stream', 'comtypes._post_coinit',
]
for _name in _collect_comtypes_submodules():
    if _name not in hiddenimports:
        hiddenimports.append(_name)

a = Analysis(
    ['main_da.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    # 注意：不能排除 asyncua / cryptography。ui/main_window.py 无条件 import
    # opc.client_engine（UA 引擎），DA 版复用同一个 MainWindow 基类，因此这些
    # 依赖在导入期就必须存在；排除它们会让 exe 启动即 ModuleNotFoundError。
    excludes=['PySide6', 'shiboken6'],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='LightingControlDA_v1.2.1',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['lighting_logo.ico'],
)
