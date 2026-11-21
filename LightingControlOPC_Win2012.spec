# -*- mode: python ; coding: utf-8 -*-
# 单文件打包配置（Windows Server 2012 目标）。
# EXE() 里直接传入 a.binaries / a.datas 即为 onefile 模式；数据文件运行时解压到
# sys._MEIPASS（只读资源），可写数据（证书、配置、日志）由 utils.paths.get_app_path()
# 放在 exe 同级目录，保证重启后仍然存在。
import os

# 数据文件
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

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    # 只列真正需要但静态分析看不到的模块。'asyncqt' 是 qasync 的旧前身，本项目
    # 未使用；把它写进 hiddenimports 只会在每次构建时产生 "Hidden import not found"。
    hiddenimports=['qasync', 'PyQt5', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 'PyQt5.sip', 'ctypes', 'ctypes.wintypes', 'utils.security'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
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
    name='LightingControl_v1.4.0',
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
