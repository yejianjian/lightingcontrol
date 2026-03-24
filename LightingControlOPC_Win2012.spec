# -*- mode: python ; coding: utf-8 -*-
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
    hiddenimports=['qasync', 'asyncqt', 'PyQt5', 'PyQt5.QtCore', 'PyQt5.QtGui', 'PyQt5.QtWidgets', 'PyQt5.sip'],
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
    name='LightingControl_v1.2.3beta',
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
