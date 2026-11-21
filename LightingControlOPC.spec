# -*- mode: python ; coding: utf-8 -*-


import os
os_path = os.path.join(os.getcwd(), 'ui', 'style.qss')
datas = []
if os.path.exists(os_path):
    datas.append(('ui/style.qss', 'ui'))
data_json_src = os.path.join(os.getcwd(), 'data', 'lighting_config.json')
if os.path.exists(data_json_src):
    datas.append(('data/lighting_config.json', 'data'))
icon_path = os.path.join(os.getcwd(), 'lighting_logo.png')
if os.path.exists(icon_path):
    datas.append(('lighting_logo.png', '.'))

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=datas,
    hiddenimports=['qasync', 'asyncqt'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='LightingControlOPC',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='LightingControlOPC',
)
