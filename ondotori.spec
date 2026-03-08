# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec file for おんどとりデータ取得ツール。"""

a = Analysis(
    ['main.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=['src', 'src.api_client', 'src.config_manager', 'src.data_processor',
                   'src.gap_manager', 'src.csv_exporter', 'src.gui',
                   'scripts', 'scripts.run'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='ondotori_tool',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    icon=None,
)
