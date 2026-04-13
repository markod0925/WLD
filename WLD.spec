# -*- mode: python ; coding: utf-8 -*-

import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_submodules


ROOT = Path(os.path.dirname(os.path.abspath("WLD.spec")))
SRC = ROOT / "src"

datas = [
    (str(ROOT / "README.md"), "."),
    (str(ROOT / "sample_config.json"), "."),
]

hiddenimports = collect_submodules("worklog_diary")

a = Analysis(
    [str(ROOT / "wld_launcher.py")],
    pathex=[str(SRC)],
    binaries=[],
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name="WLD",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=False,
)

coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="WLD",
)
