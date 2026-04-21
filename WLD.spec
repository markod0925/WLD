# -*- mode: python ; coding: utf-8 -*-

import os
from pathlib import Path

from PyInstaller.utils.hooks import collect_dynamic_libs, collect_submodules


ROOT = Path(os.path.dirname(os.path.abspath("WLD.spec")))
SRC = ROOT / "src"

datas = [
    (str(ROOT / "README.md"), "."),
    (str(ROOT / "sample_config.json"), "."),
    (str(ROOT / "assets" / "WLD_Logo.png"), "assets"),
    (str(ROOT / "assets" / "WLD_Logo.ico"), "assets"),
]

hiddenimports = collect_submodules("worklog_diary")
binaries = []

# The LM Studio client imports requests at runtime and the frozen bundle must carry it.
for package_name in ("requests",):
    try:
        hiddenimports += collect_submodules(package_name)
    except Exception:
        pass

for package_name in ("sqlcipher3", "pysqlcipher3"):
    try:
        hiddenimports += collect_submodules(package_name)
    except Exception:
        pass
    try:
        binaries += collect_dynamic_libs(package_name)
    except Exception:
        pass

a = Analysis(
    [str(ROOT / "wld_launcher.py")],
    pathex=[str(SRC)],
    binaries=binaries,
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
    icon=str(ROOT / "assets" / "WLD_Logo.ico"),
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
