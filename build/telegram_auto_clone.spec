# -*- mode: python ; coding: utf-8 -*-
from pathlib import Path
from PyInstaller.utils.hooks import collect_all


datas = []
binaries = []
hiddenimports = []

for package_name in (
    "aiosqlite",
    "cryptg",
    "fastapi",
    "fasttelethonhelper",
    "jinja2",
    "pydantic",
    "pydantic_settings",
    "qrcode",
    "starlette",
    "telethon",
    "uvicorn",
):
    package_datas, package_binaries, package_hiddenimports = collect_all(package_name)
    datas += package_datas
    binaries += package_binaries
    hiddenimports += package_hiddenimports

root_dir = Path(__file__).resolve().parent.parent

datas += [
    (str(root_dir / "app/static"), "app/static"),
    (str(root_dir / "app/templates"), "app/templates"),
]

entry_script = root_dir / "run_server.py"

a = Analysis(
    [str(entry_script)],
    pathex=[str(root_dir)],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
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
    [],
    exclude_binaries=True,
    name="telegram-auto-clone",
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=False,
    console=True,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=False,
    name="telegram-auto-clone",
)
