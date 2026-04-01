from __future__ import annotations

from pathlib import Path
from typing import Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COREMARK_LINUX_BUNDLE_DIR = PROJECT_ROOT / "bin" / "coremark"
FIO_LINUX_BUNDLE_DIR = PROJECT_ROOT / "bin" / "fio"

COREMARK_ARCH_TO_BUNDLE_DIR = {
    "x86_64": "x86_64",
    "arm64": "arm64",
    "aarch64": "arm64",
}
FIO_ARCH_TO_BUNDLE_DIR = {
    "x86_64": "x86_64",
    "arm64": "arm64",
    "aarch64": "arm64",
}


def resolve_coremark_bundle_for_arch(architecture: str) -> Dict[str, object]:
    arch = str(architecture or "").strip().lower()
    bundle_dir_name = COREMARK_ARCH_TO_BUNDLE_DIR.get(arch)
    if not arch:
        raise RuntimeError("Instance architecture is unavailable.")
    if not bundle_dir_name:
        raise RuntimeError(f"Unsupported architecture for coremark bundle: {arch}")

    bundle_root = COREMARK_LINUX_BUNDLE_DIR / bundle_dir_name
    coremark_binary = bundle_root / "coremark"
    if not coremark_binary.exists() or not coremark_binary.is_file():
        raise RuntimeError(
            "Local coremark binary file is missing:\n" + str(coremark_binary)
        )

    return {
        "architecture": arch,
        "bundle_root": bundle_root,
        "coremark_binary": coremark_binary,
    }


def resolve_fio_bundle_for_arch(architecture: str) -> Dict[str, object]:
    arch = str(architecture or "").strip().lower()
    bundle_dir_name = FIO_ARCH_TO_BUNDLE_DIR.get(arch)
    if not arch:
        raise RuntimeError("Instance architecture is unavailable.")
    if not bundle_dir_name:
        raise RuntimeError(f"Unsupported architecture for fio bundle: {arch}")

    bundle_root = FIO_LINUX_BUNDLE_DIR / bundle_dir_name
    fio_binary = bundle_root / "fio"
    fio_engine = bundle_root / "engines" / "fio-libaio.so"
    shared_libs = [
        bundle_root / "lib" / "libaio.so.1",
        bundle_root / "lib" / "libaio.so.1.0.1",
        bundle_root / "lib" / "libnuma.so.1",
        bundle_root / "lib" / "libnuma.so.1.0.0",
    ]

    required_paths = [fio_binary, fio_engine, *shared_libs]
    missing = [str(path) for path in required_paths if not path.exists()]
    if missing:
        raise RuntimeError("Local fio bundle files are missing:\n" + "\n".join(missing))

    return {
        "architecture": arch,
        "bundle_root": bundle_root,
        "fio_binary": fio_binary,
        "fio_engine": fio_engine,
        "shared_libs": shared_libs,
    }
