"""Setuptools hooks for generating gRPC Python stubs during package builds."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

from setuptools import Command, find_packages, setup
from setuptools.command.build_py import build_py as _build_py

try:
    from setuptools.command.develop import develop as _develop
except ImportError:  # pragma: no cover
    _develop = None

try:
    from setuptools.command.editable_wheel import editable_wheel as _editable_wheel
except ImportError:  # pragma: no cover - setuptools>=64 provides editable_wheel
    _editable_wheel = None

PROJECT_ROOT = Path(__file__).resolve().parent
PROTO_FILENAME = "hl_historical.proto"
PROTO_OUTPUT_DIR = PROJECT_ROOT / "src" / "hl_historical_client" / "proto"


def _find_repo_root_proto() -> Optional[Path]:
    current = PROJECT_ROOT.parent
    while True:
        candidate = current / "proto" / PROTO_FILENAME
        if candidate.is_file():
            return candidate

        parent = current.parent
        if parent == current:
            return None
        current = parent


def _sync_bundled_proto(repo_proto: Path, bundled_proto: Path) -> None:
    bundled_proto.parent.mkdir(parents=True, exist_ok=True)
    if not bundled_proto.exists() or bundled_proto.read_bytes() != repo_proto.read_bytes():
        shutil.copy2(repo_proto, bundled_proto)


def _find_proto_file() -> Path:
    env_proto_path = os.environ.get("HL_HISTORICAL_PROTO_PATH")
    if env_proto_path:
        proto_path = Path(env_proto_path).expanduser().resolve()
        if proto_path.is_file():
            return proto_path
        raise RuntimeError(
            f"HL_HISTORICAL_PROTO_PATH points to a missing file: {proto_path}"
        )

    bundled_proto = PROJECT_ROOT / "proto" / PROTO_FILENAME
    repo_root_proto = _find_repo_root_proto()

    if repo_root_proto is not None:
        _sync_bundled_proto(repo_root_proto, bundled_proto)

    if bundled_proto.is_file():
        return bundled_proto

    raise RuntimeError(
        "Could not locate proto/hl_historical.proto. "
        "Set HL_HISTORICAL_PROTO_PATH to override the location."
    )


def _grpc_tools_include_dir() -> Path:
    try:
        import grpc_tools
    except ImportError as exc:  # pragma: no cover - surfaced during install/build
        raise RuntimeError(
            "grpcio-tools is required to generate Python gRPC stubs during build"
        ) from exc

    return Path(grpc_tools.__path__[0]) / "_proto"


def _rewrite_grpc_imports(path: Path) -> None:
    if not path.exists():
        return

    original = path.read_text(encoding="utf-8")
    rewritten = re.sub(
        r"^import ([A-Za-z0-9_]+_pb2) as ([A-Za-z0-9_]+)$",
        r"from . import \1 as \2",
        original,
        flags=re.MULTILINE,
    )

    if rewritten != original:
        path.write_text(rewritten, encoding="utf-8")


def generate_proto() -> None:
    proto_file = _find_proto_file()
    include_dir = _grpc_tools_include_dir()

    PROTO_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    (PROTO_OUTPUT_DIR / "__init__.py").touch(exist_ok=True)

    cmd = [
        sys.executable,
        "-m",
        "grpc_tools.protoc",
        f"-I{proto_file.parent}",
        f"-I{include_dir}",
        f"--python_out={PROTO_OUTPUT_DIR}",
        f"--pyi_out={PROTO_OUTPUT_DIR}",
        f"--grpc_python_out={PROTO_OUTPUT_DIR}",
        str(proto_file),
    ]
    subprocess.check_call(cmd, cwd=PROJECT_ROOT)

    pb2_grpc_py = PROTO_OUTPUT_DIR / f"{proto_file.stem}_pb2_grpc.py"
    pb2_grpc_pyi = PROTO_OUTPUT_DIR / f"{proto_file.stem}_pb2_grpc.pyi"
    _rewrite_grpc_imports(pb2_grpc_py)
    _rewrite_grpc_imports(pb2_grpc_pyi)


class GenerateProtoCommand(Command):
    description = "Generate Python protobuf + gRPC stubs"
    user_options: list[tuple[str, str, str]] = []

    def initialize_options(self) -> None:
        return None

    def finalize_options(self) -> None:
        return None

    def run(self) -> None:
        generate_proto()


class BuildPyCommand(_build_py):
    def run(self) -> None:
        self.run_command("generate_proto")
        super().run()


cmdclass: dict[str, type[Command]] = {
    "generate_proto": GenerateProtoCommand,
    "build_py": BuildPyCommand,
}

if _develop is not None:

    class DevelopCommand(_develop):
        def run(self) -> None:
            self.run_command("generate_proto")
            super().run()

    cmdclass["develop"] = DevelopCommand

if _editable_wheel is not None:

    class EditableWheelCommand(_editable_wheel):
        def run(self) -> None:
            self.run_command("generate_proto")
            super().run()

    cmdclass["editable_wheel"] = EditableWheelCommand

setup(
    name="hl-historical-client",
    version="0.1.0",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    package_data={
        "hl_historical_client": ["py.typed"],
        "hl_historical_client.proto": ["*.pyi"],
    },
    cmdclass=cmdclass,
)
