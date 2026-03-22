from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Optional

import boto3


class AwsConfigError(ValueError):
    """Raised when the local AWS env file is invalid."""


@dataclass
class AwsEnvConfig:
    access_key_id: str
    secret_access_key: str
    session_token: Optional[str] = None
    region: Optional[str] = None

    def to_dict(self) -> Dict[str, Optional[str]]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> "AwsEnvConfig":
        return cls(
            access_key_id=data["access_key_id"],
            secret_access_key=data["secret_access_key"],
            session_token=data.get("session_token"),
            region=data.get("region"),
        )


def parse_env_file(env_file_path: str) -> Dict[str, str]:
    """Parse KEY=VALUE lines from an env file (compatible with macOS and Windows)."""
    path = Path(env_file_path).expanduser().resolve()
    if not path.exists():
        raise AwsConfigError(f"Env file does not exist: {path}")
    if not path.is_file():
        raise AwsConfigError(f"Env file path is not a file: {path}")

    env_map: Dict[str, str] = {}
    content = path.read_text(encoding="utf-8")

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if (
            len(value) >= 2
            and value[0] == value[-1]
            and value[0] in ("'", '"')
        ):
            value = value[1:-1]

        env_map[key] = value

    return env_map


def load_aws_env_config(env_file_path: str) -> AwsEnvConfig:
    env_map = parse_env_file(env_file_path)

    access_key_id = env_map.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_access_key = env_map.get("AWS_SECRET_ACCESS_KEY", "").strip()
    session_token = env_map.get("AWS_SESSION_TOKEN", "").strip() or None
    region = (
        env_map.get("AWS_REGION", "").strip()
        or env_map.get("AWS_DEFAULT_REGION", "").strip()
        or None
    )

    missing = []
    if not access_key_id:
        missing.append("AWS_ACCESS_KEY_ID")
    if not secret_access_key:
        missing.append("AWS_SECRET_ACCESS_KEY")

    if missing:
        joined = ", ".join(missing)
        raise AwsConfigError(f"Required key(s) missing in env file: {joined}")

    return AwsEnvConfig(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        session_token=session_token,
        region=region,
    )


def build_boto3_session(
    config: AwsEnvConfig, region_override: Optional[str] = None
) -> boto3.session.Session:
    region_name = (region_override or config.region or "").strip() or None
    return boto3.session.Session(
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        aws_session_token=config.session_token,
        region_name=region_name,
    )


def mask_access_key(access_key_id: str) -> str:
    if len(access_key_id) <= 4:
        return "*" * len(access_key_id)
    return f"{'*' * (len(access_key_id) - 4)}{access_key_id[-4:]}"

