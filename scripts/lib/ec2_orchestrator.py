from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from aws_env import build_boto3_session, load_aws_env_config
from ec2_service import (
    associate_instance_profile,
    create_instance,
    ensure_ssm_role_and_profile,
    get_al2023_ami_id,
    get_instance,
    wait_for_ssm_online,
)


@dataclass
class AwsClients:
    region: str
    ec2: Any
    ssm: Any
    iam: Any


def build_aws_clients(*, env_file: str, region_override: str = "") -> AwsClients:
    config = load_aws_env_config(env_file)
    region = (region_override or config.region or "").strip()
    if not region:
        raise RuntimeError("Region is required. Set AWS_REGION in env file or pass --region.")

    session = build_boto3_session(config, region_override=region)
    return AwsClients(
        region=region,
        ec2=session.client("ec2", region_name=region),
        ssm=session.client("ssm", region_name=region),
        iam=session.client("iam", region_name=region),
    )


def ensure_ssm_online_or_raise(
    ssm_client: Any,
    *,
    instance_id: str,
    timeout_seconds: int = 420,
    poll_seconds: int = 10,
) -> Dict[str, Any]:
    info = wait_for_ssm_online(
        ssm_client,
        instance_id,
        timeout_seconds=max(int(timeout_seconds), 30),
        poll_seconds=max(int(poll_seconds), 1),
    )
    if not info:
        raise RuntimeError(f"SSM is not online for instance within timeout: {instance_id}")
    return info


def create_benchmark_instance(
    ec2_client: Any,
    ssm_client: Any,
    iam_client: Any,
    *,
    instance_type: str,
    architecture: str = "x86_64",
    name_tag: str = "ec2-benchmark",
    ami_id: str = "",
    no_public_ip: bool = False,
    subnet_id: str = "",
    security_group_ids: Optional[List[str]] = None,
    key_name: str = "",
    iam_instance_profile_name: str = "",
    ensure_ssm_profile: bool = True,
) -> Dict[str, Any]:
    selected_ami = ami_id.strip() or get_al2023_ami_id(ssm_client, architecture)

    profile_name = iam_instance_profile_name.strip()
    if ensure_ssm_profile and not profile_name:
        profile_name = ensure_ssm_role_and_profile(iam_client)["ProfileName"]

    created = create_instance(
        ec2_client=ec2_client,
        ami_id=selected_ami,
        instance_type=instance_type,
        name_tag=name_tag,
        no_public_ip=bool(no_public_ip),
        subnet_id=subnet_id.strip() or None,
        security_group_ids=security_group_ids or [],
        key_name=key_name.strip() or None,
        iam_instance_profile_name=profile_name or None,
        wait_until_running=True,
    )

    return {
        "instance": created,
        "ami_id": selected_ami,
        "iam_profile_name": profile_name or None,
    }


def ensure_instance_ssm_profile(
    ec2_client: Any,
    iam_client: Any,
    *,
    instance_id: str,
    iam_instance_profile_name: str = "",
) -> Dict[str, Any]:
    profile_name = iam_instance_profile_name.strip()
    if not profile_name:
        profile_name = ensure_ssm_role_and_profile(iam_client)["ProfileName"]

    association_id = associate_instance_profile(ec2_client, instance_id, profile_name)
    latest = get_instance(ec2_client, instance_id)
    return {
        "instance": latest,
        "iam_profile_name": profile_name,
        "profile_association_id": association_id,
    }
