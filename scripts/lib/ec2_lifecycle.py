from __future__ import annotations

from typing import Any, Dict, List, Optional

from ec2_service import create_instance, terminate_instances


def create_ec2_instance(
    ec2_client: Any,
    *,
    ami_id: str,
    instance_type: str,
    name_tag: str,
    no_public_ip: bool,
    subnet_id: Optional[str] = None,
    security_group_ids: Optional[List[str]] = None,
    key_name: Optional[str] = None,
    iam_instance_profile_name: Optional[str] = None,
    wait_until_running: bool = True,
) -> Dict[str, Any]:
    return create_instance(
        ec2_client=ec2_client,
        ami_id=ami_id,
        instance_type=instance_type,
        name_tag=name_tag,
        no_public_ip=no_public_ip,
        subnet_id=subnet_id,
        security_group_ids=security_group_ids,
        key_name=key_name,
        iam_instance_profile_name=iam_instance_profile_name,
        wait_until_running=wait_until_running,
    )


def terminate_ec2_instances(
    ec2_client: Any,
    instance_ids: List[str],
) -> List[Dict[str, Any]]:
    normalized_ids = [str(item).strip() for item in (instance_ids or []) if str(item).strip()]
    if not normalized_ids:
        raise ValueError("instance_ids cannot be empty.")
    return terminate_instances(ec2_client, normalized_ids)
