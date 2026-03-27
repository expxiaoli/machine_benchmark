from __future__ import annotations

import base64
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from botocore.exceptions import ClientError


AL2023_PARAMETER_BY_ARCH = {
    "x86_64": "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
    "arm64": "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
}

SSM_ENDPOINT_SERVICES = ("ssm", "ssmmessages", "ec2messages")

COREMARK_SCORE_PATTERN = re.compile(r"CoreMark\s+1\.0\s*:\s*([0-9]+(?:\.[0-9]+)?)")
COREMARK_ITER_PER_SEC_PATTERN = re.compile(
    r"Iterations/Sec\s*:\s*([0-9]+(?:\.[0-9]+)?)"
)
COREMARK_EXIT_CODE_PATTERN = re.compile(r"__COREMARK_EXIT_CODE__=(\d+)")
FIO_BW_PATTERN = re.compile(r"\bbw=([0-9]+(?:\.[0-9]+)?)([KMGTP]?i?B/s)")
FIO_IOPS_PATTERN = re.compile(r"\bIOPS=([0-9]+(?:\.[0-9]+)?)([kKmM]?)")
FIO_CPU_PATTERN = re.compile(
    r"\bcpu\s*:\s*usr=([0-9]+(?:\.[0-9]+)?)%,\s*sys=([0-9]+(?:\.[0-9]+)?)%",
    re.IGNORECASE,
)
FIO_DISK_UTIL_PATTERN = re.compile(r"\butil=([0-9]+(?:\.[0-9]+)?)%")
FIO_CLAT_AVG_PATTERN = re.compile(
    r"\bclat\s*\((nsec|usec|msec|sec)\):[^\n]*?\bavg=([0-9]+(?:\.[0-9]+)?)",
    re.IGNORECASE,
)
FIO_CLAT_PERCENTILES_SECTION_PATTERN = re.compile(
    r"clat\s+percentiles\s*\((nsec|usec|msec|sec)\):(?P<body>(?:\n\s*\|[^\n]*)+)",
    re.IGNORECASE,
)
FIO_P95_PATTERN = re.compile(r"95\.00th=\[\s*([0-9]+(?:\.[0-9]+)?)\]")
FIO_P99_PATTERN = re.compile(r"99\.00th=\[\s*([0-9]+(?:\.[0-9]+)?)\]")
FIO_EXIT_CODE_PATTERN = re.compile(r"__FIO_EXIT_CODE__=(\d+)")


def _extract_name_tag(tags: Optional[List[Dict[str, str]]]) -> str:
    if not tags:
        return ""
    for tag in tags:
        if tag.get("Key") == "Name":
            return tag.get("Value", "")
    return ""


def _as_iso8601(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return ""


def normalize_instance(instance: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "InstanceId": instance.get("InstanceId", ""),
        "Name": _extract_name_tag(instance.get("Tags")),
        "Architecture": instance.get("Architecture", ""),
        "State": instance.get("State", {}).get("Name", ""),
        "InstanceType": instance.get("InstanceType", ""),
        "PrivateIpAddress": instance.get("PrivateIpAddress"),
        "PublicIpAddress": instance.get("PublicIpAddress"),
        "ImageId": instance.get("ImageId", ""),
        "KeyName": instance.get("KeyName"),
        "SubnetId": instance.get("SubnetId"),
        "VpcId": instance.get("VpcId"),
        "SecurityGroupIds": [
            item.get("GroupId", "") for item in instance.get("SecurityGroups", [])
        ],
        "LaunchTime": _as_iso8601(instance.get("LaunchTime")),
        "IamInstanceProfileArn": (
            instance.get("IamInstanceProfile", {}) or {}
        ).get("Arn"),
    }


def list_instances(ec2_client: Any, include_terminated: bool = False) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {}
    if not include_terminated:
        params["Filters"] = [
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped", "shutting-down"],
            }
        ]

    paginator = ec2_client.get_paginator("describe_instances")
    instances: List[Dict[str, Any]] = []

    for page in paginator.paginate(**params):
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                instances.append(normalize_instance(instance))

    instances.sort(key=lambda item: (item["State"], item["InstanceId"]))
    return instances


def get_instance(ec2_client: Any, instance_id: str) -> Dict[str, Any]:
    response = ec2_client.describe_instances(InstanceIds=[instance_id])
    reservations = response.get("Reservations", [])
    if not reservations or not reservations[0].get("Instances"):
        raise RuntimeError(f"Instance not found: {instance_id}")
    return normalize_instance(reservations[0]["Instances"][0])


def get_al2023_ami_id(ssm_client: Any, architecture: str) -> str:
    param_name = AL2023_PARAMETER_BY_ARCH.get(architecture)
    if not param_name:
        raise ValueError(f"Unsupported architecture: {architecture}")
    response = ssm_client.get_parameter(Name=param_name)
    return response["Parameter"]["Value"]


def get_default_network(ec2_client: Any) -> Dict[str, str]:
    vpcs = ec2_client.describe_vpcs(
        Filters=[{"Name": "isDefault", "Values": ["true"]}]
    ).get("Vpcs", [])
    if not vpcs:
        raise RuntimeError("No default VPC found. Please provide subnet and security group.")
    vpc_id = vpcs[0]["VpcId"]

    subnets = ec2_client.describe_subnets(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "default-for-az", "Values": ["true"]},
        ]
    ).get("Subnets", [])
    if not subnets:
        subnets = ec2_client.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        ).get("Subnets", [])
    if not subnets:
        raise RuntimeError("No subnet found in default VPC.")
    subnets = sorted(subnets, key=lambda item: (item.get("AvailabilityZone", ""), item["SubnetId"]))
    subnet_id = subnets[0]["SubnetId"]

    sgs = ec2_client.describe_security_groups(
        Filters=[
            {"Name": "vpc-id", "Values": [vpc_id]},
            {"Name": "group-name", "Values": ["default"]},
        ]
    ).get("SecurityGroups", [])
    if not sgs:
        raise RuntimeError("No default security group found in default VPC.")
    security_group_id = sgs[0]["GroupId"]

    return {
        "VpcId": vpc_id,
        "SubnetId": subnet_id,
        "SecurityGroupId": security_group_id,
    }


def parse_security_group_ids(raw_value: str) -> List[str]:
    values = [item.strip() for item in raw_value.split(",")]
    return [item for item in values if item]


def list_instance_families(
    ec2_client: Any,
    architecture: str,
    *,
    only_current_generation: bool = True,
) -> List[str]:
    paginator = ec2_client.get_paginator("describe_instance_types")
    families = set()

    for page in paginator.paginate():
        for item in page.get("InstanceTypes", []):
            instance_type_name = item.get("InstanceType", "")
            if not instance_type_name or "." not in instance_type_name:
                continue

            supported_arch = item.get("ProcessorInfo", {}).get("SupportedArchitectures", [])
            if architecture not in supported_arch:
                continue
            if only_current_generation and not item.get("CurrentGeneration", False):
                continue

            family = instance_type_name.split(".", 1)[0]
            if family.startswith("u-") or family.startswith("mac"):
                continue
            families.add(family)

    return sorted(families)


def suggest_instance_types(
    ec2_client: Any,
    vcpu: int,
    memory_gib: float,
    architecture: str,
    family_prefixes: Optional[List[str]] = None,
    limit: int = 20,
) -> List[Dict[str, Any]]:
    required_memory_mib = int(memory_gib * 1024)
    normalized_prefixes = [
        prefix.strip().lower() for prefix in (family_prefixes or []) if prefix and prefix.strip()
    ]
    paginator = ec2_client.get_paginator("describe_instance_types")
    candidates: List[Dict[str, Any]] = []

    for page in paginator.paginate():
        for item in page.get("InstanceTypes", []):
            default_vcpu = item.get("VCpuInfo", {}).get("DefaultVCpus")
            memory_mib = item.get("MemoryInfo", {}).get("SizeInMiB")
            supported_arch = item.get("ProcessorInfo", {}).get("SupportedArchitectures", [])
            current_generation = item.get("CurrentGeneration", False)
            instance_type_name = item.get("InstanceType", "")

            if default_vcpu != vcpu:
                continue
            if memory_mib is None or memory_mib < required_memory_mib:
                continue
            if architecture not in supported_arch:
                continue
            if not current_generation:
                continue
            if instance_type_name.startswith("u-") or instance_type_name.startswith("mac"):
                continue
            if normalized_prefixes:
                lowered_type = instance_type_name.lower()
                # Match by family token, e.g. "c6i" -> "c6i.*"
                if not any(lowered_type.startswith(f"{prefix}.") for prefix in normalized_prefixes):
                    continue

            candidates.append(
                {
                    "InstanceType": instance_type_name,
                    "vCPU": default_vcpu,
                    "MemoryGiB": round(memory_mib / 1024.0, 2),
                }
            )

    candidates.sort(key=lambda item: (item["MemoryGiB"], item["InstanceType"]))
    return candidates[:limit]


def create_key_pair(ec2_client: Any, key_name: str) -> Dict[str, str]:
    response = ec2_client.create_key_pair(KeyName=key_name)
    return {
        "KeyName": response["KeyName"],
        "KeyFingerprint": response["KeyFingerprint"],
        "KeyMaterial": response["KeyMaterial"],
    }


def create_instance(
    ec2_client: Any,
    *,
    ami_id: str,
    instance_type: str,
    name_tag: str,
    no_public_ip: bool,
    subnet_id: Optional[str],
    security_group_ids: Optional[List[str]],
    key_name: Optional[str],
    iam_instance_profile_name: Optional[str],
    wait_until_running: bool,
) -> Dict[str, Any]:
    group_ids = security_group_ids or []
    run_args: Dict[str, Any] = {
        "ImageId": ami_id,
        "InstanceType": instance_type,
        "MinCount": 1,
        "MaxCount": 1,
    }

    if name_tag.strip():
        run_args["TagSpecifications"] = [
            {
                "ResourceType": "instance",
                "Tags": [{"Key": "Name", "Value": name_tag.strip()}],
            }
        ]

    if key_name:
        run_args["KeyName"] = key_name

    if iam_instance_profile_name:
        run_args["IamInstanceProfile"] = {"Name": iam_instance_profile_name}

    if no_public_ip:
        if not subnet_id:
            raise ValueError("SubnetId is required when creating a private instance.")
        if not group_ids:
            raise ValueError(
                "At least one security group is required when creating a private instance."
            )
        run_args["NetworkInterfaces"] = [
            {
                "DeviceIndex": 0,
                "SubnetId": subnet_id,
                "Groups": group_ids,
                "AssociatePublicIpAddress": False,
            }
        ]
    else:
        if subnet_id:
            run_args["SubnetId"] = subnet_id
        if group_ids:
            run_args["SecurityGroupIds"] = group_ids

    response = ec2_client.run_instances(**run_args)
    instance_id = response["Instances"][0]["InstanceId"]

    if wait_until_running:
        waiter = ec2_client.get_waiter("instance_running")
        waiter.wait(InstanceIds=[instance_id])

    return get_instance(ec2_client, instance_id)


def ensure_ssm_role_and_profile(
    iam_client: Any,
    role_name: str = "CodexEC2SSMRole",
    profile_name: str = "CodexEC2SSMInstanceProfile",
) -> Dict[str, str]:
    trust_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "ec2.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }

    try:
        iam_client.get_role(RoleName=role_name)
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
        )

    iam_client.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore",
    )

    try:
        profile = iam_client.get_instance_profile(InstanceProfileName=profile_name)[
            "InstanceProfile"
        ]
    except ClientError as error:
        if error.response["Error"]["Code"] != "NoSuchEntity":
            raise
        profile = iam_client.create_instance_profile(InstanceProfileName=profile_name)[
            "InstanceProfile"
        ]

    attached_role_names = [role["RoleName"] for role in profile.get("Roles", [])]
    if role_name not in attached_role_names:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name,
        )

    return {
        "RoleName": role_name,
        "ProfileName": profile_name,
        "ProfileArn": profile.get("Arn", ""),
    }


def associate_instance_profile(
    ec2_client: Any, instance_id: str, profile_name: str
) -> str:
    response = ec2_client.describe_iam_instance_profile_associations(
        Filters=[{"Name": "instance-id", "Values": [instance_id]}]
    )
    associations = response.get("IamInstanceProfileAssociations", [])

    if not associations:
        assoc = ec2_client.associate_iam_instance_profile(
            InstanceId=instance_id,
            IamInstanceProfile={"Name": profile_name},
        )["IamInstanceProfileAssociation"]
        return assoc["AssociationId"]

    current_association_id = associations[0]["AssociationId"]
    assoc = ec2_client.replace_iam_instance_profile_association(
        AssociationId=current_association_id,
        IamInstanceProfile={"Name": profile_name},
    )["IamInstanceProfileAssociation"]
    return assoc["AssociationId"]


def ensure_ssm_vpc_endpoints(
    ec2_client: Any,
    *,
    region: str,
    vpc_id: str,
    subnet_id: str,
    security_group_id: str,
    wait_timeout_seconds: int = 240,
) -> List[Dict[str, Any]]:
    endpoint_ids: List[str] = []

    for service_suffix in SSM_ENDPOINT_SERVICES:
        service_name = f"com.amazonaws.{region}.{service_suffix}"
        existing = ec2_client.describe_vpc_endpoints(
            Filters=[
                {"Name": "vpc-id", "Values": [vpc_id]},
                {"Name": "service-name", "Values": [service_name]},
            ]
        ).get("VpcEndpoints", [])

        if existing:
            endpoint_ids.append(existing[0]["VpcEndpointId"])
            continue

        created = ec2_client.create_vpc_endpoint(
            VpcId=vpc_id,
            VpcEndpointType="Interface",
            ServiceName=service_name,
            SubnetIds=[subnet_id],
            SecurityGroupIds=[security_group_id],
            PrivateDnsEnabled=True,
        )["VpcEndpoint"]
        endpoint_ids.append(created["VpcEndpointId"])

    deadline = time.time() + wait_timeout_seconds
    last_seen: List[Dict[str, Any]] = []

    while time.time() < deadline:
        details = ec2_client.describe_vpc_endpoints(VpcEndpointIds=endpoint_ids).get(
            "VpcEndpoints", []
        )
        last_seen = details
        states = {item.get("State", "") for item in details}
        if details and states == {"available"}:
            break
        time.sleep(5)

    normalized = [
        {
            "VpcEndpointId": item.get("VpcEndpointId", ""),
            "ServiceName": item.get("ServiceName", ""),
            "State": item.get("State", ""),
        }
        for item in last_seen
    ]
    normalized.sort(key=lambda item: item["ServiceName"])
    return normalized


def wait_for_ssm_online(
    ssm_client: Any,
    instance_id: str,
    timeout_seconds: int = 300,
    poll_seconds: int = 10,
) -> Optional[Dict[str, Any]]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        response = ssm_client.describe_instance_information(
            Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
        )
        info_list = response.get("InstanceInformationList", [])
        if info_list and info_list[0].get("PingStatus") == "Online":
            return info_list[0]
        time.sleep(poll_seconds)
    return None


def get_instance_type_specs(ec2_client: Any, instance_type: str) -> Dict[str, Any]:
    response = ec2_client.describe_instance_types(InstanceTypes=[instance_type])
    details = response.get("InstanceTypes", [])
    if not details:
        raise RuntimeError(f"Instance type not found: {instance_type}")

    item = details[0]
    vcpu = item.get("VCpuInfo", {}).get("DefaultVCpus")
    memory_mib = item.get("MemoryInfo", {}).get("SizeInMiB")
    return {
        "InstanceType": instance_type,
        "vCPU": vcpu,
        "MemoryGiB": round((memory_mib or 0) / 1024.0, 2) if memory_mib else None,
    }


def start_coremark_benchmark(
    ssm_client: Any,
    *,
    instance_id: str,
    linux_binary_path: str,
    duration_seconds: int = 30,
    remote_dir: str = "/tmp/coremark_streamlit",
    chunk_size: int = 1800,
    chunks_per_command: int = 12,
    upload_timeout_seconds: int = 600,
    upload_binary: bool = True,
    cgroup_cpu_cores: Optional[int] = None,
    cgroup_memory_mib: Optional[int] = None,
    cgroup_name_prefix: str = "benchmark-cpu",
) -> str:
    max_duration = max(int(duration_seconds), 1)
    safe_remote_dir = remote_dir.strip() or "/tmp/coremark_streamlit"
    safe_remote_dir_q = _shell_quote_single(safe_remote_dir)
    remote_coremark = f"{safe_remote_dir}/coremark"
    commands = [
        "set -euo pipefail",
        f"REMOTE_DIR={safe_remote_dir_q}",
        'mkdir -p "$REMOTE_DIR"',
    ]
    commands.extend(
        _build_cgroup_v2_setup_commands(
            cgroup_name_prefix=cgroup_name_prefix,
            cgroup_cpu_cores=cgroup_cpu_cores,
            cgroup_memory_mib=cgroup_memory_mib,
        )
    )
    if upload_binary:
        binary_file = Path(linux_binary_path)
        if not binary_file.exists():
            raise FileNotFoundError(f"Linux CoreMark binary not found: {linux_binary_path}")
        if not binary_file.is_file():
            raise RuntimeError(f"Linux CoreMark path is not a file: {linux_binary_path}")

        _send_shell_command_and_wait(
            ssm_client,
            instance_id=instance_id,
            commands=[
                "set -euo pipefail",
                f"REMOTE_DIR={safe_remote_dir_q}",
                'mkdir -p "$REMOTE_DIR"',
                'rm -f "$REMOTE_DIR/coremark.log"',
            ],
            timeout_seconds=min(upload_timeout_seconds, 120),
            comment="Prepare remote CoreMark runtime directory",
        )
        _upload_file_via_ssm(
            ssm_client,
            instance_id=instance_id,
            local_path=str(binary_file),
            remote_path=remote_coremark,
            mode="0755",
            chunk_size=max(int(chunk_size), 1),
            chunks_per_command=max(int(chunks_per_command), 1),
            timeout_seconds=max(int(upload_timeout_seconds), 30),
            comment_prefix="Upload CoreMark binary",
        )
    else:
        commands.extend(
            [
                'if [ ! -x "$REMOTE_DIR/coremark" ]; then',
                '  echo "__COREMARK_ERROR__ missing existing coremark binary";',
                "  exit 127;",
                "fi",
            ]
        )

    commands.extend(
        [
            'echo "__COREMARK_START__"',
            'rm -f "$REMOTE_DIR/coremark.log"',
            'touch "$REMOTE_DIR/coremark.log"',
            'if [ "$CGROUP_ENABLED" -eq 1 ]; then',
            (
                f'  (echo $$ > "$CGROUP_PATH/cgroup.procs"; '
                f'timeout {max_duration}s "$REMOTE_DIR/coremark" 0x0 0x0 0x66 0 '
                '> "$REMOTE_DIR/coremark.log" 2>&1) &'
            ),
            'else',
            (
                f'  (timeout {max_duration}s "$REMOTE_DIR/coremark" 0x0 0x0 0x66 0 '
                '> "$REMOTE_DIR/coremark.log" 2>&1) &'
            ),
            'fi',
            "COREMARK_PID=$!",
            'tail -n +1 -f "$REMOTE_DIR/coremark.log" &',
            "TAIL_PID=$!",
            (
                'while kill -0 "$COREMARK_PID" >/dev/null 2>&1; '
                'do echo "__COREMARK_RUNNING__ $(date -u +%Y-%m-%dT%H:%M:%SZ)"; sleep 2; done &'
            ),
            "HEARTBEAT_PID=$!",
            'wait "$COREMARK_PID" || EXIT_CODE=$?',
            "EXIT_CODE=${EXIT_CODE:-0}",
            'kill "$TAIL_PID" >/dev/null 2>&1 || true',
            'kill "$HEARTBEAT_PID" >/dev/null 2>&1 || true',
            'wait "$TAIL_PID" >/dev/null 2>&1 || true',
            'wait "$HEARTBEAT_PID" >/dev/null 2>&1 || true',
            'if [ "$CGROUP_ENABLED" -eq 1 ]; then rmdir "$CGROUP_PATH" >/dev/null 2>&1 || true; fi',
            'echo "__COREMARK_EXIT_CODE__=$EXIT_CODE"',
            'if [ "$EXIT_CODE" -ne 0 ] && [ "$EXIT_CODE" -ne 124 ]; then exit "$EXIT_CODE"; fi',
            'echo "__COREMARK_DONE__"',
        ]
    )

    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=max_duration + 300,
        Comment=(
            f"CoreMark {max_duration}s benchmark "
            + ("from uploaded Linux binary" if upload_binary else "from existing remote binary")
        ),
    )
    return response["Command"]["CommandId"]


def get_command_invocation(
    ssm_client: Any, *, command_id: str, instance_id: str
) -> Dict[str, Any]:
    try:
        return ssm_client.get_command_invocation(
            CommandId=command_id,
            InstanceId=instance_id,
        )
    except ClientError as error:
        code = error.response.get("Error", {}).get("Code", "")
        if code == "InvocationDoesNotExist":
            return {
                "Status": "Pending",
                "StandardOutputContent": "",
                "StandardErrorContent": "",
            }
        raise


def parse_coremark_result(raw_output: str) -> Dict[str, Any]:
    score_matches = COREMARK_SCORE_PATTERN.findall(raw_output or "")
    iter_matches = COREMARK_ITER_PER_SEC_PATTERN.findall(raw_output or "")
    exit_code_match = COREMARK_EXIT_CODE_PATTERN.search(raw_output or "")

    score = float(score_matches[-1]) if score_matches else None
    iterations_per_sec = float(iter_matches[-1]) if iter_matches else None
    exit_code = int(exit_code_match.group(1)) if exit_code_match else None

    return {
        "coremark_score": score,
        "iterations_per_sec": iterations_per_sec,
        "exit_code": exit_code,
    }


def _to_mib_per_sec(value: float, unit: str) -> Optional[float]:
    normalized = (unit or "").strip().lower()
    if normalized in {"kib/s", "kb/s"}:
        return value / 1024.0
    if normalized in {"mib/s", "mb/s"}:
        return value
    if normalized in {"gib/s", "gb/s"}:
        return value * 1024.0
    if normalized in {"tib/s", "tb/s"}:
        return value * 1024.0 * 1024.0
    return None


def _to_millis(value: float, unit: str) -> Optional[float]:
    normalized = (unit or "").strip().lower()
    if normalized == "nsec":
        return value / (1000.0 * 1000.0)
    if normalized == "usec":
        return value / 1000.0
    if normalized == "msec":
        return value
    if normalized == "sec":
        return value * 1000.0
    return None


def _parse_iops(value: str, suffix: str) -> Optional[float]:
    try:
        base = float(value)
    except (TypeError, ValueError):
        return None
    factor = 1.0
    if (suffix or "").lower() == "k":
        factor = 1000.0
    if (suffix or "").lower() == "m":
        factor = 1000.0 * 1000.0
    return base * factor


def parse_fio_result(raw_output: str) -> Dict[str, Any]:
    bw_matches = FIO_BW_PATTERN.findall(raw_output or "")
    iops_matches = FIO_IOPS_PATTERN.findall(raw_output or "")
    cpu_matches = FIO_CPU_PATTERN.findall(raw_output or "")
    disk_util_matches = FIO_DISK_UTIL_PATTERN.findall(raw_output or "")
    clat_avg_matches = FIO_CLAT_AVG_PATTERN.findall(raw_output or "")
    clat_percentile_sections = FIO_CLAT_PERCENTILES_SECTION_PATTERN.findall(raw_output or "")
    exit_code_match = FIO_EXIT_CODE_PATTERN.search(raw_output or "")

    bw_mib_s = None
    if bw_matches:
        value_raw, unit = bw_matches[-1]
        try:
            bw_mib_s = _to_mib_per_sec(float(value_raw), unit)
        except ValueError:
            bw_mib_s = None

    iops = None
    if iops_matches:
        value_raw, suffix = iops_matches[-1]
        iops = _parse_iops(value_raw, suffix)

    cpu_usr_pct = None
    cpu_sys_pct = None
    cpu_total_pct = None
    if cpu_matches:
        usr_raw, sys_raw = cpu_matches[-1]
        try:
            cpu_usr_pct = float(usr_raw)
        except ValueError:
            cpu_usr_pct = None
        try:
            cpu_sys_pct = float(sys_raw)
        except ValueError:
            cpu_sys_pct = None
        if cpu_usr_pct is not None and cpu_sys_pct is not None:
            cpu_total_pct = cpu_usr_pct + cpu_sys_pct

    disk_util_pct = None
    if disk_util_matches:
        try:
            disk_util_pct = float(disk_util_matches[-1])
        except ValueError:
            disk_util_pct = None

    avg_latency_ms = None
    if clat_avg_matches:
        unit_raw, value_raw = clat_avg_matches[-1]
        try:
            avg_latency_ms = _to_millis(float(value_raw), unit_raw)
        except ValueError:
            avg_latency_ms = None

    p95_latency_ms = None
    p99_latency_ms = None
    if clat_percentile_sections:
        # Some SSM outputs are truncated at the tail. Walk backward and pick the
        # latest percentile block that still contains the target percentiles.
        for unit_raw, section_body in reversed(clat_percentile_sections):
            body_text = section_body or ""
            if p95_latency_ms is None:
                p95_match = FIO_P95_PATTERN.search(body_text)
                if p95_match:
                    try:
                        p95_latency_ms = _to_millis(float(p95_match.group(1)), unit_raw)
                    except ValueError:
                        p95_latency_ms = None
            if p99_latency_ms is None:
                p99_match = FIO_P99_PATTERN.search(body_text)
                if p99_match:
                    try:
                        p99_latency_ms = _to_millis(float(p99_match.group(1)), unit_raw)
                    except ValueError:
                        p99_latency_ms = None
            if p95_latency_ms is not None and p99_latency_ms is not None:
                break

    exit_code = int(exit_code_match.group(1)) if exit_code_match else None
    return {
        "bw_mib_s": bw_mib_s,
        "iops": iops,
        "avg_latency_ms": avg_latency_ms,
        "p95_latency_ms": p95_latency_ms,
        "p99_latency_ms": p99_latency_ms,
        "cpu_usr_pct": cpu_usr_pct,
        "cpu_sys_pct": cpu_sys_pct,
        "cpu_total_pct": cpu_total_pct,
        "disk_util_pct": disk_util_pct,
        "exit_code": exit_code,
    }


def _shell_quote_single(value: str) -> str:
    return "'" + (value or "").replace("'", "'\"'\"'") + "'"




def _build_cgroup_v2_setup_commands(
    *,
    cgroup_name_prefix: str,
    cgroup_cpu_cores: Optional[int],
    cgroup_memory_mib: Optional[int],
) -> List[str]:
    if cgroup_cpu_cores is None and cgroup_memory_mib is None:
        return [
            "CGROUP_ENABLED=0",
            'echo "__CGROUP__ disabled"',
        ]

    commands = [
        "CGROUP_ENABLED=1",
        f"CGROUP_CPU_CORES={int(cgroup_cpu_cores) if cgroup_cpu_cores is not None else 0}",
        f"CGROUP_MEMORY_MIB={int(cgroup_memory_mib) if cgroup_memory_mib is not None else 0}",
        f"CGROUP_NAME_PREFIX={_shell_quote_single(re.sub(r'[^a-zA-Z0-9_-]', '-', cgroup_name_prefix).strip('-') or 'benchmark')}",
        'if [ "$(id -u)" -ne 0 ]; then echo "__CGROUP_ERROR__ root privileges are required for cgroup v2"; exit 126; fi',
        'if [ ! -f /sys/fs/cgroup/cgroup.controllers ]; then echo "__CGROUP_ERROR__ cgroup v2 not detected"; exit 126; fi',
        'if ! grep -qw cpu /sys/fs/cgroup/cgroup.controllers; then echo "__CGROUP_ERROR__ cpu controller unavailable"; exit 126; fi',
        'if ! grep -qw memory /sys/fs/cgroup/cgroup.controllers; then echo "__CGROUP_ERROR__ memory controller unavailable"; exit 126; fi',
        'if ! grep -qw cpu /sys/fs/cgroup/cgroup.subtree_control; then echo "+cpu" > /sys/fs/cgroup/cgroup.subtree_control; fi',
        'if ! grep -qw memory /sys/fs/cgroup/cgroup.subtree_control; then echo "+memory" > /sys/fs/cgroup/cgroup.subtree_control; fi',
        'CGROUP_NAME="${CGROUP_NAME_PREFIX}-$(date +%s)-$$"',
        'CGROUP_PATH="/sys/fs/cgroup/${CGROUP_NAME}"',
        'mkdir -p "$CGROUP_PATH"',
        'if [ "$CGROUP_CPU_CORES" -gt 0 ]; then echo "$((CGROUP_CPU_CORES * 100000)) 100000" > "$CGROUP_PATH/cpu.max"; fi',
        'if [ "$CGROUP_MEMORY_MIB" -gt 0 ]; then echo "$((CGROUP_MEMORY_MIB * 1024 * 1024))" > "$CGROUP_PATH/memory.max"; fi',
        'echo "__CGROUP__ path=$CGROUP_PATH cpu_cores=${CGROUP_CPU_CORES} memory_mib=${CGROUP_MEMORY_MIB}"',
    ]
    return commands


def _wait_command_success(
    ssm_client: Any,
    *,
    instance_id: str,
    command_id: str,
    timeout_seconds: int = 600,
    poll_seconds: int = 2,
) -> Dict[str, Any]:
    deadline = time.time() + max(int(timeout_seconds), 30)
    terminal_statuses = {"Success", "Cancelled", "Failed", "TimedOut", "Cancelling"}

    while time.time() < deadline:
        invocation = get_command_invocation(
            ssm_client,
            command_id=command_id,
            instance_id=instance_id,
        )
        status = invocation.get("Status", "Pending")
        if status not in terminal_statuses:
            time.sleep(max(int(poll_seconds), 1))
            continue

        if status == "Success":
            return invocation

        stdout_tail = (invocation.get("StandardOutputContent", "") or "").strip()[-300:]
        stderr_tail = (invocation.get("StandardErrorContent", "") or "").strip()[-300:]
        tail_text = "\n".join(part for part in [stdout_tail, stderr_tail] if part)
        raise RuntimeError(
            f"SSM command failed with status={status}. "
            f"command_id={command_id}. tail={tail_text or '<empty>'}"
        )

    raise RuntimeError(
        f"SSM command timed out waiting for completion. command_id={command_id}"
    )


def _send_shell_command_and_wait(
    ssm_client: Any,
    *,
    instance_id: str,
    commands: List[str],
    timeout_seconds: int,
    comment: str,
) -> Dict[str, Any]:
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=max(int(timeout_seconds), 30),
        Comment=comment,
    )
    command_id = response["Command"]["CommandId"]
    return _wait_command_success(
        ssm_client,
        instance_id=instance_id,
        command_id=command_id,
        timeout_seconds=timeout_seconds + 60,
    )


def _upload_file_via_ssm(
    ssm_client: Any,
    *,
    instance_id: str,
    local_path: str,
    remote_path: str,
    mode: str = "0644",
    chunk_size: int = 1800,
    chunks_per_command: int = 12,
    timeout_seconds: int = 600,
    comment_prefix: str = "Upload file",
) -> None:
    local_file = Path(local_path)
    if not local_file.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")
    if not local_file.is_file():
        raise RuntimeError(f"Local path is not a file: {local_path}")

    encoded = base64.b64encode(local_file.read_bytes()).decode("ascii")
    if not encoded:
        raise RuntimeError(f"Local file is empty: {local_path}")

    safe_remote_path = _shell_quote_single(remote_path)
    remote_b64 = f"{remote_path}.b64"
    safe_remote_b64 = _shell_quote_single(remote_b64)
    safe_mode = (mode or "0644").strip()
    chunks = [encoded[i : i + chunk_size] for i in range(0, len(encoded), chunk_size)]

    _send_shell_command_and_wait(
        ssm_client,
        instance_id=instance_id,
        commands=[
            "set -euo pipefail",
            f"REMOTE_PATH={safe_remote_path}",
            f"REMOTE_B64={safe_remote_b64}",
            'mkdir -p "$(dirname "$REMOTE_PATH")"',
            'rm -f "$REMOTE_PATH" "$REMOTE_B64"',
        ],
        timeout_seconds=min(timeout_seconds, 120),
        comment=f"{comment_prefix}: init {local_file.name}",
    )

    total_batches = max((len(chunks) + chunks_per_command - 1) // chunks_per_command, 1)
    for index in range(0, len(chunks), chunks_per_command):
        batch = chunks[index : index + chunks_per_command]
        batch_no = index // chunks_per_command + 1
        commands = [
            "set -euo pipefail",
            f"REMOTE_B64={safe_remote_b64}",
        ]
        for chunk in batch:
            commands.append(f'echo "{chunk}" >> "$REMOTE_B64"')
        _send_shell_command_and_wait(
            ssm_client,
            instance_id=instance_id,
            commands=commands,
            timeout_seconds=min(timeout_seconds, 120),
            comment=(
                f"{comment_prefix}: append {local_file.name} "
                f"batch {batch_no}/{total_batches}"
            ),
        )

    _send_shell_command_and_wait(
        ssm_client,
        instance_id=instance_id,
        commands=[
            "set -euo pipefail",
            f"REMOTE_PATH={safe_remote_path}",
            f"REMOTE_B64={safe_remote_b64}",
            'base64 -d "$REMOTE_B64" > "$REMOTE_PATH"',
            f'chmod {safe_mode} "$REMOTE_PATH"',
            'rm -f "$REMOTE_B64"',
        ],
        timeout_seconds=min(timeout_seconds, 120),
        comment=f"{comment_prefix}: finalize {local_file.name}",
    )


def start_fio_benchmark(
    ssm_client: Any,
    *,
    instance_id: str,
    fio_command: str,
    linux_fio_binary_path: str,
    linux_fio_engine_libaio_path: str,
    linux_shared_lib_paths: Optional[List[str]] = None,
    test_name: str,
    cleanup_glob: str,
    remote_dir: str = "/tmp/fio_streamlit",
    upload_bundle: bool = True,
    upload_timeout_seconds: int = 600,
    timeout_seconds: int = 600,
    cgroup_cpu_cores: Optional[int] = None,
    cgroup_memory_mib: Optional[int] = None,
    cgroup_name_prefix: str = "benchmark-fio",
) -> str:
    safe_fio_cmd = _shell_quote_single(fio_command)
    safe_cleanup = cleanup_glob.strip() or "/mnt/fio/*"
    safe_cleanup_glob = _shell_quote_single(safe_cleanup)
    safe_remote_dir = remote_dir.strip() or "/tmp/fio_streamlit"
    safe_remote_dir_q = _shell_quote_single(safe_remote_dir)
    safe_test_name = re.sub(r"[^a-zA-Z0-9_-]", "-", test_name).strip("-") or "fio"
    remote_log = f"/tmp/fio-{safe_test_name}.log"
    remote_fio = f"{safe_remote_dir}/fio"
    remote_engine = f"{safe_remote_dir}/engines/fio-libaio.so"
    remote_lib_dir = f"{safe_remote_dir}/lib"

    lib_paths = [item for item in (linux_shared_lib_paths or []) if item and item.strip()]
    if upload_bundle:
        _send_shell_command_and_wait(
            ssm_client,
            instance_id=instance_id,
            commands=[
                "set -euo pipefail",
                f"REMOTE_DIR={safe_remote_dir_q}",
                'mkdir -p "$REMOTE_DIR/lib" "$REMOTE_DIR/engines"',
            ],
            timeout_seconds=min(upload_timeout_seconds, 120),
            comment="Prepare remote fio runtime directory",
        )
        _upload_file_via_ssm(
            ssm_client,
            instance_id=instance_id,
            local_path=linux_fio_binary_path,
            remote_path=remote_fio,
            mode="0755",
            timeout_seconds=upload_timeout_seconds,
            comment_prefix="Upload fio binary",
        )
        _upload_file_via_ssm(
            ssm_client,
            instance_id=instance_id,
            local_path=linux_fio_engine_libaio_path,
            remote_path=remote_engine,
            mode="0644",
            timeout_seconds=upload_timeout_seconds,
            comment_prefix="Upload fio libaio engine",
        )
        for lib_path in lib_paths:
            remote_lib = f"{remote_lib_dir}/{Path(lib_path).name}"
            _upload_file_via_ssm(
                ssm_client,
                instance_id=instance_id,
                local_path=lib_path,
                remote_path=remote_lib,
                mode="0644",
                timeout_seconds=upload_timeout_seconds,
                comment_prefix="Upload fio shared library",
            )

    commands = [
        "set -euo pipefail",
        f"FIO_CMD={safe_fio_cmd}",
        f'CLEANUP_GLOB={safe_cleanup_glob}',
        f'REMOTE_DIR={safe_remote_dir_q}',
        f'REMOTE_LOG={_shell_quote_single(remote_log)}',
        f'REMOTE_FIO={_shell_quote_single(remote_fio)}',
        f'REMOTE_ENGINE={_shell_quote_single(remote_engine)}',
        f'REMOTE_LIB_DIR={_shell_quote_single(remote_lib_dir)}',
        f'echo "__FIO_COMMAND__=$FIO_CMD"',
        'echo "__FIO_RUNTIME__ fio=$REMOTE_FIO engine=$REMOTE_ENGINE"',
        "mkdir -p /mnt/fio",
        'if [ ! -x "$REMOTE_FIO" ]; then echo "__FIO_ERROR__ missing fio binary"; exit 127; fi',
        'if [ ! -f "$REMOTE_ENGINE" ]; then echo "__FIO_ERROR__ missing fio engine"; exit 127; fi',
        'export PATH="$REMOTE_DIR:$PATH"',
        'export LD_LIBRARY_PATH="$REMOTE_LIB_DIR:${LD_LIBRARY_PATH:-}"',
        'export FIO_PLUGIN_DIR="$REMOTE_DIR/engines"',
        (
            'FIO_CMD_RESOLVED=$(printf \'%s\' "$FIO_CMD" | '
            'sed "s#--ioengine=libaio#--ioengine=$REMOTE_ENGINE#g; '
            's#--ioengine[[:space:]]\\+libaio#--ioengine=$REMOTE_ENGINE#g")'
        ),
        'echo "__FIO_COMMAND_RESOLVED__=$FIO_CMD_RESOLVED"',
    ]
    commands.extend(
        _build_cgroup_v2_setup_commands(
            cgroup_name_prefix=cgroup_name_prefix,
            cgroup_cpu_cores=cgroup_cpu_cores,
            cgroup_memory_mib=cgroup_memory_mib,
        )
    )
    commands.extend([
        "EXIT_CODE=0",
        'touch "$REMOTE_LOG"',
        'if [ "$CGROUP_ENABLED" -eq 1 ]; then',
        '  (echo $$ > "$CGROUP_PATH/cgroup.procs"; eval "$FIO_CMD_RESOLVED" > "$REMOTE_LOG" 2>&1) &',
        'else',
        '  (eval "$FIO_CMD_RESOLVED" > "$REMOTE_LOG" 2>&1) &',
        'fi',
        "FIO_PID=$!",
        'tail -n +1 -f "$REMOTE_LOG" &',
        "TAIL_PID=$!",
        (
            'while kill -0 "$FIO_PID" >/dev/null 2>&1; '
            'do echo "__FIO_RUNNING__ $(date -u +%Y-%m-%dT%H:%M:%SZ)"; sleep 2; done &'
        ),
        "HEARTBEAT_PID=$!",
        'wait "$FIO_PID" || EXIT_CODE=$?',
        "EXIT_CODE=${EXIT_CODE:-0}",
        'kill "$TAIL_PID" >/dev/null 2>&1 || true',
        'kill "$HEARTBEAT_PID" >/dev/null 2>&1 || true',
        'wait "$TAIL_PID" >/dev/null 2>&1 || true',
        'wait "$HEARTBEAT_PID" >/dev/null 2>&1 || true',
        'echo "__FIO_CLEANUP_START__"',
        "rm -f ${CLEANUP_GLOB} || true",
        'rm -f "$REMOTE_LOG" || true',
        'echo "__FIO_CLEANUP_DONE__"',
        'if [ "$CGROUP_ENABLED" -eq 1 ]; then rmdir "$CGROUP_PATH" >/dev/null 2>&1 || true; fi',
        'echo "__FIO_EXIT_CODE__=$EXIT_CODE"',
        'if [ "$EXIT_CODE" -ne 0 ]; then exit "$EXIT_CODE"; fi',
        'echo "__FIO_DONE__"',
    ])
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": commands},
        TimeoutSeconds=max(timeout_seconds, 60),
        Comment=f"{safe_test_name} fio benchmark",
    )
    return response["Command"]["CommandId"]


def terminate_instances(ec2_client: Any, instance_ids: List[str]) -> List[Dict[str, Any]]:
    if not instance_ids:
        raise ValueError("instance_ids cannot be empty.")
    response = ec2_client.terminate_instances(InstanceIds=instance_ids)
    statuses = response.get("TerminatingInstances", [])
    normalized = [
        {
            "InstanceId": item.get("InstanceId", ""),
            "PreviousState": (item.get("PreviousState", {}) or {}).get("Name"),
            "CurrentState": (item.get("CurrentState", {}) or {}).get("Name"),
        }
        for item in statuses
    ]
    normalized.sort(key=lambda item: item["InstanceId"])
    return normalized
