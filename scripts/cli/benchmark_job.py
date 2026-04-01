#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib import (
    build_aws_clients,
    create_benchmark_instance,
    ensure_instance_ssm_profile,
    ensure_ssm_online_or_raise,
    format_compact_summary_line,
    run_benchmark_suite,
    summarize_output_ok,
    terminate_ec2_instances,
)


def _parse_csv(raw: str) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "One-shot benchmark job: prepare instance (optional create), wait SSM, "
            "run benchmark, emit JSON summary, and optional terminate."
        )
    )
    parser.add_argument("--env-file", required=True, help="Path to AWS env file")
    parser.add_argument("--region", default="", help="AWS region override")

    target = parser.add_argument_group("Target")
    target.add_argument("--instance-id", default="", help="Run on existing instance ID")
    target.add_argument("--instance-type", default="c6i.xlarge", help="Instance type for auto-create")
    target.add_argument(
        "--architecture",
        default="x86_64",
        choices=["x86_64", "arm64"],
        help="Architecture used when auto-selecting AL2023 AMI",
    )
    target.add_argument("--name-tag", default="ec2-benchmark-job", help="Name tag for auto-created instance")
    target.add_argument("--ami-id", default="", help="Custom AMI ID; empty means AL2023 by architecture")
    target.add_argument("--no-public-ip", action="store_true", help="Create private-only instance")
    target.add_argument("--subnet-id", default="", help="Subnet ID for private/public launch")
    target.add_argument(
        "--security-group-ids",
        default="",
        help="Comma-separated SG IDs for private/public launch",
    )
    target.add_argument("--key-name", default="", help="EC2 key pair name for auto-create")

    runtime = parser.add_argument_group("Runtime")
    runtime.add_argument(
        "--ensure-ssm-profile",
        action="store_true",
        help="Ensure instance has an SSM instance profile attached",
    )
    runtime.add_argument(
        "--iam-instance-profile-name",
        default="",
        help="Use this IAM instance profile name when attaching/creating instance",
    )
    runtime.add_argument(
        "--test",
        default="suite",
        choices=["cpu", "seqwrite", "randwrite", "suite"],
        help="Benchmark test type",
    )
    runtime.add_argument("--cpu-threads", type=int, default=16, help="CoreMark worker threads")
    runtime.add_argument(
        "--cgroup-cpu-cores",
        type=int,
        default=0,
        help="Limit benchmark process group to this many CPU cores via cgroup v2",
    )
    runtime.add_argument(
        "--cgroup-memory-mib",
        type=int,
        default=0,
        help="Limit benchmark process group memory in MiB via cgroup v2",
    )
    runtime.add_argument(
        "--wait-ssm-timeout-seconds",
        type=int,
        default=420,
        help="Timeout waiting for SSM Online",
    )
    runtime.add_argument(
        "--wait-ssm-poll-seconds",
        type=int,
        default=10,
        help="Polling interval for SSM Online checks",
    )

    output = parser.add_argument_group("Output")
    output.add_argument(
        "--output-file",
        default="",
        help="Write full JSON result to file path",
    )
    output.add_argument(
        "--print-compact-summary",
        action="store_true",
        help="Print one-line compact summary after JSON",
    )

    cleanup = parser.add_argument_group("Cleanup")
    cleanup.add_argument(
        "--terminate-policy",
        default="never",
        choices=["never", "on-success", "always"],
        help="Terminate target instance after benchmark by policy",
    )

    return parser


def _save_json_if_needed(output_file: str, payload: Dict[str, Any]) -> None:
    path = Path(output_file).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = _build_parser().parse_args()

    clients = build_aws_clients(env_file=args.env_file, region_override=args.region)

    instance_id = args.instance_id.strip()
    created_now = False
    created_metadata: Dict[str, Any] = {}

    if instance_id:
        if args.ensure_ssm_profile:
            ensure_instance_ssm_profile(
                clients.ec2,
                clients.iam,
                instance_id=instance_id,
                iam_instance_profile_name=args.iam_instance_profile_name,
            )
    else:
        created = create_benchmark_instance(
            clients.ec2,
            clients.ssm,
            clients.iam,
            instance_type=args.instance_type,
            architecture=args.architecture,
            name_tag=args.name_tag,
            ami_id=args.ami_id,
            no_public_ip=bool(args.no_public_ip),
            subnet_id=args.subnet_id,
            security_group_ids=_parse_csv(args.security_group_ids),
            key_name=args.key_name,
            iam_instance_profile_name=args.iam_instance_profile_name,
            ensure_ssm_profile=bool(args.ensure_ssm_profile),
        )
        instance_id = created["instance"]["InstanceId"]
        created_now = True
        created_metadata = created

    ensure_ssm_online_or_raise(
        clients.ssm,
        instance_id=instance_id,
        timeout_seconds=args.wait_ssm_timeout_seconds,
        poll_seconds=args.wait_ssm_poll_seconds,
    )

    benchmark_output = run_benchmark_suite(
        clients.ec2,
        clients.ssm,
        instance_id=instance_id,
        test=args.test,
        cpu_threads=max(int(args.cpu_threads), 1),
        cgroup_cpu_cores=max(int(args.cgroup_cpu_cores), 1) if int(args.cgroup_cpu_cores) > 0 else None,
        cgroup_memory_mib=max(int(args.cgroup_memory_mib), 1) if int(args.cgroup_memory_mib) > 0 else None,
        wait_ssm_online_enabled=False,
    )
    all_ok = summarize_output_ok(benchmark_output)

    payload: Dict[str, Any] = {
        "region": clients.region,
        "instance_id": instance_id,
        "created_now": created_now,
        "created_metadata": created_metadata,
        "terminate_policy": args.terminate_policy,
        "benchmark": benchmark_output,
    }

    should_terminate = (
        args.terminate_policy == "always"
        or (args.terminate_policy == "on-success" and all_ok)
    )
    if should_terminate:
        payload["termination"] = terminate_ec2_instances(clients.ec2, [instance_id])

    if args.output_file.strip():
        _save_json_if_needed(args.output_file, payload)

    print(json.dumps(payload, ensure_ascii=False, indent=2))
    if args.print_compact_summary:
        print(format_compact_summary_line(benchmark_output))

    return 0 if all_ok else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # pylint: disable=broad-except
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise
