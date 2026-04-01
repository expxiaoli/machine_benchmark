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

from aws_env import build_boto3_session, load_aws_env_config
from scripts.lib import create_ec2_instance, terminate_ec2_instances


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Create/Delete EC2 via scripts/lib wrappers")
    parser.add_argument("--env-file", required=True, help="Path to AWS env file")
    parser.add_argument("--region", default="", help="AWS region override")

    subparsers = parser.add_subparsers(dest="command", required=True)

    create_cmd = subparsers.add_parser("create", help="Create EC2 instance")
    create_cmd.add_argument("--ami-id", required=True)
    create_cmd.add_argument("--instance-type", required=True)
    create_cmd.add_argument("--name-tag", default="ec2-benchmark")
    create_cmd.add_argument("--no-public-ip", action="store_true")
    create_cmd.add_argument("--subnet-id", default="")
    create_cmd.add_argument("--security-group-ids", default="", help="Comma-separated SG IDs")
    create_cmd.add_argument("--key-name", default="")
    create_cmd.add_argument("--iam-instance-profile-name", default="")
    create_cmd.add_argument("--no-wait", action="store_true")

    delete_cmd = subparsers.add_parser("delete", help="Terminate EC2 instance(s)")
    delete_cmd.add_argument(
        "--instance-ids",
        required=True,
        help="Comma-separated instance IDs, e.g. i-1,i-2",
    )

    return parser


def _parse_csv(raw: str) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _build_clients(env_file: str, region_override: str) -> Dict[str, Any]:
    config = load_aws_env_config(env_file)
    region = (region_override or config.region or "").strip()
    if not region:
        raise RuntimeError("Region is required. Set AWS_REGION in env file or pass --region.")
    session = build_boto3_session(config, region_override=region)
    return {
        "region": region,
        "ec2": session.client("ec2", region_name=region),
    }


def main() -> int:
    args = _build_parser().parse_args()
    clients = _build_clients(args.env_file, args.region)
    ec2_client = clients["ec2"]

    if args.command == "create":
        created = create_ec2_instance(
            ec2_client=ec2_client,
            ami_id=args.ami_id,
            instance_type=args.instance_type,
            name_tag=args.name_tag,
            no_public_ip=bool(args.no_public_ip),
            subnet_id=args.subnet_id.strip() or None,
            security_group_ids=_parse_csv(args.security_group_ids),
            key_name=args.key_name.strip() or None,
            iam_instance_profile_name=args.iam_instance_profile_name.strip() or None,
            wait_until_running=not bool(args.no_wait),
        )
        print(json.dumps({"region": clients["region"], "instance": created}, ensure_ascii=False, indent=2))
        return 0

    if args.command == "delete":
        results = terminate_ec2_instances(ec2_client, _parse_csv(args.instance_ids))
        print(json.dumps({"region": clients["region"], "terminated": results}, ensure_ascii=False, indent=2))
        return 0

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # pylint: disable=broad-except
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise
