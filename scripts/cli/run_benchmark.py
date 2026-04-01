#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.lib import (
    build_aws_clients,
    format_compact_summary_line,
    run_benchmark_suite,
    summarize_output_ok,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run EC2 benchmark via reusable scripts/lib functions")
    parser.add_argument("--env-file", required=True, help="Path to AWS env file")
    parser.add_argument("--region", default="", help="AWS region override")
    parser.add_argument("--instance-id", required=True, help="Target EC2 instance ID")
    parser.add_argument(
        "--test",
        default="suite",
        choices=["cpu", "seqwrite", "randwrite", "suite"],
        help="Benchmark test type",
    )
    parser.add_argument("--cpu-threads", type=int, default=16, help="CoreMark worker threads")
    parser.add_argument(
        "--wait-ssm-online",
        action="store_true",
        help="Wait until SSM ping status is Online before running tests",
    )
    parser.add_argument(
        "--cgroup-cpu-cores",
        type=int,
        default=0,
        help="Limit benchmark process group to this many CPU cores via cgroup v2",
    )
    parser.add_argument(
        "--cgroup-memory-mib",
        type=int,
        default=0,
        help="Limit benchmark process group memory in MiB via cgroup v2",
    )
    parser.add_argument(
        "--wait-ssm-timeout-seconds",
        type=int,
        default=180,
        help="Timeout for --wait-ssm-online",
    )
    parser.add_argument(
        "--wait-ssm-poll-seconds",
        type=int,
        default=10,
        help="Polling interval for --wait-ssm-online",
    )
    parser.add_argument(
        "--output-file",
        default="",
        help="Write full benchmark JSON to file",
    )
    parser.add_argument(
        "--print-compact-summary",
        action="store_true",
        help="Print one-line compact summary after JSON",
    )
    return parser


def _save_json_if_needed(output_file: str, payload: dict) -> None:
    path = Path(output_file).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> int:
    args = _build_parser().parse_args()

    clients = build_aws_clients(env_file=args.env_file, region_override=args.region)
    output = run_benchmark_suite(
        clients.ec2,
        clients.ssm,
        instance_id=args.instance_id,
        test=args.test,
        cpu_threads=max(int(args.cpu_threads), 1),
        cgroup_cpu_cores=max(int(args.cgroup_cpu_cores), 1) if int(args.cgroup_cpu_cores) > 0 else None,
        cgroup_memory_mib=max(int(args.cgroup_memory_mib), 1) if int(args.cgroup_memory_mib) > 0 else None,
        wait_ssm_online_enabled=bool(args.wait_ssm_online),
        wait_ssm_timeout_seconds=args.wait_ssm_timeout_seconds,
        wait_ssm_poll_seconds=args.wait_ssm_poll_seconds,
    )

    if args.output_file.strip():
        _save_json_if_needed(args.output_file, output)

    print(json.dumps(output, ensure_ascii=False, indent=2))
    if args.print_compact_summary:
        print(format_compact_summary_line(output))

    return 0 if summarize_output_ok(output) else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # pylint: disable=broad-except
        print(json.dumps({"error": str(error)}, ensure_ascii=False), file=sys.stderr)
        raise
