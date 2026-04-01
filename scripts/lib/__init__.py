"""Reusable EC2 benchmark library modules."""

from .bundles import (
    COREMARK_LINUX_BUNDLE_DIR,
    FIO_LINUX_BUNDLE_DIR,
    resolve_coremark_bundle_for_arch,
    resolve_fio_bundle_for_arch,
)
from .ec2_lifecycle import create_ec2_instance, terminate_ec2_instances
from .ec2_benchmark import (
    BenchmarkRunResult,
    PollCommandResult,
    ShellCommandRunResult,
    probe_remote_coremark_exists,
    probe_remote_fio_exists,
    run_shell_command_once,
    run_coremark_once,
    run_fio_once,
)
from .ec2_orchestrator import (
    AwsClients,
    build_aws_clients,
    create_benchmark_instance,
    ensure_instance_ssm_profile,
    ensure_ssm_online_or_raise,
)
from .benchmark_runner import run_benchmark_suite, summarize_output_ok
from .benchmark_report import (
    build_markdown_report,
    detect_coremark_output_issue,
    evaluate_coremark_result,
    evaluate_fio_result,
    extract_summary_metrics,
    format_compact_summary_line,
)

__all__ = [
    "COREMARK_LINUX_BUNDLE_DIR",
    "FIO_LINUX_BUNDLE_DIR",
    "resolve_coremark_bundle_for_arch",
    "resolve_fio_bundle_for_arch",
    "create_ec2_instance",
    "terminate_ec2_instances",
    "BenchmarkRunResult",
    "PollCommandResult",
    "ShellCommandRunResult",
    "probe_remote_coremark_exists",
    "probe_remote_fio_exists",
    "run_shell_command_once",
    "run_coremark_once",
    "run_fio_once",
    "AwsClients",
    "build_aws_clients",
    "create_benchmark_instance",
    "ensure_instance_ssm_profile",
    "ensure_ssm_online_or_raise",
    "run_benchmark_suite",
    "summarize_output_ok",
    "build_markdown_report",
    "detect_coremark_output_issue",
    "evaluate_coremark_result",
    "evaluate_fio_result",
    "extract_summary_metrics",
    "format_compact_summary_line",
]
