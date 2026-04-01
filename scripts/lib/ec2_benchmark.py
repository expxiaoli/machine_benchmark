from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Sequence

from botocore.exceptions import ClientError

from ec2_service import (
    get_command_invocation,
    parse_coremark_result,
    parse_fio_result,
    start_coremark_benchmark,
    start_fio_benchmark,
)


@dataclass
class PollCommandResult:
    status: str
    poll_error: Optional[str]
    combined_output: str
    live_output_lines: List[str]


@dataclass
class BenchmarkRunResult:
    command_id: Optional[str]
    status: str
    poll_error: Optional[str]
    output: str
    parsed: Dict[str, Any]


@dataclass
class ShellCommandRunResult:
    command_id: str
    status: str
    poll_error: Optional[str]
    output: str


def _utc_iso8601_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def poll_command_with_live_output(
    ssm_client: Any,
    *,
    instance_id: str,
    command_id: str,
    command_text: str,
    marker_prefixes_to_skip: Optional[Sequence[str]] = None,
    max_polls: int = 240,
    poll_interval_seconds: int = 2,
    running_marker_prefix: str = "__RUNNING__",
    live_output_lines: Optional[List[str]] = None,
    on_update: Optional[Callable[[str, List[str]], None]] = None,
) -> PollCommandResult:
    terminal_status = {"Success", "Failed", "Cancelled", "TimedOut", "Cancelling"}
    marker_prefixes = tuple(marker_prefixes_to_skip or ())

    combined_output = ""
    last_remote_output = ""
    lines = live_output_lines if live_output_lines is not None else []
    if lines:
        lines.append("")
    lines.append(f"$ {command_text}")

    status = "Pending"
    poll_error: Optional[str] = None
    poll_start_time = time.time()

    def _append_live_output(new_text: str) -> None:
        if not new_text:
            return
        filtered_lines: List[str] = []
        for line in new_text.splitlines():
            if any(line.startswith(prefix) for prefix in marker_prefixes):
                continue
            filtered_lines.append(line)
        chunk = "\n".join(filtered_lines).strip()
        if not chunk:
            return
        lines.extend(chunk.splitlines())

    if on_update:
        on_update(status, lines)

    for _ in range(max_polls):
        try:
            invocation = get_command_invocation(
                ssm_client,
                command_id=command_id,
                instance_id=instance_id,
            )
            status = str(invocation.get("Status", "Pending"))
            stdout = str(invocation.get("StandardOutputContent", "") or "")
            stderr = str(invocation.get("StandardErrorContent", "") or "")
            combined_output = stdout + (f"\n{stderr}" if stderr else "")

            if combined_output != last_remote_output:
                if combined_output.startswith(last_remote_output):
                    delta = combined_output[len(last_remote_output) :]
                else:
                    delta = combined_output
                _append_live_output(delta)
                last_remote_output = combined_output

            if status not in terminal_status:
                elapsed_seconds = int(time.time() - poll_start_time)
                lines.append(
                    f"{running_marker_prefix} {_utc_iso8601_now()} elapsed={elapsed_seconds}s"
                )

            if on_update:
                on_update(status, lines)

            if status in terminal_status:
                break
            time.sleep(poll_interval_seconds)
        except ClientError as error:
            poll_error = str(error)
            break

    return PollCommandResult(
        status=status,
        poll_error=poll_error,
        combined_output=combined_output,
        live_output_lines=lines,
    )


def run_coremark_once(
    ssm_client: Any,
    *,
    instance_id: str,
    linux_binary_path: str,
    command_text: str,
    duration_seconds: int = 30,
    cpu_threads: int = 16,
    upload_binary: bool = True,
    cgroup_cpu_cores: Optional[int] = None,
    cgroup_memory_mib: Optional[int] = None,
    marker_prefixes_to_skip: Optional[Sequence[str]] = ("__COREMARK_RUNNING__",),
    max_polls: int = 240,
    poll_interval_seconds: int = 2,
    running_marker_prefix: str = "__CPU_RUNNING__",
    live_output_lines: Optional[List[str]] = None,
    on_update: Optional[Callable[[str, List[str]], None]] = None,
) -> BenchmarkRunResult:
    command_id = start_coremark_benchmark(
        ssm_client,
        instance_id=instance_id,
        linux_binary_path=linux_binary_path,
        duration_seconds=duration_seconds,
        cpu_threads=max(int(cpu_threads), 1),
        upload_binary=upload_binary,
        cgroup_cpu_cores=cgroup_cpu_cores,
        cgroup_memory_mib=cgroup_memory_mib,
        cgroup_name_prefix="benchmark-cpu",
    )
    poll = poll_command_with_live_output(
        ssm_client,
        instance_id=instance_id,
        command_id=command_id,
        command_text=command_text,
        marker_prefixes_to_skip=marker_prefixes_to_skip,
        max_polls=max_polls,
        poll_interval_seconds=poll_interval_seconds,
        running_marker_prefix=running_marker_prefix,
        live_output_lines=live_output_lines,
        on_update=on_update,
    )

    return BenchmarkRunResult(
        command_id=command_id,
        status=poll.status,
        poll_error=poll.poll_error,
        output=poll.combined_output,
        parsed=parse_coremark_result(poll.combined_output),
    )


def run_fio_once(
    ssm_client: Any,
    *,
    instance_id: str,
    test_name: str,
    fio_command: str,
    cleanup_glob: str,
    linux_fio_binary_path: str,
    linux_fio_engine_libaio_path: str,
    linux_shared_lib_paths: Sequence[str],
    command_text: str,
    upload_bundle: bool = True,
    upload_timeout_seconds: int = 1200,
    timeout_seconds: int = 900,
    cgroup_cpu_cores: Optional[int] = None,
    cgroup_memory_mib: Optional[int] = None,
    marker_prefixes_to_skip: Optional[Sequence[str]] = None,
    max_polls: int = 360,
    poll_interval_seconds: int = 2,
    running_marker_prefix: str = "__FIO_RUNNING__",
    live_output_lines: Optional[List[str]] = None,
    on_update: Optional[Callable[[str, List[str]], None]] = None,
) -> BenchmarkRunResult:
    command_id = start_fio_benchmark(
        ssm_client,
        instance_id=instance_id,
        fio_command=fio_command,
        linux_fio_binary_path=linux_fio_binary_path,
        linux_fio_engine_libaio_path=linux_fio_engine_libaio_path,
        linux_shared_lib_paths=[path for path in linux_shared_lib_paths],
        test_name=test_name,
        cleanup_glob=cleanup_glob,
        upload_bundle=upload_bundle,
        upload_timeout_seconds=upload_timeout_seconds,
        timeout_seconds=timeout_seconds,
        cgroup_cpu_cores=cgroup_cpu_cores,
        cgroup_memory_mib=cgroup_memory_mib,
        cgroup_name_prefix=f"benchmark-{test_name}",
    )

    poll = poll_command_with_live_output(
        ssm_client,
        instance_id=instance_id,
        command_id=command_id,
        command_text=command_text,
        marker_prefixes_to_skip=marker_prefixes_to_skip,
        max_polls=max_polls,
        poll_interval_seconds=poll_interval_seconds,
        running_marker_prefix=running_marker_prefix,
        live_output_lines=live_output_lines,
        on_update=on_update,
    )

    return BenchmarkRunResult(
        command_id=command_id,
        status=poll.status,
        poll_error=poll.poll_error,
        output=poll.combined_output,
        parsed=parse_fio_result(poll.combined_output),
    )


def run_shell_command_once(
    ssm_client: Any,
    *,
    instance_id: str,
    commands: Sequence[str],
    command_text: str,
    comment: str,
    timeout_seconds: int = 120,
    max_polls: int = 60,
    poll_interval_seconds: int = 2,
    running_marker_prefix: str = "__SHELL_RUNNING__",
    live_output_lines: Optional[List[str]] = None,
    on_update: Optional[Callable[[str, List[str]], None]] = None,
) -> ShellCommandRunResult:
    response = ssm_client.send_command(
        InstanceIds=[instance_id],
        DocumentName="AWS-RunShellScript",
        Parameters={"commands": [str(item) for item in commands]},
        TimeoutSeconds=max(int(timeout_seconds), 30),
        Comment=comment,
    )
    command_id = response["Command"]["CommandId"]
    poll = poll_command_with_live_output(
        ssm_client,
        instance_id=instance_id,
        command_id=command_id,
        command_text=command_text,
        max_polls=max_polls,
        poll_interval_seconds=poll_interval_seconds,
        running_marker_prefix=running_marker_prefix,
        live_output_lines=live_output_lines,
        on_update=on_update,
    )
    return ShellCommandRunResult(
        command_id=command_id,
        status=poll.status,
        poll_error=poll.poll_error,
        output=poll.combined_output,
    )


def probe_remote_coremark_exists(ssm_client: Any, *, instance_id: str) -> bool:
    result = run_shell_command_once(
        ssm_client,
        instance_id=instance_id,
        commands=[
            "set -euo pipefail",
            'if [ -x "/tmp/coremark_streamlit/coremark" ]; then echo "__EXISTS__=1"; else echo "__EXISTS__=0"; fi',
        ],
        command_text='test -x "/tmp/coremark_streamlit/coremark"',
        comment="Probe existing coremark binary",
        timeout_seconds=90,
        max_polls=45,
        poll_interval_seconds=2,
        running_marker_prefix="__PROBE_COREMARK__",
    )
    return result.status == "Success" and "__EXISTS__=1" in result.output


def probe_remote_fio_exists(ssm_client: Any, *, instance_id: str) -> bool:
    result = run_shell_command_once(
        ssm_client,
        instance_id=instance_id,
        commands=[
            "set -euo pipefail",
            (
                'if [ -x "/tmp/fio_streamlit/fio" ] '
                '&& [ -f "/tmp/fio_streamlit/engines/fio-libaio.so" ] '
                '&& [ -f "/tmp/fio_streamlit/lib/libaio.so.1" ] '
                '&& [ -f "/tmp/fio_streamlit/lib/libnuma.so.1" ]; '
                'then echo "__EXISTS__=1"; else echo "__EXISTS__=0"; fi'
            ),
        ],
        command_text="probe fio runtime bundle",
        comment="Probe existing fio runtime",
        timeout_seconds=90,
        max_polls=45,
        poll_interval_seconds=2,
        running_marker_prefix="__PROBE_FIO__",
    )
    return result.status == "Success" and "__EXISTS__=1" in result.output
