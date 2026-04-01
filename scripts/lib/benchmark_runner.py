from __future__ import annotations

from typing import Any, Dict, List

from ec2_service import get_instance, wait_for_ssm_online

from .benchmark_report import evaluate_coremark_result, evaluate_fio_result
from .bundles import resolve_coremark_bundle_for_arch, resolve_fio_bundle_for_arch
from .ec2_benchmark import (
    probe_remote_coremark_exists,
    probe_remote_fio_exists,
    run_coremark_once,
    run_fio_once,
    run_shell_command_once,
)

FIO_SEQWRITE_COMMAND = (
    "taskset -c 0-15 fio --name=seqwrite "
    "--directory=/mnt/fio "
    "--rw=write "
    "--bs=1M "
    "--ioengine=libaio "
    "--direct=1 "
    "--iodepth=32 "
    "--numjobs=4 "
    "--size=50G "
    "--time_based "
    "--runtime=30 "
    "--status-interval=1 "
    "--group_reporting"
)

FIO_RANDWRITE_COMMAND = (
    "taskset -c 0-15 fio --name=randwrite "
    "--directory=/mnt/fio "
    "--rw=randwrite "
    "--bs=16k "
    "--ioengine=libaio "
    "--direct=1 "
    "--iodepth=64 "
    "--numjobs=8 "
    "--filesize=2560M "
    "--time_based "
    "--runtime=30 "
    "--status-interval=1 "
    "--group_reporting"
)


def _result_to_dict(run_result: Any) -> Dict[str, Any]:
    return {
        "command_id": run_result.command_id,
        "status": run_result.status,
        "poll_error": run_result.poll_error,
        "output": run_result.output,
        "parsed": run_result.parsed,
    }


def _run_cpu(
    ssm_client: Any,
    *,
    instance_id: str,
    arch: str,
    cpu_threads: int,
    upload_binary: bool,
    cgroup_cpu_cores: int | None = None,
    cgroup_memory_mib: int | None = None,
) -> Dict[str, Any]:
    coremark_binary = resolve_coremark_bundle_for_arch(arch)["coremark_binary"]
    duration_seconds = 30
    remote_dir = "/tmp/coremark_streamlit"
    command_text = (
        f"parallel_coremark workers={cpu_threads}, "
        f"per_worker='timeout {duration_seconds}s {remote_dir}/coremark 0x0 0x0 0x66 0'"
    )
    run_result = run_coremark_once(
        ssm_client,
        instance_id=instance_id,
        linux_binary_path=str(coremark_binary),
        command_text=command_text,
        duration_seconds=duration_seconds,
        cpu_threads=cpu_threads,
        upload_binary=upload_binary,
        cgroup_cpu_cores=cgroup_cpu_cores,
        cgroup_memory_mib=cgroup_memory_mib,
        max_polls=240,
        poll_interval_seconds=2,
        marker_prefixes_to_skip=["__COREMARK_RUNNING__"],
        running_marker_prefix="__CPU_RUNNING__",
    )
    return _result_to_dict(run_result)


def _run_fio_case(
    ssm_client: Any,
    *,
    instance_id: str,
    arch: str,
    test_name: str,
    fio_command: str,
    upload_bundle: bool,
    cgroup_cpu_cores: int | None = None,
    cgroup_memory_mib: int | None = None,
) -> Dict[str, Any]:
    fio_bundle = resolve_fio_bundle_for_arch(arch)
    fio_binary = fio_bundle["fio_binary"]
    fio_engine = fio_bundle["fio_engine"]
    libs = fio_bundle["shared_libs"]

    prep = run_shell_command_once(
        ssm_client,
        instance_id=instance_id,
        commands=["set -euo pipefail", "mkdir -p /mnt/fio"],
        command_text="mkdir -p /mnt/fio",
        comment=f"Prepare fio dir for {test_name}",
        timeout_seconds=120,
        max_polls=60,
        poll_interval_seconds=2,
        running_marker_prefix=f"__{test_name.upper()}_PREP__",
    )
    if prep.poll_error or prep.status != "Success":
        return {
            "command_id": prep.command_id,
            "status": prep.status,
            "poll_error": prep.poll_error or f"prepare status={prep.status}",
            "output": prep.output,
            "parsed": {},
        }

    cleanup_glob = f"/mnt/fio/{test_name}*"
    displayed_command = f"{fio_command} ; rm -f {cleanup_glob}"
    run_result = run_fio_once(
        ssm_client,
        instance_id=instance_id,
        test_name=test_name,
        fio_command=fio_command,
        cleanup_glob=cleanup_glob,
        linux_fio_binary_path=str(fio_binary),
        linux_fio_engine_libaio_path=str(fio_engine),
        linux_shared_lib_paths=[str(path) for path in libs],
        command_text=displayed_command,
        upload_bundle=upload_bundle,
        upload_timeout_seconds=1200,
        timeout_seconds=900,
        cgroup_cpu_cores=cgroup_cpu_cores,
        cgroup_memory_mib=cgroup_memory_mib,
        max_polls=360,
        poll_interval_seconds=2,
        running_marker_prefix=f"__{test_name.upper()}_RUNNING__",
    )
    return _result_to_dict(run_result)


def run_benchmark_suite(
    ec2_client: Any,
    ssm_client: Any,
    *,
    instance_id: str,
    test: str = "suite",
    cpu_threads: int = 16,
    cgroup_cpu_cores: int | None = None,
    cgroup_memory_mib: int | None = None,
    wait_ssm_online_enabled: bool = False,
    wait_ssm_timeout_seconds: int = 180,
    wait_ssm_poll_seconds: int = 10,
) -> Dict[str, Any]:
    normalized_test = str(test or "suite").strip().lower()
    if normalized_test not in {"cpu", "seqwrite", "randwrite", "suite"}:
        raise ValueError(f"Unsupported test: {test}")

    instance = get_instance(ec2_client, instance_id)
    arch = str(instance.get("Architecture") or "").strip().lower()
    if not arch:
        raise RuntimeError("Instance architecture is unavailable.")

    if wait_ssm_online_enabled:
        ssm_info = wait_for_ssm_online(
            ssm_client,
            instance_id,
            timeout_seconds=max(int(wait_ssm_timeout_seconds), 30),
            poll_seconds=max(int(wait_ssm_poll_seconds), 1),
        )
        if not ssm_info:
            raise RuntimeError("SSM is not online for instance within timeout.")

    output: Dict[str, Any] = {
        "instance_id": instance_id,
        "instance_type": instance.get("InstanceType"),
        "architecture": arch,
        "test": normalized_test,
        "resource_limits": {
            "cgroup_cpu_cores": cgroup_cpu_cores,
            "cgroup_memory_mib": cgroup_memory_mib,
        },
        "results": {},
        "summary": [],
    }

    remote_fio_ready = probe_remote_fio_exists(ssm_client, instance_id=instance_id)
    remote_coremark_ready = probe_remote_coremark_exists(ssm_client, instance_id=instance_id)

    safe_cpu_threads = max(int(cpu_threads), 1)
    if normalized_test in {"cpu", "suite"}:
        cpu_result = _run_cpu(
            ssm_client,
            instance_id=instance_id,
            arch=arch,
            cpu_threads=safe_cpu_threads,
            upload_binary=not remote_coremark_ready,
            cgroup_cpu_cores=cgroup_cpu_cores,
            cgroup_memory_mib=cgroup_memory_mib,
        )
        output["results"]["cpu"] = cpu_result
        ok, message = evaluate_coremark_result(cpu_result, threads=safe_cpu_threads)
        output["summary"].append({"test": "cpu", "ok": ok, "message": message})

    fio_upload_bundle = not remote_fio_ready
    if normalized_test in {"seqwrite", "suite"}:
        seq_result = _run_fio_case(
            ssm_client,
            instance_id=instance_id,
            arch=arch,
            test_name="seqwrite",
            fio_command=FIO_SEQWRITE_COMMAND,
            upload_bundle=fio_upload_bundle,
            cgroup_cpu_cores=cgroup_cpu_cores,
            cgroup_memory_mib=cgroup_memory_mib,
        )
        fio_upload_bundle = False
        output["results"]["seqwrite"] = seq_result
        ok, message = evaluate_fio_result(seq_result, name="seqwrite")
        output["summary"].append({"test": "seqwrite", "ok": ok, "message": message})

    if normalized_test in {"randwrite", "suite"}:
        rand_result = _run_fio_case(
            ssm_client,
            instance_id=instance_id,
            arch=arch,
            test_name="randwrite",
            fio_command=FIO_RANDWRITE_COMMAND,
            upload_bundle=fio_upload_bundle,
            cgroup_cpu_cores=cgroup_cpu_cores,
            cgroup_memory_mib=cgroup_memory_mib,
        )
        output["results"]["randwrite"] = rand_result
        ok, message = evaluate_fio_result(rand_result, name="randwrite")
        output["summary"].append({"test": "randwrite", "ok": ok, "message": message})

    return output


def summarize_output_ok(output: Dict[str, Any]) -> bool:
    summaries: List[Dict[str, Any]] = output.get("summary") or []
    return all(item.get("ok") for item in summaries)
