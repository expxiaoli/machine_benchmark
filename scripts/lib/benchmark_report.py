from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


def detect_coremark_output_issue(raw_output: str, *, thread_count: int) -> Optional[str]:
    output = raw_output or ""
    if "Errors detected" in output or "Must execute for at least 10 secs" in output:
        return "CoreMark output invalid: errors detected or runtime below 10 seconds"
    if thread_count > 1 and "awk:" in output and "syntax error" in output.lower():
        return "CoreMark output invalid: awk aggregation failed in multi-thread mode"
    return None


def evaluate_coremark_result(result: Dict[str, Any], *, threads: int) -> Tuple[bool, str]:
    if result.get("poll_error"):
        return False, f"poll failed: {result['poll_error']}"
    if result.get("status") != "Success":
        return False, f"status={result.get('status')}"

    parsed = result.get("parsed") or {}
    score = parsed.get("coremark_score")
    ips = parsed.get("iterations_per_sec")
    issue = detect_coremark_output_issue(result.get("output", ""), thread_count=threads)
    if issue:
        return False, issue
    if score is None and ips is None:
        return False, "missing coremark metrics"

    return True, f"coremark={score}, iterations_per_sec={ips}, exit_code={parsed.get('exit_code')}"


def evaluate_fio_result(result: Dict[str, Any], *, name: str) -> Tuple[bool, str]:
    if result.get("poll_error"):
        return False, f"{name} poll failed: {result['poll_error']}"
    if result.get("status") != "Success":
        return False, f"{name} status={result.get('status')}"
    parsed = result.get("parsed") or {}
    bw = parsed.get("bw_mib_s")
    if bw is None:
        return False, f"{name} missing bandwidth metric"
    return True, f"{name} bw_mib_s={bw}, iops={parsed.get('iops')}, exit_code={parsed.get('exit_code')}"


def extract_summary_metrics(output: Dict[str, Any]) -> Dict[str, Any]:
    results = output.get("results") or {}
    cpu = (results.get("cpu") or {}).get("parsed") or {}
    seqwrite = (results.get("seqwrite") or {}).get("parsed") or {}
    randwrite = (results.get("randwrite") or {}).get("parsed") or {}

    return {
        "instance_id": output.get("instance_id"),
        "instance_type": output.get("instance_type"),
        "architecture": output.get("architecture"),
        "test": output.get("test"),
        "cpu_coremark": cpu.get("coremark_score"),
        "cpu_iterations_per_sec": cpu.get("iterations_per_sec"),
        "seqwrite_bw_mib_s": seqwrite.get("bw_mib_s"),
        "seqwrite_iops": seqwrite.get("iops"),
        "randwrite_bw_mib_s": randwrite.get("bw_mib_s"),
        "randwrite_iops": randwrite.get("iops"),
        "all_ok": all(item.get("ok") for item in output.get("summary", [])),
    }


def format_compact_summary_line(output: Dict[str, Any]) -> str:
    metrics = extract_summary_metrics(output)
    return (
        f"instance_id={metrics['instance_id']} "
        f"instance_type={metrics['instance_type']} "
        f"test={metrics['test']} "
        f"cpu_coremark={metrics['cpu_coremark']} "
        f"seqwrite_bw_mib_s={metrics['seqwrite_bw_mib_s']} "
        f"randwrite_bw_mib_s={metrics['randwrite_bw_mib_s']} "
        f"randwrite_iops={metrics['randwrite_iops']} "
        f"all_ok={metrics['all_ok']}"
    )


def build_markdown_report(output: Dict[str, Any]) -> str:
    metrics = extract_summary_metrics(output)
    summary_items: List[str] = []
    for item in output.get("summary", []):
        status = "PASS" if item.get("ok") else "FAIL"
        summary_items.append(f"- {item.get('test')}: {status} ({item.get('message')})")

    lines = [
        f"# EC2 Benchmark Report ({metrics.get('instance_type')})",
        "",
        f"- Instance ID: `{metrics.get('instance_id')}`",
        f"- Instance Type: `{metrics.get('instance_type')}`",
        f"- Architecture: `{metrics.get('architecture')}`",
        f"- Test: `{metrics.get('test')}`",
        "",
        "## Key Metrics",
        f"- CPU CoreMark: `{metrics.get('cpu_coremark')}`",
        f"- SeqWrite BW (MiB/s): `{metrics.get('seqwrite_bw_mib_s')}`",
        f"- SeqWrite IOPS: `{metrics.get('seqwrite_iops')}`",
        f"- RandWrite BW (MiB/s): `{metrics.get('randwrite_bw_mib_s')}`",
        f"- RandWrite IOPS: `{metrics.get('randwrite_iops')}`",
        "",
        "## Summary",
        *summary_items,
    ]
    return "\n".join(lines).rstrip() + "\n"
