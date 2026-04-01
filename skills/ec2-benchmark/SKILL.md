---
name: ec2-benchmark
description: Run reproducible AWS EC2 performance benchmarks (CoreMark + fio seqwrite/randwrite), probe and prepare SSM runtime state, and emit machine-readable benchmark results. Use when an agent needs to benchmark EC2 instances, compare instance types, or troubleshoot CPU/IO performance regressions on AWS EC2.
---

# EC2 Benchmark Skill (Draft)

Use this skill to run EC2 benchmark workflows through reusable Python functions instead of ad-hoc shell commands.

## Required Inputs

- AWS credentials env file path (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, optional token/region)
- AWS region (from env file or explicit override)
- Target `instance_id`
- Test type: `cpu`, `seqwrite`, `randwrite`, or `suite`

## Reusable Function Contracts

Use functions from `/Users/xiaoli/coding/machine_benchmark/scripts/lib/ec2_benchmark.py`:

- `probe_remote_coremark_exists(ssm_client, instance_id=...) -> bool`
- `probe_remote_fio_exists(ssm_client, instance_id=...) -> bool`
- `run_shell_command_once(ssm_client, instance_id=..., commands=[...], command_text=..., comment=...) -> ShellCommandRunResult`
- `run_coremark_once(ssm_client, instance_id=..., linux_binary_path=..., command_text=..., ...) -> BenchmarkRunResult`
- `run_fio_once(ssm_client, instance_id=..., test_name=..., fio_command=..., cleanup_glob=..., linux_fio_binary_path=..., linux_fio_engine_libaio_path=..., linux_shared_lib_paths=[...], command_text=..., ...) -> BenchmarkRunResult`

Use bundle resolvers from `/Users/xiaoli/coding/machine_benchmark/scripts/lib/bundles.py`:

- `resolve_coremark_bundle_for_arch(architecture) -> {architecture, bundle_root, coremark_binary}`
- `resolve_fio_bundle_for_arch(architecture) -> {architecture, bundle_root, fio_binary, fio_engine, shared_libs}`

Use lifecycle wrappers from `/Users/xiaoli/coding/machine_benchmark/scripts/lib/ec2_lifecycle.py`:

- `create_ec2_instance(ec2_client, ami_id=..., instance_type=..., ...) -> instance_dict`
- `terminate_ec2_instances(ec2_client, instance_ids=[...]) -> terminated_status_list`

## Standard Workflow

1. Build boto3 clients from local AWS env file.
2. Load instance metadata, especially architecture (`x86_64` or `arm64`).
3. Probe remote runtime state:
   - If coremark already exists on EC2, skip coremark upload.
   - If fio runtime already exists on EC2, skip fio upload on first fio run.
4. For fio tests, always run `run_shell_command_once(..., commands=["set -euo pipefail", "mkdir -p /mnt/fio"], ...)` before starting fio.
5. Run tests via reusable functions and parse `BenchmarkRunResult.parsed`.
6. Emit structured output JSON with raw status + key metrics.

## Output Contract

Always output JSON with these top-level fields:

- `instance_id`
- `instance_type`
- `architecture`
- `test`
- `results` (raw command status/output/parsed metrics per test)
- `summary` (pass/fail + concise message per test)

## Error Handling Rules

- Treat `poll_error` as failure.
- Treat command `status != "Success"` as failure.
- CPU: fail when both `coremark_score` and `iterations_per_sec` are missing.
- FIO: fail when `bw_mib_s` is missing.
- Keep `command_id` in output for traceability.

## CLI Reference

Use `/Users/xiaoli/coding/machine_benchmark/scripts/cli/run_benchmark.py` for non-UI execution:

```bash
python3 /Users/xiaoli/coding/machine_benchmark/scripts/cli/run_benchmark.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  --instance-id i-xxxxxxxxxxxxxxxxx \
  --test suite \
  --cpu-threads 16 \
  --wait-ssm-online
```

`run_benchmark.py` also supports:

- `--output-file /absolute/path/to/result.json` to save full JSON result.
- `--print-compact-summary` to print a one-line summary after JSON.

Use `/Users/xiaoli/coding/machine_benchmark/scripts/cli/benchmark_job.py` for full job orchestration:

```bash
python3 /Users/xiaoli/coding/machine_benchmark/scripts/cli/benchmark_job.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  --instance-type c6i.xlarge \
  --architecture x86_64 \
  --ensure-ssm-profile \
  --test suite \
  --cpu-threads 4 \
  --output-file /absolute/path/to/c6i_suite.json \
  --print-compact-summary \
  --terminate-policy on-success
```

If `--instance-id` is provided, `benchmark_job.py` runs on that existing instance instead of creating a new one.

Use `/Users/xiaoli/coding/machine_benchmark/scripts/cli/ec2_lifecycle.py` for create/delete:

```bash
python3 /Users/xiaoli/coding/machine_benchmark/scripts/cli/ec2_lifecycle.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  create \
  --ami-id ami-xxxxxxxx \
  --instance-type c7i.large \
  --name-tag bench-node

python3 /Users/xiaoli/coding/machine_benchmark/scripts/cli/ec2_lifecycle.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  delete \
  --instance-ids i-xxxxxxxxxxxxxxxxx
```

## Notes for Agent Implementations

- Keep app/UI layer thin: orchestration, input validation, rendering, persistence.
- Keep execution logic in `scripts/lib` functions so app and CLI share behavior.
- Prefer deterministic return structures over free-form stdout parsing in upper layers.
- Prefer `benchmark_job.py` for end-to-end automation (create/wait/run/summarize/cleanup).
- Reuse `scripts/lib/ec2_orchestrator.py`, `scripts/lib/benchmark_runner.py`,
  and `scripts/lib/benchmark_report.py` instead of ad-hoc inline Python blocks.
