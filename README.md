# EC2 Benchmark

Do benchmark test for AWS EC2 instances from a local AWS env file with a web UI.

## Features

- Sidebar navigation links: `EC2 List`, `Create EC2`, `Settings`, `Test Results`
- Load AWS credentials from a local env file path and cache them after `Confirm`
- List EC2 instances
- Search instances by `Instance ID` or `Name` from a single search box
- Create EC2 instances with parameterized CPU/memory/image
- Create EC2 with dynamic architecture and CPU family/generation selection loaded from AWS API
- Open instance detail page from clickable Instance ID in list results
- Show instance detail info and login commands in detail page
- Run `CPU/IO Test` (CoreMark + SeqWrite + RandWrite) from instance detail page
- Upload local Linux benchmark binaries (`coremark`, `fio`) to EC2 via SSM before execution
- Persist performance test history to local SQLite and view in `Test Results`
- Delete (terminate) EC2 instance from detail page
- Optional private-access setup for no-public-IP instances:
  - IAM role/profile for SSM
  - SSM VPC interface endpoints

## Project Files

- `app.py`: Streamlit UI
- `aws_env.py`: Local AWS env file parser and session builder
- `ec2_service.py`: EC2/SSM/IAM operations
- `requirements.txt`: Python dependencies
- `bin/coremark/x86_64/coremark`: Linux CoreMark executable for x86_64 instances
- `bin/coremark/arm64/coremark`: Linux CoreMark executable for arm64 instances
- `bin/fio/x86_64/*`: Linux fio runtime bundle for x86_64 instances
- `bin/fio/arm64/*`: Linux fio runtime bundle for arm64 instances
- `test_results.db`: Local SQLite database for performance test history

## Setup

### macOS / Linux

```bash
cd /path/to/machine_benchmark
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
streamlit run app.py
```

### Windows (PowerShell)

```powershell
cd C:\path\to\machine_benchmark
py -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
streamlit run app.py
```

## Env File Format

Use a file that contains:

```bash
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
AWS_SESSION_TOKEN=...   # optional but common for STS
AWS_REGION=us-west-2    # optional if entered in UI
```

`export KEY=VALUE` format is also supported.

## CLI Quick Start

Use these command templates for repeatable non-UI benchmark runs.

### 1) One-shot benchmark job (create + run + optional auto-terminate)

```bash
python3 scripts/cli/benchmark_job.py \
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

### 2) Run benchmark on an existing instance

```bash
python3 scripts/cli/run_benchmark.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  --instance-id i-xxxxxxxxxxxxxxxxx \
  --test suite \
  --cpu-threads 4 \
  --wait-ssm-online \
  --output-file /absolute/path/to/existing_instance_suite.json \
  --print-compact-summary
```

### 3) Create/Delete EC2 instance only (lifecycle)

```bash
python3 scripts/cli/ec2_lifecycle.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  create \
  --ami-id ami-xxxxxxxx \
  --instance-type c6i.xlarge \
  --name-tag bench-node

python3 scripts/cli/ec2_lifecycle.py \
  --env-file /absolute/path/to/aws-env-set \
  --region us-west-2 \
  delete \
  --instance-ids i-xxxxxxxxxxxxxxxxx
```

## Notes

- Instance type selection is parameterized by requested vCPU/memory and architecture.
- For Amazon Linux 2023, AMI IDs are resolved dynamically from AWS SSM public parameters.
- For private instances, direct SSH requires private network reachability.
- CoreMark tests require local bundles at `bin/coremark/x86_64/coremark` and/or `bin/coremark/arm64/coremark`.
- FIO tests require local bundles at `bin/fio/x86_64` and/or `bin/fio/arm64`.
