from __future__ import annotations

import sqlite3
import time
import re
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import quote_plus

import streamlit as st
from botocore.exceptions import ClientError

from aws_env import (
    AwsConfigError,
    AwsEnvConfig,
    build_boto3_session,
    load_aws_env_config,
    mask_access_key,
)
from ec2_service import (
    associate_instance_profile,
    create_key_pair,
    ensure_ssm_role_and_profile,
    ensure_ssm_vpc_endpoints,
    get_instance_type_specs,
    get_al2023_ami_id,
    get_default_network,
    get_instance,
    list_instance_families,
    list_instances,
    parse_security_group_ids,
    suggest_instance_types,
    wait_for_ssm_online,
)
from scripts.lib import (
    COREMARK_LINUX_BUNDLE_DIR,
    FIO_LINUX_BUNDLE_DIR,
    create_ec2_instance,
    probe_remote_coremark_exists,
    probe_remote_fio_exists,
    resolve_coremark_bundle_for_arch,
    resolve_fio_bundle_for_arch,
    run_coremark_once,
    run_fio_once,
    run_shell_command_once,
    terminate_ec2_instances,
)


VIEW_LIST = "list"
VIEW_CREATE = "create"
VIEW_SETTINGS = "settings"
VIEW_RESULTS = "results"

VIEW_TO_LABEL = {
    VIEW_LIST: "EC2 List",
    VIEW_CREATE: "Create EC2",
    VIEW_SETTINGS: "Settings",
    VIEW_RESULTS: "Test Results",
}

APP_DIR = Path(__file__).resolve().parent
TEST_RESULTS_DB_PATH = APP_DIR / "test_results.db"
TEST_RESULTS_DB_CONNECT_TIMEOUT_SECONDS = 30.0
TEST_RESULTS_DB_BUSY_TIMEOUT_MS = 30_000
TEST_RESULTS_DB_WRITE_RETRIES = 6
TEST_RESULTS_DB_RETRY_INITIAL_SLEEP_SECONDS = 0.1

TEST_TYPE_CPU = "cpu"
TEST_TYPE_SEQWRITE = "seqwrite"
TEST_TYPE_RANDWRITE = "randwrite"

DEFAULT_CGROUP_LIMIT_CPU_CORES = 2
DEFAULT_CGROUP_LIMIT_MEMORY_GIB = 4.0

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

CPU_ITERATIONS_PER_SEC_PATTERNS = (
    re.compile(r"iterations/sec\s*=\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE),
    re.compile(r"Iterations/Sec\s*:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE),
)


def _detect_coremark_output_issue(raw_output: str, *, thread_count: int) -> Optional[str]:
    output = raw_output or ""
    if "Errors detected" in output or "Must execute for at least 10 secs" in output:
        return "CoreMark output invalid: errors detected or runtime below 10 seconds"
    if thread_count > 1 and "awk:" in output and "syntax error" in output.lower():
        return (
            "CoreMark output invalid: awk aggregation failed in multi-thread mode; "
            "CPU score may be silently under-reported"
        )
    return None


st.set_page_config(page_title="EC2 Manager", page_icon=":cloud:", layout="wide")
st.title("AWS EC2 Benchmark")
st.caption("Create, list, search, and manage EC2 instances from a local AWS env file.")


@st.cache_resource
def _global_settings_cache() -> Dict[str, str]:
    """Persist confirmed settings across page switches/new Streamlit sessions."""
    return {}


def _get_default_env_file_path() -> str:
    candidate = Path.cwd() / "aws-env-set"
    return str(candidate) if candidate.exists() else ""


def _load_session_from_state() -> Optional[Dict[str, str]]:
    config_dict = st.session_state.get("aws_config")
    region = st.session_state.get("region")
    if not config_dict or not region:
        return None
    return {"region": region, "config_dict": config_dict}


def _build_clients() -> Dict[str, object]:
    state_data = _load_session_from_state()
    if not state_data:
        raise RuntimeError("AWS credentials are not loaded.")

    config = AwsEnvConfig.from_dict(state_data["config_dict"])
    region = state_data["region"]
    session = build_boto3_session(config, region_override=region)
    return {
        "session": session,
        "region": region,
        "ec2": session.client("ec2", region_name=region),
        "ssm": session.client("ssm", region_name=region),
        "iam": session.client("iam"),
        "sts": session.client("sts", region_name=region),
    }


def _persist_confirmed_settings(env_file_path: str, region_override: str) -> None:
    cache = _global_settings_cache()
    cache["aws_env_path"] = env_file_path
    cache["region_override"] = region_override


def _get_persisted_settings() -> Dict[str, str]:
    cache = _global_settings_cache()
    return {
        "aws_env_path": cache.get("aws_env_path", ""),
        "region_override": cache.get("region_override", ""),
    }


def _hydrate_session_from_persisted_settings() -> None:
    """Restore session_state from cached settings if session_state is empty."""
    if _load_session_from_state():
        return

    persisted = _get_persisted_settings()
    env_file_path = persisted.get("aws_env_path", "").strip()
    region_override = persisted.get("region_override", "").strip()
    if not env_file_path:
        return

    try:
        config = load_aws_env_config(env_file_path)
        final_region = (region_override or config.region or "").strip()
        if not final_region:
            return

        session = build_boto3_session(config, region_override=final_region)
        identity = session.client("sts", region_name=final_region).get_caller_identity()

        st.session_state["aws_env_path"] = env_file_path
        st.session_state["region_override"] = region_override
        st.session_state["aws_config"] = config.to_dict()
        st.session_state["region"] = final_region
        st.session_state["identity"] = identity
    except (AwsConfigError, ClientError, OSError, RuntimeError):
        # Keep UI stable: user can always re-confirm in Settings.
        return


def _refresh_instance_cache(
    ec2_client: object, include_terminated: bool = False
) -> List[Dict[str, object]]:
    items = list_instances(ec2_client, include_terminated=include_terminated)
    st.session_state["instance_cache"] = items
    st.session_state["instance_list_include_terminated"] = include_terminated
    return items


def _query_param_value(name: str, default: str = "") -> str:
    value = st.query_params.get(name, default)
    if isinstance(value, list):
        return value[0] if value else default
    return value


def _navigate_to(view: str, instance_id: Optional[str] = None) -> None:
    st.query_params["view"] = view
    if instance_id:
        st.query_params["instance_id"] = instance_id
    elif "instance_id" in st.query_params:
        del st.query_params["instance_id"]
    st.rerun()


def _instance_detail_url(instance_id: str) -> str:
    return f"?view={VIEW_LIST}&instance_id={quote_plus(instance_id)}"


def _md_escape(value: object) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _notify_data_loading() -> None:
    if hasattr(st, "toast"):
        try:
            st.toast("data loading")
        except Exception:  # pylint: disable=broad-except
            # Keep UX stable on older Streamlit versions.
            pass


@contextmanager
def _data_loading_scope() -> object:
    _notify_data_loading()
    with st.spinner("data loading"):
        yield


def _connect_test_results_db() -> sqlite3.Connection:
    conn = sqlite3.connect(
        TEST_RESULTS_DB_PATH,
        timeout=TEST_RESULTS_DB_CONNECT_TIMEOUT_SECONDS,
    )
    conn.execute(f"PRAGMA busy_timeout = {TEST_RESULTS_DB_BUSY_TIMEOUT_MS}")
    return conn


def _is_sqlite_busy_error(error: sqlite3.OperationalError) -> bool:
    message = str(error).strip().lower()
    return "locked" in message or "busy" in message


def _extract_cpu_iterations_per_sec(value: object) -> Optional[float]:
    text = str(value or "").strip()
    if not text:
        return None
    for pattern in CPU_ITERATIONS_PER_SEC_PATTERNS:
        match = pattern.search(text)
        if not match:
            continue
        try:
            return float(match.group(1))
        except ValueError:
            continue
    return None


def _create_test_results_table(
    conn: sqlite3.Connection, *, table_name: str = "test_results"
) -> None:
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            instance_id TEXT NOT NULL,
            instance_type TEXT,
            vcpu INTEGER,
            memory_gib REAL,
            test_time TEXT NOT NULL,
            status TEXT NOT NULL,
            cpu_score REAL,
            cpu_iterations_per_sec REAL,
            cpu_test_threads INTEGER,
            seqwrite_bw_mib_s REAL,
            seqwrite_iops REAL,
            seqwrite_disk_util_pct REAL,
            seqwrite_cpu_usr_pct REAL,
            seqwrite_cpu_sys_pct REAL,
            seqwrite_cpu_total_pct REAL,
            randwrite_bw_mib_s REAL,
            randwrite_iops REAL,
            randwrite_avg_latency_ms REAL,
            randwrite_p95_latency_ms REAL,
            randwrite_p99_latency_ms REAL,
            randwrite_disk_util_pct REAL,
            randwrite_cpu_usr_pct REAL,
            randwrite_cpu_sys_pct REAL,
            randwrite_cpu_total_pct REAL,
            cgroup_cpu_cores INTEGER,
            cgroup_memory_gib REAL,
            cgroup_profile TEXT,
            result_summary TEXT NOT NULL,
            raw_output TEXT,
            error_message TEXT
        )
        """
    )


def _migrate_test_results_remove_summary_columns(
    conn: sqlite3.Connection, existing_columns: set[str]
) -> None:
    migrated_table_name = "test_results_migrated"
    target_columns = [
        "id",
        "instance_id",
        "instance_type",
        "vcpu",
        "memory_gib",
        "test_time",
        "status",
        "cpu_score",
        "cpu_iterations_per_sec",
        "cpu_test_threads",
        "seqwrite_bw_mib_s",
        "seqwrite_iops",
        "seqwrite_disk_util_pct",
        "seqwrite_cpu_usr_pct",
        "seqwrite_cpu_sys_pct",
        "seqwrite_cpu_total_pct",
        "randwrite_bw_mib_s",
        "randwrite_iops",
        "randwrite_avg_latency_ms",
        "randwrite_p95_latency_ms",
        "randwrite_p99_latency_ms",
        "randwrite_disk_util_pct",
        "randwrite_cpu_usr_pct",
        "randwrite_cpu_sys_pct",
        "randwrite_cpu_total_pct",
        "cgroup_cpu_cores",
        "cgroup_memory_gib",
        "cgroup_profile",
        "result_summary",
        "raw_output",
        "error_message",
    ]

    conn.execute(f"DROP TABLE IF EXISTS {migrated_table_name}")
    _create_test_results_table(conn, table_name=migrated_table_name)

    target_columns_sql = ", ".join(target_columns)
    select_items: List[str] = []
    for column_name in target_columns:
        if column_name in existing_columns:
            select_items.append(column_name)
        else:
            select_items.append(f"NULL AS {column_name}")

    conn.execute(
        f"""
        INSERT INTO {migrated_table_name} ({target_columns_sql})
        SELECT {", ".join(select_items)}
        FROM test_results
        """
    )

    if "cpu_result" in existing_columns:
        rows = conn.execute(
            """
            SELECT id, cpu_result
            FROM test_results
            WHERE cpu_result IS NOT NULL AND TRIM(cpu_result) != ''
            """
        ).fetchall()
        updates: List[tuple[float, int]] = []
        for row_id, cpu_result in rows:
            parsed_iterations = _extract_cpu_iterations_per_sec(cpu_result)
            if parsed_iterations is None:
                continue
            try:
                normalized_id = int(row_id)
            except (TypeError, ValueError):
                continue
            if normalized_id <= 0:
                continue
            updates.append((parsed_iterations, normalized_id))
        if updates:
            conn.executemany(
                f"""
                UPDATE {migrated_table_name}
                SET cpu_iterations_per_sec = COALESCE(cpu_iterations_per_sec, ?)
                WHERE id = ?
                """,
                updates,
            )

    conn.execute("DROP TABLE test_results")
    conn.execute(f"ALTER TABLE {migrated_table_name} RENAME TO test_results")


def _init_test_results_db() -> None:
    TEST_RESULTS_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    required_columns = {
        "id",
        "instance_id",
        "instance_type",
        "vcpu",
        "memory_gib",
        "test_time",
        "status",
        "cpu_score",
        "cpu_iterations_per_sec",
        "cpu_test_threads",
        "seqwrite_bw_mib_s",
        "seqwrite_iops",
        "seqwrite_disk_util_pct",
        "seqwrite_cpu_usr_pct",
        "seqwrite_cpu_sys_pct",
        "seqwrite_cpu_total_pct",
        "randwrite_bw_mib_s",
        "randwrite_iops",
        "randwrite_avg_latency_ms",
        "randwrite_p95_latency_ms",
        "randwrite_p99_latency_ms",
        "randwrite_disk_util_pct",
        "randwrite_cpu_usr_pct",
        "randwrite_cpu_sys_pct",
        "randwrite_cpu_total_pct",
        "cgroup_cpu_cores",
        "cgroup_memory_gib",
        "cgroup_profile",
        "result_summary",
        "raw_output",
        "error_message",
    }
    deprecated_columns = {"cpu_result", "seqwrite_result", "randwrite_result"}
    legacy_columns = {"command_id", "test_type", "metric_value", "metric_unit"}
    column_migration_types = {
        "instance_id": "TEXT",
        "instance_type": "TEXT",
        "vcpu": "INTEGER",
        "memory_gib": "REAL",
        "test_time": "TEXT",
        "status": "TEXT",
        "cpu_score": "REAL",
        "cpu_iterations_per_sec": "REAL",
        "cpu_test_threads": "INTEGER",
        "seqwrite_bw_mib_s": "REAL",
        "seqwrite_iops": "REAL",
        "seqwrite_disk_util_pct": "REAL",
        "seqwrite_cpu_usr_pct": "REAL",
        "seqwrite_cpu_sys_pct": "REAL",
        "seqwrite_cpu_total_pct": "REAL",
        "randwrite_bw_mib_s": "REAL",
        "randwrite_iops": "REAL",
        "randwrite_avg_latency_ms": "REAL",
        "randwrite_p95_latency_ms": "REAL",
        "randwrite_p99_latency_ms": "REAL",
        "randwrite_disk_util_pct": "REAL",
        "randwrite_cpu_usr_pct": "REAL",
        "randwrite_cpu_sys_pct": "REAL",
        "randwrite_cpu_total_pct": "REAL",
        "cgroup_cpu_cores": "INTEGER",
        "cgroup_memory_gib": "REAL",
        "cgroup_profile": "TEXT",
        "result_summary": "TEXT",
        "raw_output": "TEXT",
        "error_message": "TEXT",
    }

    with _connect_test_results_db() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        table_exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='test_results'"
        ).fetchone()
        if table_exists:
            existing_columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(test_results)").fetchall()
            }
            if existing_columns & legacy_columns:
                conn.execute("DROP TABLE test_results")
                table_exists = None
            elif existing_columns & deprecated_columns:
                _migrate_test_results_remove_summary_columns(conn, existing_columns)
                existing_columns = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(test_results)").fetchall()
                }
            if table_exists:
                missing_columns = sorted(required_columns - existing_columns)
                for column_name in missing_columns:
                    if column_name == "id":
                        continue
                    column_type = column_migration_types.get(column_name)
                    if not column_type:
                        continue
                    conn.execute(
                        f"ALTER TABLE test_results ADD COLUMN {column_name} {column_type}"
                    )

        if not table_exists:
            _create_test_results_table(conn, table_name="test_results")
        conn.commit()


def _insert_test_result(record: Dict[str, object]) -> Optional[int]:
    instance_id = str(record.get("instance_id") or "").strip()
    if not instance_id:
        return None
    test_time = str(
        record.get("test_time")
        or (datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    )
    status = str(record.get("status") or "error")
    result_summary = str(record.get("result_summary") or status)
    insert_params = (
        instance_id,
        record.get("instance_type"),
        record.get("vcpu"),
        record.get("memory_gib"),
        test_time,
        status,
        record.get("cpu_score"),
        record.get("cpu_iterations_per_sec"),
        record.get("cpu_test_threads"),
        record.get("seqwrite_bw_mib_s"),
        record.get("seqwrite_iops"),
        record.get("seqwrite_disk_util_pct"),
        record.get("seqwrite_cpu_usr_pct"),
        record.get("seqwrite_cpu_sys_pct"),
        record.get("seqwrite_cpu_total_pct"),
        record.get("randwrite_bw_mib_s"),
        record.get("randwrite_iops"),
        record.get("randwrite_avg_latency_ms"),
        record.get("randwrite_p95_latency_ms"),
        record.get("randwrite_p99_latency_ms"),
        record.get("randwrite_disk_util_pct"),
        record.get("randwrite_cpu_usr_pct"),
        record.get("randwrite_cpu_sys_pct"),
        record.get("randwrite_cpu_total_pct"),
        record.get("cgroup_cpu_cores"),
        record.get("cgroup_memory_gib"),
        record.get("cgroup_profile"),
        result_summary,
        record.get("raw_output"),
        record.get("error_message"),
    )

    retry_sleep_seconds = TEST_RESULTS_DB_RETRY_INITIAL_SLEEP_SECONDS
    for attempt in range(TEST_RESULTS_DB_WRITE_RETRIES):
        try:
            with _connect_test_results_db() as conn:
                cursor = conn.execute(
                    """
                    INSERT INTO test_results (
                        instance_id,
                        instance_type,
                        vcpu,
                        memory_gib,
                        test_time,
                        status,
                        cpu_score,
                        cpu_iterations_per_sec,
                        cpu_test_threads,
                        seqwrite_bw_mib_s,
                        seqwrite_iops,
                        seqwrite_disk_util_pct,
                        seqwrite_cpu_usr_pct,
                        seqwrite_cpu_sys_pct,
                        seqwrite_cpu_total_pct,
                        randwrite_bw_mib_s,
                        randwrite_iops,
                        randwrite_avg_latency_ms,
                        randwrite_p95_latency_ms,
                        randwrite_p99_latency_ms,
                        randwrite_disk_util_pct,
                        randwrite_cpu_usr_pct,
                        randwrite_cpu_sys_pct,
                        randwrite_cpu_total_pct,
                        cgroup_cpu_cores,
                        cgroup_memory_gib,
                        cgroup_profile,
                        result_summary,
                        raw_output,
                        error_message
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    insert_params,
                )
                conn.commit()
                row_id = int(cursor.lastrowid or 0)
                return row_id if row_id > 0 else None
        except sqlite3.OperationalError as error:
            if not _is_sqlite_busy_error(error) or attempt >= TEST_RESULTS_DB_WRITE_RETRIES - 1:
                raise
            time.sleep(retry_sleep_seconds)
            retry_sleep_seconds = min(retry_sleep_seconds * 2, 1.0)

    return None


def _load_test_results(limit: int = 200) -> List[Dict[str, object]]:
    with _connect_test_results_db() as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT
                id,
                instance_id,
                instance_type,
                vcpu,
                memory_gib,
                test_time,
                status,
                cpu_score,
                cpu_iterations_per_sec,
                cpu_test_threads,
                seqwrite_bw_mib_s,
                seqwrite_iops,
                seqwrite_disk_util_pct,
                seqwrite_cpu_usr_pct,
                seqwrite_cpu_sys_pct,
                seqwrite_cpu_total_pct,
                randwrite_bw_mib_s,
                randwrite_iops,
                randwrite_avg_latency_ms,
                randwrite_p95_latency_ms,
                randwrite_p99_latency_ms,
                randwrite_disk_util_pct,
                randwrite_cpu_usr_pct,
                randwrite_cpu_sys_pct,
                randwrite_cpu_total_pct,
                cgroup_cpu_cores,
                cgroup_memory_gib,
                cgroup_profile,
                result_summary,
                raw_output,
                error_message
            FROM test_results
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

    return [dict(row) for row in rows]


def _delete_test_results_by_ids(row_ids: List[int]) -> None:
    normalized_ids: List[int] = []
    for row_id in row_ids:
        try:
            parsed = int(row_id)
        except (TypeError, ValueError):
            continue
        if parsed > 0:
            normalized_ids.append(parsed)
    if not normalized_ids:
        return

    unique_ids = sorted(set(normalized_ids))
    placeholders = ",".join(["?"] * len(unique_ids))
    retry_sleep_seconds = TEST_RESULTS_DB_RETRY_INITIAL_SLEEP_SECONDS
    for attempt in range(TEST_RESULTS_DB_WRITE_RETRIES):
        try:
            with _connect_test_results_db() as conn:
                conn.execute(
                    f"DELETE FROM test_results WHERE id IN ({placeholders})",
                    tuple(unique_ids),
                )
                conn.commit()
            return
        except sqlite3.OperationalError as error:
            if not _is_sqlite_busy_error(error) or attempt >= TEST_RESULTS_DB_WRITE_RETRIES - 1:
                raise
            time.sleep(retry_sleep_seconds)
            retry_sleep_seconds = min(retry_sleep_seconds * 2, 1.0)


def _get_cpu_family_options(clients: Dict[str, object], architecture: str) -> List[str]:
    cache = st.session_state.setdefault("cpu_family_options_cache", {})
    cache_key = f"{clients['region']}::{architecture}"
    if cache_key in cache:
        return cache[cache_key]

    with _data_loading_scope():
        options = list_instance_families(clients["ec2"], architecture)
    cache[cache_key] = options
    return options


def _split_cpu_family_generation(token: str) -> Optional[tuple[str, str]]:
    cleaned = token.strip().lower()
    if not cleaned:
        return None
    match = re.match(r"^([a-z]+)(.+)$", cleaned)
    if not match:
        return None
    family = match.group(1)
    generation = match.group(2)
    if not generation:
        return None
    return family, generation


def _generation_sort_key(generation: str) -> tuple[int, str]:
    match = re.match(r"^(\d+)(.*)$", generation)
    if not match:
        return (10_000, generation)
    return (int(match.group(1)), match.group(2))


def _build_family_generation_options(family_tokens: List[str]) -> Dict[str, List[str]]:
    grouped: Dict[str, set[str]] = {}
    for token in family_tokens:
        parsed = _split_cpu_family_generation(token)
        if not parsed:
            continue
        family, generation = parsed
        grouped.setdefault(family, set()).add(generation)

    result: Dict[str, List[str]] = {}
    for family in sorted(grouped):
        result[family] = sorted(grouped[family], key=_generation_sort_key)
    return result


def _default_instance_name() -> str:
    return datetime.now().strftime("benchmark-%Y%m%d-%H%M%S")


def _round_to_int_string(value: object) -> str:
    if value is None or value == "":
        return ""
    try:
        return str(int(round(float(value))))
    except (TypeError, ValueError):
        return str(value)


def _format_test_metric(value: object, unit: str, fallback: str = "") -> str:
    rounded = _round_to_int_string(value)
    if rounded:
        normalized_unit = (unit or "").strip()
        if normalized_unit and normalized_unit.lower() not in {"coremark", "score"}:
            return f"{rounded} {normalized_unit}"
        return rounded
    return fallback


def _format_percent_metric(value: object, fallback: str = "") -> str:
    if value is None or value == "":
        return fallback
    try:
        number = float(value)
    except (TypeError, ValueError):
        return fallback
    normalized = f"{number:.2f}".rstrip("0").rstrip(".")
    return f"{normalized}%"


def _render_connection_status() -> None:
    state_data = _load_session_from_state()
    if not state_data:
        st.info("AWS credentials are not loaded. Go to `Settings` and click `Confirm`.")
        return

    identity = st.session_state.get("identity", {})
    account_id = identity.get("Account", "unknown")
    arn = identity.get("Arn", "unknown")
    region = st.session_state.get("region", "unknown")
    masked_key = mask_access_key(st.session_state["aws_config"]["access_key_id"])

    st.success(f"Connected to account `{account_id}` in region `{region}`.")
    with st.expander("Connection details", expanded=False):
        st.write(f"ARN: `{arn}`")
        st.write(f"AWS Access Key: `{masked_key}`")
        st.write(f"Env file: `{st.session_state.get('aws_env_path', '')}`")


def _render_instance_table(rows: List[Dict[str, object]]) -> None:
    headers = [
        "Instance ID",
        "Name",
        "State",
        "Instance Type",
        "Private IP",
        "Public IP",
        "Image ID",
        "Launch Time",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    for row in rows:
        instance_id = _md_escape(row.get("InstanceId", ""))
        name = _md_escape(row.get("Name", ""))
        state = _md_escape(row.get("State", ""))
        instance_type = _md_escape(row.get("InstanceType", ""))
        private_ip = _md_escape(row.get("PrivateIpAddress", ""))
        public_ip = _md_escape(row.get("PublicIpAddress", ""))
        image_id = _md_escape(row.get("ImageId", ""))
        launch_time = _md_escape(row.get("LaunchTime", ""))

        link = f"[{instance_id}]({_instance_detail_url(instance_id)})"
        values = [link, name, state, instance_type, private_ip, public_ip, image_id, launch_time]
        lines.append("| " + " | ".join(values) + " |")

    st.markdown("\n".join(lines))


def _filter_instances_by_keyword(
    instances: List[Dict[str, object]], keyword: str
) -> List[Dict[str, object]]:
    cleaned = keyword.strip().lower()
    if not cleaned:
        return instances

    filtered: List[Dict[str, object]] = []
    for item in instances:
        instance_id = str(item.get("InstanceId", "")).lower()
        name = str(item.get("Name", "") or "").lower()
        if cleaned in instance_id or cleaned in name:
            filtered.append(item)
    return filtered


def _render_list_page(clients: Dict[str, object], instance_id_param: str) -> None:
    if instance_id_param:
        _render_detail_page(clients, clients["region"], instance_id_param)
        return

    col_a, col_b = st.columns([1, 2])
    include_terminated = col_a.checkbox(
        "Include terminated",
        value=st.session_state.get("instance_list_include_terminated", False),
    )
    refresh_clicked = col_b.button("Refresh list", use_container_width=True)

    search_col1, search_col2, search_col3 = st.columns([3, 1, 1])
    search_value = search_col1.text_input(
        "Search (Instance ID or Name)",
        value=st.session_state.get("instance_search_keyword", ""),
        placeholder="e.g. i-0e992440665599a25 or streamlit-ec2",
    )
    search_clicked = search_col2.button("Search", use_container_width=True)
    clear_search_clicked = search_col3.button("Clear", use_container_width=True)

    if clear_search_clicked:
        st.session_state["instance_search_keyword"] = ""

    if search_clicked:
        st.session_state["instance_search_keyword"] = search_value.strip()

    need_refresh_by_filter_change = (
        st.session_state.get("instance_list_include_terminated") != include_terminated
    )

    cache = st.session_state.get("instance_cache", [])
    if refresh_clicked or need_refresh_by_filter_change or not cache:
        try:
            with _data_loading_scope():
                cache = _refresh_instance_cache(
                    clients["ec2"], include_terminated=include_terminated
                )
        except ClientError as error:
            st.error(f"Failed to load instances: {error}")
            cache = []

    keyword = st.session_state.get("instance_search_keyword", "").strip()
    filtered_rows = _filter_instances_by_keyword(cache, keyword)

    if keyword:
        st.caption(f"Search keyword: `{keyword}`")

    if not filtered_rows:
        if keyword:
            st.info("No instance matched the search keyword.")
        else:
            st.info("No instances found.")
        return

    st.caption(f"Total: {len(filtered_rows)} instance(s)")
    _render_instance_table(filtered_rows)


def _render_create_page(clients: Dict[str, object], region: str) -> None:
    st.subheader("Create EC2")
    st.caption(
        "CPU/memory and image are parameterized. The app will recommend instance types that satisfy your requirement."
    )

    left, right = st.columns(2)
    if "create_instance_name" not in st.session_state:
        st.session_state["create_instance_name"] = _default_instance_name()
    name_tag = left.text_input("Name", key="create_instance_name")
    architecture = left.selectbox(
        "Architecture",
        options=["x86_64", "arm64"],
        index=None,
        placeholder="Select architecture",
    )
    selected_cpu_family = None
    selected_generation = None
    preferred_family = None
    if architecture:
        family_options = _get_cpu_family_options(clients, architecture)
        family_generation_map = _build_family_generation_options(family_options)
        available_families = list(family_generation_map.keys())

        if available_families:
            selected_cpu_family = left.selectbox(
                "CPU family",
                options=available_families,
                index=None,
                placeholder="Select CPU family",
                help=(
                    "Options are loaded from AWS API for the selected architecture "
                    "in current region. Select family first, then generation."
                ),
            )
        else:
            left.selectbox(
                "CPU family",
                options=["No available CPU family"],
                index=0,
                disabled=True,
            )
            left.warning("No available CPU family options found for this architecture.")

        generation_options = (
            family_generation_map.get(selected_cpu_family, []) if selected_cpu_family else []
        )
        if selected_cpu_family and generation_options:
            selected_generation = left.selectbox(
                "Generation",
                options=generation_options,
                index=None,
                placeholder="Select generation",
            )
        elif selected_cpu_family:
            left.selectbox(
                "Generation",
                options=["No generation options"],
                index=0,
                disabled=True,
            )
            left.warning("No available generation options found for selected CPU family.")
        else:
            left.selectbox(
                "Generation",
                options=["Select CPU family first"],
                index=0,
                disabled=True,
            )

        if selected_cpu_family and selected_generation:
            preferred_family = f"{selected_cpu_family}{selected_generation}"
    else:
        left.selectbox(
            "CPU family",
            options=["Select architecture first"],
            index=0,
            disabled=True,
        )
        left.selectbox(
            "Generation",
            options=["Select CPU family first"],
            index=0,
            disabled=True,
        )
    vcpu = left.number_input("Requested vCPU", min_value=1, max_value=128, value=2, step=1)
    memory_gib = left.number_input(
        "Requested memory (GiB)",
        min_value=1.0,
        max_value=1024.0,
        value=2.0,
        step=1.0,
    )

    image_mode = right.selectbox(
        "Image",
        options=["Amazon Linux 2023", "Custom AMI ID"],
        index=0,
    )
    custom_ami_id = ""
    if image_mode == "Custom AMI ID":
        custom_ami_id = right.text_input("Custom AMI ID", value="")

    manual_instance_type = right.text_input(
        "Manual instance type override (optional)",
        value="",
        help="Leave empty to auto-select from recommended types.",
    )

    no_public_ip = st.checkbox("Disable public IP", value=True)
    wait_until_running = st.checkbox("Wait until running", value=True)

    st.markdown("**Network settings**")
    network_col1, network_col2 = st.columns(2)
    subnet_id_input = network_col1.text_input(
        "Subnet ID (optional)",
        value="",
        help="If empty, default subnet is used.",
    )
    security_groups_input = network_col2.text_input(
        "Security group IDs (comma separated, optional)",
        value="",
        help="If empty, default security group is used.",
    )

    st.markdown("**Key pair settings**")
    key_mode = st.selectbox(
        "Key pair mode",
        options=["No key pair (SSM only)", "Use existing key pair", "Create new key pair"],
        index=0,
    )
    existing_key_name = ""
    key_prefix = "streamlit-ec2-key"
    if key_mode == "Use existing key pair":
        existing_key_name = st.text_input("Existing key pair name", value="")
    if key_mode == "Create new key pair":
        key_prefix = st.text_input("New key pair prefix", value="streamlit-ec2-key")

    st.markdown("**Optional private-access setup**")
    setup_ssm_for_private = st.checkbox(
        "Configure private SSM access (IAM role/profile + VPC endpoints)",
        value=True,
        help="Recommended for instances without public IP.",
    )
    ssm_role_name = st.text_input("SSM role name", value="CodexEC2SSMRole")
    ssm_profile_name = st.text_input(
        "SSM instance profile name", value="CodexEC2SSMInstanceProfile"
    )

    submitted = st.button("Create instance", type="primary")

    if not submitted:
        return

    try:
        ec2_client = clients["ec2"]
        ssm_client = clients["ssm"]
        iam_client = clients["iam"]

        with _data_loading_scope():
            if not architecture:
                raise ValueError("Please select architecture first.")

            selected_family_prefixes: List[str] = []
            if not manual_instance_type.strip():
                if not preferred_family:
                    raise ValueError("Please select CPU family and generation.")
                selected_family_prefixes = [preferred_family]

            if image_mode == "Amazon Linux 2023":
                ami_id = get_al2023_ami_id(ssm_client, architecture)
            else:
                if not custom_ami_id.strip():
                    raise ValueError("Custom AMI ID is required when image mode is Custom AMI ID.")
                ami_id = custom_ami_id.strip()

            if manual_instance_type.strip():
                selected_instance_type = manual_instance_type.strip()
                recommendations = []
            else:
                recommendations = suggest_instance_types(
                    ec2_client=ec2_client,
                    vcpu=int(vcpu),
                    memory_gib=float(memory_gib),
                    architecture=architecture,
                    family_prefixes=selected_family_prefixes,
                )
                if not recommendations:
                    family_hint = (
                        f" and family `{preferred_family}`"
                        if preferred_family
                        else ""
                    )
                    raise RuntimeError(
                        "No matching instance type found for the requested "
                        f"CPU/memory/architecture{family_hint}."
                    )
                selected_instance_type = recommendations[0]["InstanceType"]

            subnet_id = subnet_id_input.strip() or None
            security_group_ids = parse_security_group_ids(security_groups_input)

            if no_public_ip and (not subnet_id or not security_group_ids):
                defaults = get_default_network(ec2_client)
                subnet_id = subnet_id or defaults["SubnetId"]
                if not security_group_ids:
                    security_group_ids = [defaults["SecurityGroupId"]]

        key_name = None
        key_result = None
        if key_mode == "Use existing key pair":
            if not existing_key_name.strip():
                raise ValueError("Existing key pair name cannot be empty.")
            key_name = existing_key_name.strip()
        if key_mode == "Create new key pair":
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            key_name = f"{key_prefix.strip() or 'streamlit-ec2-key'}-{timestamp}"
            key_result = create_key_pair(ec2_client, key_name)

        iam_profile_name_to_use = None
        ssm_setup_result = None
        endpoint_result = []
        if setup_ssm_for_private:
            ssm_setup_result = ensure_ssm_role_and_profile(
                iam_client,
                role_name=ssm_role_name.strip(),
                profile_name=ssm_profile_name.strip(),
            )
            iam_profile_name_to_use = ssm_setup_result["ProfileName"]

        with st.spinner("Creating EC2 instance..."):
            created = create_ec2_instance(
                ec2_client=ec2_client,
                ami_id=ami_id,
                instance_type=selected_instance_type,
                name_tag=name_tag,
                no_public_ip=no_public_ip,
                subnet_id=subnet_id,
                security_group_ids=security_group_ids,
                key_name=key_name,
                iam_instance_profile_name=iam_profile_name_to_use,
                wait_until_running=wait_until_running,
            )

        if setup_ssm_for_private:
            with st.spinner("Associating SSM instance profile..."):
                associate_instance_profile(
                    ec2_client,
                    instance_id=created["InstanceId"],
                    profile_name=ssm_profile_name.strip(),
                )

            if no_public_ip:
                with st.spinner("Ensuring private SSM VPC endpoints..."):
                    endpoint_result = ensure_ssm_vpc_endpoints(
                        ec2_client=ec2_client,
                        region=region,
                        vpc_id=created["VpcId"],
                        subnet_id=created["SubnetId"],
                        security_group_id=created["SecurityGroupIds"][0],
                    )

        ssm_status = None
        if setup_ssm_for_private:
            with st.spinner("Waiting for SSM online status..."):
                ssm_status = wait_for_ssm_online(
                    ssm_client,
                    created["InstanceId"],
                    timeout_seconds=180,
                    poll_seconds=10,
                )

        st.success(f"Created instance: {created['InstanceId']}")
        if recommendations:
            st.info(
                f"Auto-selected instance type `{selected_instance_type}` "
                f"for requested {int(vcpu)} vCPU / {float(memory_gib):.1f} GiB."
            )

        result_payload = {
            "Region": region,
            "Instance": created,
            "ImageId": ami_id,
            "InstanceType": selected_instance_type,
            "CPUFamily": selected_cpu_family or "",
            "CPUGeneration": selected_generation or "",
            "PreferredFamily": preferred_family or "",
            "NoPublicIp": no_public_ip,
            "SubnetIdUsed": subnet_id,
            "SecurityGroupIdsUsed": security_group_ids,
            "SSMSetup": ssm_setup_result,
            "VpcEndpoints": endpoint_result,
            "SsmOnline": bool(ssm_status),
        }
        st.json(result_payload)

        ssm_command = f"aws ssm start-session --region {region} --target {created['InstanceId']}"
        st.code(ssm_command, language="bash")

        if key_result:
            st.warning(
                "A new key pair was created. Download the private key now. "
                "AWS does not let you retrieve it again later."
            )
            st.download_button(
                "Download private key (.pem)",
                data=key_result["KeyMaterial"],
                file_name=f"{key_result['KeyName']}.pem",
                mime="application/x-pem-file",
            )

        if key_name and created.get("PrivateIpAddress"):
            ssh_command = (
                f"ssh -i /absolute/path/to/{key_name}.pem "
                f"ec2-user@{created['PrivateIpAddress']}"
            )
            st.code(ssh_command, language="bash")

        st.markdown(f"[Go to instance detail]({_instance_detail_url(created['InstanceId'])})")
        with _data_loading_scope():
            _refresh_instance_cache(clients["ec2"], include_terminated=False)
    except (ClientError, RuntimeError, ValueError) as error:
        st.error(f"Create failed: {error}")


def _run_coremark_test(
    clients: Dict[str, object],
    instance: Dict[str, object],
    specs: Dict[str, object],
    *,
    cpu_threads: int = 16,
) -> None:
    instance_id = str(instance["InstanceId"])
    duration_seconds = 30
    remote_dir = "/tmp/coremark_streamlit"
    cpu_threads = max(int(cpu_threads), 1)
    remote_command = (
        f"parallel_coremark workers={cpu_threads}, per_worker='timeout {duration_seconds}s {remote_dir}/coremark 0x0 0x0 0x66 0'"
    )
    test_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    st.markdown("**Live output**")
    status_placeholder = st.empty()
    log_placeholder = st.empty()
    live_output_lines: List[str] = []

    def _on_live_update(status: str, lines: List[str]) -> None:
        status_placeholder.write(f"Command status: `{status}` | Refresh every 2s")
        log_placeholder.code("\n".join(lines[-4000:]), language="bash")

    try:
        arch = str(instance.get("Architecture") or "").strip().lower()
        if not arch:
            raise RuntimeError(
                "Instance architecture is unavailable. Refresh instance detail and try again."
            )
        coremark_bundle = resolve_coremark_bundle_for_arch(arch)
    except RuntimeError as error:
        error_message = f"Failed to resolve local coremark bundle: {error}"
        st.error(error_message)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": None,
                "test_type": TEST_TYPE_CPU,
                "metric_value": None,
                "metric_unit": "coremark",
                "coremark_score": None,
                "iterations_per_sec": None,
                "cpu_test_threads": cpu_threads,
                "result_summary": "CoreMark binary missing",
                "raw_output": "",
                "error_message": error_message,
            }
        )
        return

    try:
        with _data_loading_scope():
            run_result = run_coremark_once(
                clients["ssm"],
                instance_id=instance_id,
                linux_binary_path=str(coremark_bundle["coremark_binary"]),
                command_text=remote_command,
                duration_seconds=duration_seconds,
                cpu_threads=cpu_threads,
                upload_binary=True,
                max_polls=240,
                poll_interval_seconds=2,
                marker_prefixes_to_skip=["__COREMARK_RUNNING__"],
                running_marker_prefix="__RUNNING__",
                live_output_lines=live_output_lines,
                on_update=_on_live_update,
            )
    except (ClientError, FileNotFoundError, RuntimeError) as error:
        message = f"Failed to start CoreMark test: {error}"
        st.error(message)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": None,
                "test_type": TEST_TYPE_CPU,
                "metric_value": None,
                "metric_unit": "coremark",
                "coremark_score": None,
                "iterations_per_sec": None,
                "cpu_test_threads": cpu_threads,
                "result_summary": "Failed to start test",
                "raw_output": "",
                "error_message": message,
            }
        )
        return

    command_id = run_result.command_id
    st.info(f"CoreMark command started: `{command_id}`")
    status = str(run_result.status)
    poll_error = run_result.poll_error
    combined_output = str(run_result.output or "")
    parsed = run_result.parsed
    exit_code = parsed.get("exit_code")
    score = parsed.get("coremark_score")
    iterations_per_sec = parsed.get("iterations_per_sec")
    coremark_output_issue = _detect_coremark_output_issue(
        combined_output,
        thread_count=cpu_threads,
    )

    if poll_error:
        result_summary = "Failed to poll test command"
        st.error(f"{result_summary}: {poll_error}")
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": TEST_TYPE_CPU,
                "metric_value": score,
                "metric_unit": "coremark",
                "coremark_score": score,
                "iterations_per_sec": iterations_per_sec,
                "cpu_test_threads": cpu_threads,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": poll_error,
            }
        )
        return

    if status != "Success":
        result_summary = f"Command ended with status: {status}"
        st.error(result_summary)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": TEST_TYPE_CPU,
                "metric_value": score,
                "metric_unit": "coremark",
                "coremark_score": score,
                "iterations_per_sec": iterations_per_sec,
                "cpu_test_threads": cpu_threads,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": result_summary,
            }
        )
        return

    if coremark_output_issue:
        result_summary = coremark_output_issue
        st.error(result_summary)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": TEST_TYPE_CPU,
                "metric_value": None,
                "metric_unit": "coremark",
                "coremark_score": score,
                "iterations_per_sec": iterations_per_sec,
                "cpu_test_threads": cpu_threads,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": result_summary,
            }
        )
        return

    if score is None and iterations_per_sec is None:
        result_summary = "CoreMark metrics not found in output"
        st.error(result_summary)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": TEST_TYPE_CPU,
                "metric_value": None,
                "metric_unit": "coremark",
                "coremark_score": None,
                "iterations_per_sec": iterations_per_sec,
                "cpu_test_threads": cpu_threads,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": result_summary,
            }
        )
        return

    metric_value = score if score is not None else iterations_per_sec
    metric_label = (
        f"CoreMark score={score}"
        if score is not None
        else f"iterations/sec={iterations_per_sec}"
    )
    summary = f"{metric_label}, iterations/sec={iterations_per_sec}, exit_code={exit_code}"
    st.success(summary)
    _insert_test_result(
        {
            "instance_id": instance_id,
            "instance_type": instance.get("InstanceType"),
            "vcpu": specs.get("vCPU"),
            "memory_gib": specs.get("MemoryGiB"),
            "test_time": test_time,
            "status": "success",
            "command_id": command_id,
            "test_type": TEST_TYPE_CPU,
            "metric_value": metric_value,
            "metric_unit": "coremark",
            "coremark_score": score,
            "iterations_per_sec": iterations_per_sec,
            "cpu_test_threads": cpu_threads,
            "result_summary": summary,
            "raw_output": combined_output,
            "error_message": None,
        }
    )


def _run_fio_test(
    clients: Dict[str, object],
    instance: Dict[str, object],
    specs: Dict[str, object],
    *,
    test_type: str,
    display_name: str,
    fio_command: str,
) -> None:
    instance_id = str(instance["InstanceId"])
    try:
        arch = str(instance.get("Architecture") or "").strip().lower()
        if not arch:
            raise RuntimeError(
                "Instance architecture is unavailable. Refresh instance detail and try again."
            )
        fio_bundle = resolve_fio_bundle_for_arch(arch)
    except RuntimeError as error:
        message = f"Failed to resolve local fio bundle: {error}"
        st.error(message)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                "status": "error",
                "command_id": None,
                "test_type": test_type,
                "metric_value": None,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": f"{display_name} start failed",
                "raw_output": "",
                "error_message": message,
            }
        )
        return

    cleanup_glob = f"/mnt/fio/{test_type}*"
    displayed_command = f"{fio_command} ; rm -f {cleanup_glob}"
    test_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    st.markdown("**Live output**")
    status_placeholder = st.empty()
    log_placeholder = st.empty()
    live_output_lines: List[str] = []

    def _on_live_update(status: str, lines: List[str]) -> None:
        status_placeholder.write(f"Command status: `{status}` | Refresh every 2s")
        log_placeholder.code("\n".join(lines[-4000:]), language="bash")

    try:
        with _data_loading_scope():
            run_result = run_fio_once(
                clients["ssm"],
                instance_id=instance_id,
                test_name=test_type,
                fio_command=fio_command,
                cleanup_glob=cleanup_glob,
                linux_fio_binary_path=str(fio_bundle["fio_binary"]),
                linux_fio_engine_libaio_path=str(fio_bundle["fio_engine"]),
                linux_shared_lib_paths=[str(path) for path in fio_bundle["shared_libs"]],
                command_text=displayed_command,
                upload_bundle=True,
                timeout_seconds=900,
                max_polls=360,
                poll_interval_seconds=2,
                live_output_lines=live_output_lines,
                on_update=_on_live_update,
            )
    except (ClientError, RuntimeError) as error:
        message = f"Failed to start {display_name} test: {error}"
        st.error(message)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": None,
                "test_type": test_type,
                "metric_value": None,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": f"{display_name} start failed",
                "raw_output": "",
                "error_message": message,
            }
        )
        return

    command_id = run_result.command_id
    st.info(f"{display_name} command started: `{command_id}`")
    status = str(run_result.status)
    poll_error = run_result.poll_error
    combined_output = str(run_result.output or "")
    parsed = run_result.parsed
    bw_mib_s = parsed.get("bw_mib_s")
    iops = parsed.get("iops")
    exit_code = parsed.get("exit_code")

    if poll_error:
        result_summary = f"{display_name} poll failed"
        st.error(f"{result_summary}: {poll_error}")
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": test_type,
                "metric_value": bw_mib_s,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": str(poll_error),
            }
        )
        return

    if status != "Success":
        result_summary = f"{display_name} command ended with status: {status}"
        st.error(result_summary)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": test_type,
                "metric_value": bw_mib_s,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": result_summary,
            }
        )
        return

    if bw_mib_s is None:
        result_summary = f"{display_name} bandwidth not found in output"
        st.error(result_summary)
        _insert_test_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": test_time,
                "status": "error",
                "command_id": command_id,
                "test_type": test_type,
                "metric_value": None,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": result_summary,
                "raw_output": combined_output,
                "error_message": result_summary,
            }
        )
        return

    summary = (
        f"{display_name} bw={bw_mib_s:.2f} MiB/s, "
        f"iops={iops if iops is not None else 'N/A'}, exit_code={exit_code}"
    )
    st.success(summary)
    _insert_test_result(
        {
            "instance_id": instance_id,
            "instance_type": instance.get("InstanceType"),
            "vcpu": specs.get("vCPU"),
            "memory_gib": specs.get("MemoryGiB"),
            "test_time": test_time,
            "status": "success",
            "command_id": command_id,
            "test_type": test_type,
            "metric_value": bw_mib_s,
            "metric_unit": "MiB/s",
            "coremark_score": None,
            "iterations_per_sec": None,
            "result_summary": summary,
            "raw_output": combined_output,
            "error_message": None,
        }
    )


def _append_live_output_block(
    live_output_lines: List[str], log_placeholder: object, message: str
) -> None:
    for line in message.splitlines():
        live_output_lines.append(line)
    log_placeholder.code("\n".join(live_output_lines[-4000:]), language="bash")


def _run_cpu_io_test_suite(
    clients: Dict[str, object],
    instance: Dict[str, object],
    specs: Dict[str, object],
    *,
    cpu_threads: int = 16,
    cgroup_cpu_cores: Optional[int] = None,
    cgroup_memory_gib: Optional[float] = None,
) -> None:
    instance_id = str(instance["InstanceId"])
    suite_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

    st.markdown("**CPU/IO Test Live output**")
    status_placeholder = st.empty()
    log_placeholder = st.empty()
    cgroup_profile = "unlimited"
    cgroup_memory_mib: Optional[int] = None
    if cgroup_cpu_cores is not None and cgroup_memory_gib is not None:
        cgroup_memory_mib = int(float(cgroup_memory_gib) * 1024)
        cgroup_profile = f"cgroup-v2 cpu={int(cgroup_cpu_cores)} mem={float(cgroup_memory_gib):g}GiB"

    configured_cpu_threads = max(int(cpu_threads), 1)
    live_output_lines: List[str] = [
        f"# CPU/IO Test Suite started at {suite_time}",
        f"# instance_id={instance_id}",
        f"# resource_profile={cgroup_profile}",
        f"# cpu_threads={configured_cpu_threads}",
    ]
    log_placeholder.code("\n".join(live_output_lines), language="bash")

    suite_results: List[str] = []
    suite_failed_lines: List[str] = []
    suite_summary_map: Dict[str, Dict[str, str]] = {
        "CPU": {"Test": "CPU", "Status": "PENDING", "Key Metric": "", "Command ID": ""},
        "SeqWrite": {
            "Test": "SeqWrite",
            "Status": "PENDING",
            "Key Metric": "",
            "Command ID": "",
        },
        "RandWrite": {
            "Test": "RandWrite",
            "Status": "PENDING",
            "Key Metric": "",
            "Command ID": "",
        },
    }
    suite_db_record: Dict[str, object] = {
        "instance_id": instance_id,
        "instance_type": instance.get("InstanceType"),
        "vcpu": specs.get("vCPU"),
        "memory_gib": specs.get("MemoryGiB"),
        "test_time": suite_time,
        "status": "error",
        "cpu_score": None,
        "cpu_iterations_per_sec": None,
        "cpu_test_threads": configured_cpu_threads,
        "seqwrite_bw_mib_s": None,
        "seqwrite_iops": None,
        "seqwrite_disk_util_pct": None,
        "seqwrite_cpu_usr_pct": None,
        "seqwrite_cpu_sys_pct": None,
        "seqwrite_cpu_total_pct": None,
        "randwrite_bw_mib_s": None,
        "randwrite_iops": None,
        "randwrite_avg_latency_ms": None,
        "randwrite_p95_latency_ms": None,
        "randwrite_p99_latency_ms": None,
        "randwrite_disk_util_pct": None,
        "randwrite_cpu_usr_pct": None,
        "randwrite_cpu_sys_pct": None,
        "randwrite_cpu_total_pct": None,
        "cgroup_cpu_cores": cgroup_cpu_cores,
        "cgroup_memory_gib": cgroup_memory_gib,
        "cgroup_profile": cgroup_profile,
        "result_summary": "",
        "raw_output": "",
        "error_message": None,
    }
    suite_step_result_ids: List[int] = []

    def _insert_suite_step_result(record: Dict[str, object]) -> None:
        row_id = _insert_test_result(record)
        if row_id is not None:
            suite_step_result_ids.append(row_id)

    fio_bundle: Optional[Dict[str, object]] = None
    fio_bundle_error: Optional[str] = None
    fio_bundle_uploaded = False
    coremark_bundle: Optional[Dict[str, object]] = None
    coremark_bundle_error: Optional[str] = None
    try:
        arch = str(instance.get("Architecture") or "").strip().lower()
        if not arch:
            raise RuntimeError(
                "Instance architecture is unavailable. Refresh instance detail and try again."
            )
        fio_bundle = resolve_fio_bundle_for_arch(arch)
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            (
                f"# FIO bundle selected: arch={fio_bundle['architecture']} "
                f"path={fio_bundle['bundle_root']}"
            ),
        )
    except RuntimeError as error:
        fio_bundle_error = str(error)
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            f"# FIO bundle resolve failed: {fio_bundle_error}",
        )
    try:
        arch = str(instance.get("Architecture") or "").strip().lower()
        if not arch:
            raise RuntimeError(
                "Instance architecture is unavailable. Refresh instance detail and try again."
            )
        coremark_bundle = resolve_coremark_bundle_for_arch(arch)
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            (
                f"# CoreMark bundle selected: arch={coremark_bundle['architecture']} "
                f"path={coremark_bundle['bundle_root']}"
            ),
        )
    except RuntimeError as error:
        coremark_bundle_error = str(error)
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            f"# CoreMark bundle resolve failed: {coremark_bundle_error}",
        )

    def _set_command_id(label: str, command_id: Optional[object]) -> None:
        if label not in suite_summary_map:
            return
        suite_summary_map[label]["Command ID"] = str(command_id or "")

    def _record_summary(label: str, summary: str, success: bool) -> None:
        marker = "PASS" if success else "FAIL"
        line = f"[{label}] {marker}: {summary}"
        suite_results.append(line)
        if not success:
            suite_failed_lines.append(line)
        _append_live_output_block(live_output_lines, log_placeholder, line)
        if label in suite_summary_map:
            suite_summary_map[label]["Status"] = marker
            suite_summary_map[label]["Key Metric"] = summary

    if not fio_bundle_error:
        try:
            with _data_loading_scope():
                remote_fio_ready = probe_remote_fio_exists(
                    clients["ssm"],
                    instance_id=instance_id,
                )
            if remote_fio_ready:
                fio_bundle_uploaded = True
                _append_live_output_block(
                    live_output_lines,
                    log_placeholder,
                    "[IO] 机器上已存在fio或coremark，跳过上传(fio)",
                )
            else:
                _append_live_output_block(
                    live_output_lines,
                    log_placeholder,
                    "[IO] 要上传fio或coremark(fio)",
                )
        except (ClientError, RuntimeError) as error:
            _append_live_output_block(
                live_output_lines,
                log_placeholder,
                f"[IO] 要上传fio或coremark(fio，探测失败默认上传): {error}",
            )

    coremark_upload_needed = True
    try:
        with _data_loading_scope():
            remote_coremark_ready = probe_remote_coremark_exists(
                clients["ssm"],
                instance_id=instance_id,
            )
        if remote_coremark_ready:
            coremark_upload_needed = False
            _append_live_output_block(
                live_output_lines,
                log_placeholder,
                "[CPU] 机器上已存在fio或coremark，跳过上传(coremark)",
            )
        else:
            _append_live_output_block(
                live_output_lines,
                log_placeholder,
                "[CPU] 要上传fio或coremark(coremark)",
            )
    except (ClientError, RuntimeError) as error:
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            f"[CPU] 要上传fio或coremark(coremark，探测失败默认上传): {error}",
        )

    # 1) CPU test
    cpu_test_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    cpu_duration_seconds = 30
    cpu_remote_dir = "/tmp/coremark_streamlit"
    _append_live_output_block(
        live_output_lines,
        log_placeholder,
        f"[CPU] thread_count={configured_cpu_threads}",
    )
    cpu_command = (
        f"parallel_coremark workers={configured_cpu_threads}, per_worker='timeout {cpu_duration_seconds}s {cpu_remote_dir}/coremark 0x0 0x0 0x66 0'"
    )
    if coremark_bundle_error or not coremark_bundle:
        error_message = (
            f"Failed to resolve local coremark bundle: "
            f"{coremark_bundle_error or 'coremark bundle not available'}"
        )
        _record_summary("CPU", error_message, success=False)
        _insert_suite_step_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": cpu_test_time,
                "status": "error",
                "command_id": None,
                "test_type": TEST_TYPE_CPU,
                "metric_value": None,
                        "metric_unit": "coremark",
                        "coremark_score": None,
                        "iterations_per_sec": None,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": "CoreMark binary missing",
                        "raw_output": "",
                        "error_message": error_message,
            }
        )
    else:
        try:
            with _data_loading_scope():
                cpu_run_result = run_coremark_once(
                    clients["ssm"],
                    instance_id=instance_id,
                    linux_binary_path=str(coremark_bundle["coremark_binary"]),
                    command_text=cpu_command,
                    duration_seconds=cpu_duration_seconds,
                    cpu_threads=configured_cpu_threads,
                    upload_binary=coremark_upload_needed,
                    cgroup_cpu_cores=cgroup_cpu_cores,
                    cgroup_memory_mib=cgroup_memory_mib,
                    marker_prefixes_to_skip=["__COREMARK_RUNNING__"],
                    max_polls=240,
                    poll_interval_seconds=2,
                    running_marker_prefix="__CPU_RUNNING__",
                    live_output_lines=live_output_lines,
                    on_update=lambda status, lines: (
                        status_placeholder.write(
                            f"Command status: `{status}` | Refresh every 2s"
                        ),
                        log_placeholder.code("\n".join(lines[-4000:]), language="bash"),
                    ),
                )
        except (ClientError, FileNotFoundError, RuntimeError) as error:
            message = f"Failed to start CPU test: {error}"
            _record_summary("CPU", message, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": cpu_test_time,
                    "status": "error",
                    "command_id": None,
                    "test_type": TEST_TYPE_CPU,
                    "metric_value": None,
                    "metric_unit": "coremark",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "cpu_test_threads": configured_cpu_threads,
                    "result_summary": "Failed to start CPU test",
                    "raw_output": "",
                    "error_message": message,
                }
            )
        else:
            cpu_command_id = cpu_run_result.command_id
            _append_live_output_block(
                live_output_lines,
                log_placeholder,
                f"[CPU] command started: {cpu_command_id}",
            )
            _set_command_id("CPU", cpu_command_id)
            cpu_status = str(cpu_run_result.status)
            cpu_poll_error = cpu_run_result.poll_error
            cpu_output = str(cpu_run_result.output or "")
            cpu_parsed = cpu_run_result.parsed
            cpu_score = cpu_parsed.get("coremark_score")
            cpu_iterations_per_sec = cpu_parsed.get("iterations_per_sec")
            cpu_exit_code = cpu_parsed.get("exit_code")
            coremark_output_issue = _detect_coremark_output_issue(
                cpu_output,
                thread_count=configured_cpu_threads,
            )
            suite_db_record["cpu_score"] = cpu_score
            suite_db_record["cpu_iterations_per_sec"] = cpu_iterations_per_sec

            if cpu_poll_error:
                summary = f"Failed to poll CPU test: {cpu_poll_error}"
                _record_summary("CPU", summary, success=False)
                _insert_suite_step_result(
                    {
                        "instance_id": instance_id,
                        "instance_type": instance.get("InstanceType"),
                        "vcpu": specs.get("vCPU"),
                        "memory_gib": specs.get("MemoryGiB"),
                        "test_time": cpu_test_time,
                        "status": "error",
                        "command_id": cpu_command_id,
                        "test_type": TEST_TYPE_CPU,
                        "metric_value": cpu_score,
                        "metric_unit": "coremark",
                        "coremark_score": cpu_score,
                        "iterations_per_sec": cpu_iterations_per_sec,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": "Failed to poll CPU test",
                        "raw_output": cpu_output,
                        "error_message": str(cpu_poll_error),
                    }
                )
            elif cpu_status != "Success":
                summary = f"CPU test command ended with status: {cpu_status}"
                _record_summary("CPU", summary, success=False)
                _insert_suite_step_result(
                    {
                        "instance_id": instance_id,
                        "instance_type": instance.get("InstanceType"),
                        "vcpu": specs.get("vCPU"),
                        "memory_gib": specs.get("MemoryGiB"),
                        "test_time": cpu_test_time,
                        "status": "error",
                        "command_id": cpu_command_id,
                        "test_type": TEST_TYPE_CPU,
                        "metric_value": cpu_score,
                        "metric_unit": "coremark",
                        "coremark_score": cpu_score,
                        "iterations_per_sec": cpu_iterations_per_sec,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": summary,
                        "raw_output": cpu_output,
                        "error_message": summary,
                    }
                )
            elif coremark_output_issue:
                summary = coremark_output_issue
                _record_summary("CPU", summary, success=False)
                _insert_suite_step_result(
                    {
                        "instance_id": instance_id,
                        "instance_type": instance.get("InstanceType"),
                        "vcpu": specs.get("vCPU"),
                        "memory_gib": specs.get("MemoryGiB"),
                        "test_time": cpu_test_time,
                        "status": "error",
                        "command_id": cpu_command_id,
                        "test_type": TEST_TYPE_CPU,
                        "metric_value": None,
                        "metric_unit": "coremark",
                        "coremark_score": cpu_score,
                        "iterations_per_sec": cpu_iterations_per_sec,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": summary,
                        "raw_output": cpu_output,
                        "error_message": summary,
                    }
                )
            elif cpu_score is None and cpu_iterations_per_sec is None:
                summary = "CoreMark metrics not found in output"
                _record_summary("CPU", summary, success=False)
                _insert_suite_step_result(
                    {
                        "instance_id": instance_id,
                        "instance_type": instance.get("InstanceType"),
                        "vcpu": specs.get("vCPU"),
                        "memory_gib": specs.get("MemoryGiB"),
                        "test_time": cpu_test_time,
                        "status": "error",
                        "command_id": cpu_command_id,
                        "test_type": TEST_TYPE_CPU,
                        "metric_value": None,
                        "metric_unit": "coremark",
                        "coremark_score": None,
                        "iterations_per_sec": cpu_iterations_per_sec,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": summary,
                        "raw_output": cpu_output,
                        "error_message": summary,
                    }
                )
            else:
                cpu_score_str = _round_to_int_string(cpu_score)
                cpu_iter_str = _round_to_int_string(cpu_iterations_per_sec)
                metric_value = cpu_score if cpu_score is not None else cpu_iterations_per_sec
                summary = (
                    f"coremark={cpu_score_str}, iterations/sec={cpu_iter_str}, "
                    f"exit_code={cpu_exit_code}"
                )
                _record_summary("CPU", summary, success=True)
                _insert_suite_step_result(
                    {
                        "instance_id": instance_id,
                        "instance_type": instance.get("InstanceType"),
                        "vcpu": specs.get("vCPU"),
                        "memory_gib": specs.get("MemoryGiB"),
                        "test_time": cpu_test_time,
                        "status": "success",
                        "command_id": cpu_command_id,
                        "test_type": TEST_TYPE_CPU,
                        "metric_value": metric_value,
                        "metric_unit": "coremark",
                        "coremark_score": cpu_score,
                        "iterations_per_sec": cpu_iterations_per_sec,
                        "cpu_test_threads": configured_cpu_threads,
                        "result_summary": summary,
                        "raw_output": cpu_output,
                        "error_message": None,
                    }
                )

    def _run_fio_case(test_type: str, display_name: str, fio_command: str) -> None:
        nonlocal fio_bundle_uploaded
        if fio_bundle_error or not fio_bundle:
            summary = f"{display_name} start failed: {fio_bundle_error or 'fio bundle not available'}"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
                    "status": "error",
                    "command_id": None,
                    "test_type": test_type,
                    "metric_value": None,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": "",
                    "error_message": summary,
                }
            )
            return

        prep_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        prep_command = "mkdir -p /mnt/fio"
        try:
            with _data_loading_scope():
                prep_run_result = run_shell_command_once(
                    clients["ssm"],
                    instance_id=instance_id,
                    commands=["set -euo pipefail", prep_command],
                    command_text=prep_command,
                    comment=f"Prepare fio dir for {test_type}",
                    timeout_seconds=120,
                    max_polls=60,
                    poll_interval_seconds=2,
                    running_marker_prefix=f"__{display_name.upper()}_PREP__",
                    live_output_lines=live_output_lines,
                    on_update=lambda status, lines: (
                        status_placeholder.write(
                            f"Command status: `{status}` | Refresh every 2s"
                        ),
                        log_placeholder.code("\n".join(lines[-4000:]), language="bash"),
                    ),
                )
        except (ClientError, RuntimeError) as error:
            summary = f"{display_name} prepare /mnt/fio failed: {error}"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": prep_time,
                    "status": "error",
                    "command_id": None,
                    "test_type": test_type,
                    "metric_value": None,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": "",
                    "error_message": summary,
                }
            )
            return

        prep_command_id = prep_run_result.command_id
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            f"[{display_name}] prepare dir command started: {prep_command_id}",
        )
        _set_command_id(display_name, prep_command_id)
        prep_status = str(prep_run_result.status)
        prep_error = prep_run_result.poll_error
        prep_output = str(prep_run_result.output or "")
        if prep_error or prep_status != "Success":
            summary = (
                f"{display_name} prepare /mnt/fio failed: {prep_error}"
                if prep_error
                else f"{display_name} prepare /mnt/fio status={prep_status}"
            )
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": prep_time,
                    "status": "error",
                    "command_id": prep_command_id,
                    "test_type": test_type,
                    "metric_value": None,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": prep_output,
                    "error_message": summary,
                }
            )
            return

        fio_test_time = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        cleanup_glob = f"/mnt/fio/{test_type}*"
        displayed_command = f"{fio_command} ; rm -f {cleanup_glob}"
        try:
            with _data_loading_scope():
                fio_run_result = run_fio_once(
                    clients["ssm"],
                    instance_id=instance_id,
                    test_name=test_type,
                    fio_command=fio_command,
                    cleanup_glob=cleanup_glob,
                    linux_fio_binary_path=str(fio_bundle["fio_binary"]),
                    linux_fio_engine_libaio_path=str(fio_bundle["fio_engine"]),
                    linux_shared_lib_paths=[str(path) for path in fio_bundle["shared_libs"]],
                    command_text=displayed_command,
                    upload_bundle=not fio_bundle_uploaded,
                    upload_timeout_seconds=1200,
                    timeout_seconds=900,
                    cgroup_cpu_cores=cgroup_cpu_cores,
                    cgroup_memory_mib=cgroup_memory_mib,
                    max_polls=360,
                    poll_interval_seconds=2,
                    running_marker_prefix=f"__{display_name.upper()}_RUNNING__",
                    live_output_lines=live_output_lines,
                    on_update=lambda status, lines: (
                        status_placeholder.write(
                            f"Command status: `{status}` | Refresh every 2s"
                        ),
                        log_placeholder.code("\n".join(lines[-4000:]), language="bash"),
                    ),
                )
                fio_bundle_uploaded = True
        except (ClientError, RuntimeError) as error:
            summary = f"{display_name} start failed: {error}"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": fio_test_time,
                    "status": "error",
                    "command_id": None,
                    "test_type": test_type,
                    "metric_value": None,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": "",
                    "error_message": summary,
                }
            )
            return

        fio_command_id = fio_run_result.command_id
        _append_live_output_block(
            live_output_lines,
            log_placeholder,
            f"[{display_name}] command started: {fio_command_id}",
        )
        _set_command_id(display_name, fio_command_id)
        fio_status = str(fio_run_result.status)
        fio_poll_error = fio_run_result.poll_error
        fio_output = str(fio_run_result.output or "")
        fio_parsed = fio_run_result.parsed
        fio_bw_mib_s = fio_parsed.get("bw_mib_s")
        fio_iops = fio_parsed.get("iops")
        fio_avg_latency_ms = fio_parsed.get("avg_latency_ms")
        fio_p95_latency_ms = fio_parsed.get("p95_latency_ms")
        fio_p99_latency_ms = fio_parsed.get("p99_latency_ms")
        fio_disk_util_pct = fio_parsed.get("disk_util_pct")
        fio_cpu_usr_pct = fio_parsed.get("cpu_usr_pct")
        fio_cpu_sys_pct = fio_parsed.get("cpu_sys_pct")
        fio_cpu_total_pct = fio_parsed.get("cpu_total_pct")
        fio_exit_code = fio_parsed.get("exit_code")
        if test_type == TEST_TYPE_SEQWRITE:
            suite_db_record["seqwrite_bw_mib_s"] = fio_bw_mib_s
            suite_db_record["seqwrite_iops"] = fio_iops
            suite_db_record["seqwrite_disk_util_pct"] = fio_disk_util_pct
            suite_db_record["seqwrite_cpu_usr_pct"] = fio_cpu_usr_pct
            suite_db_record["seqwrite_cpu_sys_pct"] = fio_cpu_sys_pct
            suite_db_record["seqwrite_cpu_total_pct"] = fio_cpu_total_pct
        elif test_type == TEST_TYPE_RANDWRITE:
            suite_db_record["randwrite_bw_mib_s"] = fio_bw_mib_s
            suite_db_record["randwrite_iops"] = fio_iops
            suite_db_record["randwrite_avg_latency_ms"] = fio_avg_latency_ms
            suite_db_record["randwrite_p95_latency_ms"] = fio_p95_latency_ms
            suite_db_record["randwrite_p99_latency_ms"] = fio_p99_latency_ms
            suite_db_record["randwrite_disk_util_pct"] = fio_disk_util_pct
            suite_db_record["randwrite_cpu_usr_pct"] = fio_cpu_usr_pct
            suite_db_record["randwrite_cpu_sys_pct"] = fio_cpu_sys_pct
            suite_db_record["randwrite_cpu_total_pct"] = fio_cpu_total_pct

        if fio_poll_error:
            summary = f"{display_name} poll failed: {fio_poll_error}"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": fio_test_time,
                    "status": "error",
                    "command_id": fio_command_id,
                    "test_type": test_type,
                    "metric_value": fio_bw_mib_s,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": fio_output,
                    "error_message": summary,
                }
            )
            return

        if fio_status != "Success":
            summary = f"{display_name} command ended with status: {fio_status}"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": fio_test_time,
                    "status": "error",
                    "command_id": fio_command_id,
                    "test_type": test_type,
                    "metric_value": fio_bw_mib_s,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": fio_output,
                    "error_message": summary,
                }
            )
            return

        if fio_bw_mib_s is None:
            summary = f"{display_name} bandwidth not found in output"
            _record_summary(display_name, summary, success=False)
            _insert_suite_step_result(
                {
                    "instance_id": instance_id,
                    "instance_type": instance.get("InstanceType"),
                    "vcpu": specs.get("vCPU"),
                    "memory_gib": specs.get("MemoryGiB"),
                    "test_time": fio_test_time,
                    "status": "error",
                    "command_id": fio_command_id,
                    "test_type": test_type,
                    "metric_value": None,
                    "metric_unit": "MiB/s",
                    "coremark_score": None,
                    "iterations_per_sec": None,
                    "result_summary": summary,
                    "raw_output": fio_output,
                    "error_message": summary,
                }
            )
            return

        bw_str = _round_to_int_string(fio_bw_mib_s)
        iops_str = _round_to_int_string(fio_iops) if fio_iops is not None else "N/A"
        summary_parts = [
            f"bw={bw_str} MiB/s",
            f"iops={iops_str}",
        ]
        if test_type == TEST_TYPE_SEQWRITE:
            disk_util_str = _format_percent_metric(fio_disk_util_pct, fallback="N/A")
            cpu_total_str = _format_percent_metric(fio_cpu_total_pct, fallback="N/A")
            cpu_usr_str = _format_percent_metric(fio_cpu_usr_pct, fallback="N/A")
            cpu_sys_str = _format_percent_metric(fio_cpu_sys_pct, fallback="N/A")
            summary_parts.append(f"disk_util={disk_util_str}")
            summary_parts.append(f"cpu={cpu_total_str} (usr={cpu_usr_str}, sys={cpu_sys_str})")
        elif test_type == TEST_TYPE_RANDWRITE:
            avg_latency_str = _format_test_metric(fio_avg_latency_ms, "ms", fallback="N/A")
            p95_latency_str = _format_test_metric(fio_p95_latency_ms, "ms", fallback="N/A")
            p99_latency_str = _format_test_metric(fio_p99_latency_ms, "ms", fallback="N/A")
            disk_util_str = _format_percent_metric(fio_disk_util_pct, fallback="N/A")
            cpu_total_str = _format_percent_metric(fio_cpu_total_pct, fallback="N/A")
            cpu_usr_str = _format_percent_metric(fio_cpu_usr_pct, fallback="N/A")
            cpu_sys_str = _format_percent_metric(fio_cpu_sys_pct, fallback="N/A")
            summary_parts.append(f"avg_lat={avg_latency_str}")
            summary_parts.append(f"p95={p95_latency_str}")
            summary_parts.append(f"p99={p99_latency_str}")
            summary_parts.append(f"disk_util={disk_util_str}")
            summary_parts.append(f"cpu={cpu_total_str} (usr={cpu_usr_str}, sys={cpu_sys_str})")
        summary_parts.append(f"exit_code={fio_exit_code}")
        summary = ", ".join(summary_parts)
        _record_summary(display_name, summary, success=True)
        _insert_suite_step_result(
            {
                "instance_id": instance_id,
                "instance_type": instance.get("InstanceType"),
                "vcpu": specs.get("vCPU"),
                "memory_gib": specs.get("MemoryGiB"),
                "test_time": fio_test_time,
                "status": "success",
                "command_id": fio_command_id,
                "test_type": test_type,
                "metric_value": fio_bw_mib_s,
                "metric_unit": "MiB/s",
                "coremark_score": None,
                "iterations_per_sec": None,
                "result_summary": summary,
                "raw_output": fio_output,
                "error_message": None,
            }
        )

    # 2) SeqWrite
    _run_fio_case(TEST_TYPE_SEQWRITE, "SeqWrite", FIO_SEQWRITE_COMMAND)
    # 3) RandWrite
    _run_fio_case(TEST_TYPE_RANDWRITE, "RandWrite", FIO_RANDWRITE_COMMAND)

    passed = len([item for item in suite_results if "PASS" in item])
    total = len(suite_results)
    _append_live_output_block(
        live_output_lines,
        log_placeholder,
        f"[Suite] Completed: {passed}/{total} passed",
    )
    st.markdown("**CPU/IO Test Summary**")
    st.table(
        [
            suite_summary_map["CPU"],
            suite_summary_map["SeqWrite"],
            suite_summary_map["RandWrite"],
        ]
    )
    suite_db_record["status"] = "success" if (passed == total and total > 0) else "error"
    suite_db_record["result_summary"] = " | ".join(suite_results) or "CPU/IO Test completed"
    suite_db_record["raw_output"] = "\n".join(live_output_lines[-4000:])
    suite_db_record["error_message"] = (
        " | ".join(suite_failed_lines) if suite_failed_lines else None
    )
    suite_summary_row_id = _insert_test_result(suite_db_record)
    if suite_summary_row_id is not None:
        _delete_test_results_by_ids(suite_step_result_ids)

    if passed == total and total > 0:
        st.success("CPU/IO Test completed successfully.")
    else:
        st.warning("CPU/IO Test completed with failures. See live output above.")


def _render_detail_page(clients: Dict[str, object], region: str, instance_id: str) -> None:
    st.subheader("Instance Detail")
    header_col1, header_col2 = st.columns([1, 1])
    if header_col1.button("Back to EC2 List", use_container_width=True):
        _navigate_to(VIEW_LIST)
    refresh_detail = header_col2.button("Refresh detail", use_container_width=True)

    if refresh_detail:
        st.cache_data.clear()

    if not instance_id:
        st.warning("Missing `instance_id` in query parameters.")
        return

    try:
        with _data_loading_scope():
            instance = get_instance(clients["ec2"], instance_id)
            specs = get_instance_type_specs(clients["ec2"], str(instance.get("InstanceType", "")))
    except ClientError as error:
        st.error(f"Failed to load instance detail: {error}")
        return
    except RuntimeError as error:
        st.error(str(error))
        return

    st.markdown(f"**Instance ID:** `{instance['InstanceId']}`")
    st.markdown(f"**Name:** `{instance.get('Name') or 'N/A'}`")

    summary_cols = st.columns(4)
    summary_cols[0].metric("State", str(instance.get("State", "")))
    summary_cols[1].metric("Type", str(instance.get("InstanceType", "")))
    summary_cols[2].metric("vCPU", str(specs.get("vCPU") or "N/A"))
    summary_cols[3].metric("Memory (GiB)", str(specs.get("MemoryGiB") or "N/A"))

    network_cols = st.columns(2)
    network_cols[0].metric("Private IP", str(instance.get("PrivateIpAddress", "")))
    network_cols[1].metric("Public IP", str(instance.get("PublicIpAddress", "") or "None"))

    st.markdown("### Login Commands")
    ssm_command = f"aws ssm start-session --region {region} --target {instance['InstanceId']}"
    st.code(ssm_command, language="bash")

    key_name = instance.get("KeyName")
    private_ip = instance.get("PrivateIpAddress")
    if key_name and private_ip:
        ssh_command = f"ssh -i /absolute/path/to/{key_name}.pem ec2-user@{private_ip}"
        st.code(ssh_command, language="bash")

    st.markdown("### Performance Test")
    st.caption(
        "Run CPU + SeqWrite + RandWrite benchmarks in sequence. FIO test files are deleted after each test."
    )
    st.write(f"Local CoreMark bundle root: `{COREMARK_LINUX_BUNDLE_DIR}`")
    st.write(f"Local fio bundle root: `{FIO_LINUX_BUNDLE_DIR}`")
    cpu_test_threads = int(
        st.number_input(
            "threads for CPU test",
            min_value=1,
            value=16,
            step=1,
            help="Number of parallel CoreMark workers used in benchmark CPU test.",
        )
    )

    st.markdown("#### cgroup v2 Resource Limits")
    use_cgroup_limit = st.checkbox(
        "Use cgroup to limit resources",
        value=False,
        help="When enabled, benchmark runs with cgroup v2 hard limits; otherwise it uses full machine resources.",
    )

    limit_cpu_cores: Optional[int] = None
    limit_memory_gib: Optional[float] = None
    if use_cgroup_limit:
        cgroup_cols = st.columns(2)
        limit_cpu_cores = int(
            cgroup_cols[0].number_input(
                "Limit CPU cores",
                min_value=1,
                max_value=max(int(specs.get("vCPU") or 1), 1),
                value=min(DEFAULT_CGROUP_LIMIT_CPU_CORES, max(int(specs.get("vCPU") or 1), 1)),
                step=1,
                help="Hard CPU quota by cgroup v2 cpu.max. All benchmark child processes are included.",
            )
        )
        max_memory_gib = max(float(specs.get("MemoryGiB") or 1.0), 1.0)
        default_memory_gib = min(DEFAULT_CGROUP_LIMIT_MEMORY_GIB, max_memory_gib)
        limit_memory_gib = float(
            cgroup_cols[1].number_input(
                "Limit memory (GiB)",
                min_value=0.5,
                max_value=max_memory_gib,
                value=float(default_memory_gib),
                step=0.5,
                help="Hard memory limit by cgroup v2 memory.max. OOM in benchmark process is treated as failure.",
            )
        )
        st.caption(
            f"Current profile: cgroup-v2 cpu={int(limit_cpu_cores)} mem={float(limit_memory_gib):g}GiB"
        )
    else:
        st.caption("Current profile: unlimited (no cgroup limit)")

    if st.button("CPU/IO Test", type="primary"):
        _run_cpu_io_test_suite(
            clients,
            instance,
            specs,
            cpu_threads=cpu_test_threads,
            cgroup_cpu_cores=limit_cpu_cores,
            cgroup_memory_gib=limit_memory_gib,
        )

    st.markdown("### Instance Details")
    st.json(instance)

    st.markdown("### Delete Instance")
    st.caption("Terminate this instance from the detail page.")
    confirm = st.checkbox(
        f"I understand terminating `{instance['InstanceId']}` is irreversible.",
        value=False,
    )
    delete_clicked = st.button("Terminate this instance")

    if delete_clicked:
        if not confirm:
            st.warning("Please confirm before terminating this instance.")
            return
        try:
            with _data_loading_scope():
                result = terminate_ec2_instances(clients["ec2"], [instance["InstanceId"]])
                _refresh_instance_cache(clients["ec2"], include_terminated=False)
            st.success("Terminate request submitted.")
            st.json(result)
        except ClientError as error:
            st.error(f"Terminate failed: {error}")


def _render_settings_page() -> None:
    st.subheader("Settings")
    st.caption(
        "Fill in AWS env file path and optional region override, then click `Confirm` to cache credentials."
    )

    persisted = _get_persisted_settings()
    default_env_path = (
        st.session_state.get("aws_env_path")
        or persisted.get("aws_env_path")
        or _get_default_env_file_path()
    )
    default_region_override = (
        st.session_state.get("region_override")
        or persisted.get("region_override")
        or ""
    )

    with st.form("settings_form"):
        env_file_path = st.text_input(
            "AWS env file path",
            value=default_env_path,
            help="Path to file containing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN.",
        )
        region_override = st.text_input(
            "Region override (optional)",
            value=default_region_override,
            help="If empty, use AWS_REGION/AWS_DEFAULT_REGION from the env file.",
        )
        confirm = st.form_submit_button("Confirm", type="primary")

    if confirm:
        try:
            with _data_loading_scope():
                config = load_aws_env_config(env_file_path)
                final_region = (region_override or config.region or "").strip()
                if not final_region:
                    raise AwsConfigError(
                        "Region is required. Set AWS_REGION in env file or enter region override."
                    )

                session = build_boto3_session(config, region_override=final_region)
                identity = session.client("sts", region_name=final_region).get_caller_identity()

            st.session_state["aws_env_path"] = env_file_path
            st.session_state["region_override"] = region_override
            st.session_state["aws_config"] = config.to_dict()
            st.session_state["region"] = final_region
            st.session_state["identity"] = identity
            _persist_confirmed_settings(env_file_path, region_override)
            st.success("AWS env settings confirmed and cached.")
        except (AwsConfigError, ClientError, OSError, RuntimeError) as error:
            st.error(f"Failed to confirm settings: {error}")

    state_data = _load_session_from_state()
    if state_data:
        identity = st.session_state.get("identity", {})
        account_id = identity.get("Account", "unknown")
        arn = identity.get("Arn", "unknown")
        region = st.session_state.get("region", "unknown")
        masked_key = mask_access_key(st.session_state["aws_config"]["access_key_id"])
        st.markdown("### Cached AWS Context")
        st.write(f"Account: `{account_id}`")
        st.write(f"Region: `{region}`")
        st.write(f"AWS Access Key: `{masked_key}`")
        st.write(f"ARN: `{arn}`")


def _render_test_results_page() -> None:
    st.subheader("Test Results")
    refresh_clicked = st.button("Refresh results", use_container_width=True)
    if refresh_clicked:
        st.cache_data.clear()

    with _data_loading_scope():
        rows = _load_test_results(limit=500)

    if not rows:
        st.info("No performance test results yet.")
        return

    st.caption(f"Total: {len(rows)} record(s)")
    table_rows: List[Dict[str, object]] = []
    for item in rows:
        cpu_iterations_per_sec = item.get("cpu_iterations_per_sec")
        if cpu_iterations_per_sec is None:
            cpu_iterations_per_sec = _extract_cpu_iterations_per_sec(
                item.get("result_summary")
            )

        seq_bw = item.get("seqwrite_bw_mib_s")
        seq_iops = item.get("seqwrite_iops")
        seq_disk_util = item.get("seqwrite_disk_util_pct")
        seq_cpu_usr = item.get("seqwrite_cpu_usr_pct")
        seq_cpu_sys = item.get("seqwrite_cpu_sys_pct")
        seq_cpu_total = item.get("seqwrite_cpu_total_pct")
        if seq_cpu_total is None and (seq_cpu_usr is not None or seq_cpu_sys is not None):
            try:
                usr_part = float(seq_cpu_usr or 0.0)
                sys_part = float(seq_cpu_sys or 0.0)
                seq_cpu_total = usr_part + sys_part
            except (TypeError, ValueError):
                seq_cpu_total = None

        rand_bw = item.get("randwrite_bw_mib_s")
        rand_iops = item.get("randwrite_iops")
        rand_avg_latency_ms = item.get("randwrite_avg_latency_ms")
        rand_p95_latency_ms = item.get("randwrite_p95_latency_ms")
        rand_p99_latency_ms = item.get("randwrite_p99_latency_ms")
        rand_disk_util = item.get("randwrite_disk_util_pct")
        rand_cpu_usr = item.get("randwrite_cpu_usr_pct")
        rand_cpu_sys = item.get("randwrite_cpu_sys_pct")
        rand_cpu_total = item.get("randwrite_cpu_total_pct")
        if rand_cpu_total is None and (rand_cpu_usr is not None or rand_cpu_sys is not None):
            try:
                usr_part = float(rand_cpu_usr or 0.0)
                sys_part = float(rand_cpu_sys or 0.0)
                rand_cpu_total = usr_part + sys_part
            except (TypeError, ValueError):
                rand_cpu_total = None

        seq_cpu_usr_str = _format_percent_metric(seq_cpu_usr, fallback="")
        seq_cpu_sys_str = _format_percent_metric(seq_cpu_sys, fallback="")
        seq_cpu_breakdown = ""
        if seq_cpu_usr_str or seq_cpu_sys_str:
            seq_cpu_breakdown = f"{seq_cpu_usr_str or 'N/A'} / {seq_cpu_sys_str or 'N/A'}"
        rand_cpu_usr_str = _format_percent_metric(rand_cpu_usr, fallback="")
        rand_cpu_sys_str = _format_percent_metric(rand_cpu_sys, fallback="")
        rand_cpu_breakdown = ""
        if rand_cpu_usr_str or rand_cpu_sys_str:
            rand_cpu_breakdown = f"{rand_cpu_usr_str or 'N/A'} / {rand_cpu_sys_str or 'N/A'}"

        table_rows.append(
            {
                "Instance ID": item.get("instance_id"),
                "Instance Type": item.get("instance_type"),
                "vCPU": _round_to_int_string(item.get("vcpu")),
                "Memory (GiB)": _round_to_int_string(item.get("memory_gib")),
                "Test Time (UTC)": item.get("test_time"),
                "Status": item.get("status"),
                "Resource Profile": item.get("cgroup_profile") or "unlimited",
                "Limit CPU": _round_to_int_string(item.get("cgroup_cpu_cores")),
                "Limit Memory (GiB)": _format_test_metric(item.get("cgroup_memory_gib"), "GiB", fallback=""),
                "CPU Test Threads": _round_to_int_string(item.get("cpu_test_threads")),
                "CPU Iterations/sec": _format_test_metric(
                    cpu_iterations_per_sec, "", fallback=""
                ),
                "SeqWrite BW (MiB/s)": _format_test_metric(seq_bw, "MiB/s", fallback=""),
                "SeqWrite IOPS": _format_test_metric(seq_iops, "", fallback=""),
                "SeqWrite Disk Util (%)": _format_percent_metric(seq_disk_util, fallback=""),
                "SeqWrite CPU Usage (%)": _format_percent_metric(seq_cpu_total, fallback=""),
                "SeqWrite CPU usr/sys (%)": seq_cpu_breakdown,
                "RandWrite IOPS": _format_test_metric(rand_iops, "", fallback=""),
                "RandWrite Throughput (MiB/s)": _format_test_metric(rand_bw, "MiB/s", fallback=""),
                "RandWrite Avg Latency (ms)": _format_test_metric(rand_avg_latency_ms, "ms", fallback=""),
                "RandWrite p95 Latency (ms)": _format_test_metric(rand_p95_latency_ms, "ms", fallback=""),
                "RandWrite p99 Latency (ms)": _format_test_metric(rand_p99_latency_ms, "ms", fallback=""),
                "RandWrite Disk Util (%)": _format_percent_metric(rand_disk_util, fallback=""),
                "RandWrite CPU Usage (%)": _format_percent_metric(rand_cpu_total, fallback=""),
                "RandWrite CPU usr/sys (%)": rand_cpu_breakdown,
            }
        )
    st.dataframe(table_rows, use_container_width=True)


def _get_clients_for_ops() -> Optional[Dict[str, object]]:
    _hydrate_session_from_persisted_settings()
    state_data = _load_session_from_state()
    if not state_data:
        return None
    try:
        with _data_loading_scope():
            return _build_clients()
    except Exception as error:  # pylint: disable=broad-except
        st.error(f"Unable to initialize AWS clients from cached settings: {error}")
        return None


def _render_sidebar_links(current_view: str) -> None:
    st.subheader("Navigation")
    ordered_views = [VIEW_LIST, VIEW_CREATE, VIEW_SETTINGS, VIEW_RESULTS]
    for view in ordered_views:
        label = VIEW_TO_LABEL[view]
        if current_view == view:
            st.markdown(f"**{label}**")
        else:
            st.markdown(f"[{label}](?view={view})")


_init_test_results_db()
_hydrate_session_from_persisted_settings()
current_view = _query_param_value("view", VIEW_LIST)
if current_view not in VIEW_TO_LABEL:
    current_view = VIEW_LIST

instance_id_param = _query_param_value("instance_id", "").strip()
selected_view = current_view

with st.sidebar:
    _render_sidebar_links(current_view)


if selected_view == VIEW_SETTINGS:
    _render_settings_page()
else:
    _render_connection_status()
    clients = _get_clients_for_ops()
    if not clients:
        st.warning("No cached AWS settings found. Please go to `Settings` and click `Confirm`.")
        if st.button("Open Settings", type="primary"):
            _navigate_to(VIEW_SETTINGS)
        st.stop()

    region = clients["region"]
    if selected_view == VIEW_LIST:
        _render_list_page(clients, instance_id_param)
    elif selected_view == VIEW_CREATE:
        _render_create_page(clients, region)
    elif selected_view == VIEW_RESULTS:
        _render_test_results_page()
