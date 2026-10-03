"""Glue shared by the SAS-migration DAG files."""
import json
import os
from datetime import datetime
from typing import Any, Dict

DEFAULT_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
DATA_DIR = os.environ.get("SAS_MIGRATION_DATA_DIR", DEFAULT_DATA_DIR)
SAMPLE_DIR = os.path.join(DATA_DIR, "sample")

DEFAULT_ARGS = {
    "owner": "data-migration",
    "depends_on_past": False,
    "retries": 0,
}

START_DATE = datetime(2024, 1, 1)


def resolve_params(context):
    # type: (Dict[str, Any]) -> Dict[str, Any]
    """DAG params overlaid with ``dag_run.conf`` (explicit merge; works on Airflow 2.0)."""
    params = dict(context.get("params") or {})
    dag_run = context.get("dag_run")
    if dag_run is not None and getattr(dag_run, "conf", None):
        params.update(dag_run.conf)
    return params


def sample_path(*parts):
    # type: (*str) -> str
    return os.path.join(SAMPLE_DIR, *parts)


def run_dir(context, dag_name):
    # type: (Dict[str, Any], str) -> str
    """Per-run working directory, the analogue of the SAS WORK library."""
    run_id = context["run_id"].replace(":", "_").replace("+", "_")
    path = os.path.join(DATA_DIR, "work", dag_name, run_id)
    if not os.path.isdir(path):
        os.makedirs(path)
    return path


def write_json(obj, path):
    # type: (Any, str) -> str
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(obj, handle, indent=2, sort_keys=True, default=str)
    return path


def read_json(path):
    # type: (str) -> Any
    with open(path, encoding="utf-8") as handle:
        return json.load(handle)


def stage_dataset(source_path, dest_dir, name="input", read_options=None):
    # type: (str, str, str, Dict[str, Any]) -> Dict[str, Any]
    """Extract step: snapshot a source CSV into the run directory and profile it."""
    from sas_migration.io import read_dataset, string_columns_of, write_dataset
    from sas_migration.validation import profile

    df = read_dataset(source_path, **(read_options or {}))
    path = write_dataset(df, os.path.join(dest_dir, name + ".csv"))
    info = profile(df, name)
    info.update({"source": source_path, "path": path, "string_columns": string_columns_of(df)})
    write_json(info, os.path.join(dest_dir, name + ".profile.json"))
    return info


def load_staged(info):
    # type: (Dict[str, Any]) -> Any
    from sas_migration.io import read_dataset

    return read_dataset(info["path"], string_columns=info.get("string_columns"))


def finish_validation(rec, out_dir, extra=None):
    # type: (Any, str, Dict[str, Any]) -> Dict[str, Any]
    """Persist the reconciliation report, then fail the task if any check failed."""
    report = rec.report()
    if extra:
        report.update(extra)
    write_json(report, os.path.join(out_dir, "validation_report.json"))
    return rec.raise_if_failed()
