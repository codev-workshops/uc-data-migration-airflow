"""SAS %dedup_string translated to Airflow (applied to a character column of a dataset).

extract (snapshot + profile) >> dedup_string >> validate (rows, passthrough checksum, token-set checksum)
"""
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from sas_migration.dag_utils import (
    DEFAULT_ARGS, START_DATE, finish_validation, load_staged, resolve_params, run_dir, sample_path,
    stage_dataset,
)
from sas_migration.io import read_dataset, string_columns_of, write_dataset
from sas_migration.reconcile import reconcile_dedup
from sas_migration.sas_compat import optional, parse_bool
from sas_migration.validation import profile
from sas_migration.transforms.dedup_string import dedup_column

DAG_NAME = "sas_dedup_string"


def _extract(**context):
    params = resolve_params(context)
    return stage_dataset(params["data"], run_dir(context, DAG_NAME))


def _dedup(**context):
    params = resolve_params(context)
    staged = context["ti"].xcom_pull(task_ids="extract")
    out = dedup_column(load_staged(staged), params["invar"], params.get("outvar"), params.get("dlm"),
                       params.get("honor_dlm", False))
    out_dir = run_dir(context, DAG_NAME)
    path = write_dataset(out, params.get("out") or os.path.join(out_dir, "output.csv"))
    info = profile(out, "output")
    info["string_columns"] = string_columns_of(out)
    info["path"] = path
    return info


def _validate(**context):
    params = resolve_params(context)
    ti = context["ti"]
    staged = ti.xcom_pull(task_ids="extract")
    result = ti.xcom_pull(task_ids="dedup_string")
    target = optional(params.get("outvar")) or params["invar"]
    output = read_dataset(result["path"], string_columns=result["string_columns"])
    rec = reconcile_dedup(load_staged(staged), output, params["invar"], params.get("outvar"), params.get("dlm"),
                          parse_bool(params.get("honor_dlm", False), "HONOR_DLM"))
    return finish_validation(rec, run_dir(context, DAG_NAME), {"input": staged, "output": result})


with DAG(
    dag_id=DAG_NAME,
    description="Port of SAS macro %dedup_string: remove duplicate tokens from a string column",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["sas-migration", "macro:dedup_string"],
    params={
        "data": sample_path("dedup_string", "ACCOUNT_CHANNELS.csv"),
        "out": None,
        "invar": "CHANNEL_HISTORY",
        "outvar": "CHANNELS_USED",
        "dlm": None,
        "honor_dlm": False,
    },
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    dedup = PythonOperator(task_id="dedup_string", python_callable=_dedup)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    extract >> dedup >> validate
