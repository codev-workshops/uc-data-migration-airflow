"""SAS %transpose (PROC TRANSPOSE wrapper) translated to Airflow.

extract (snapshot + profile) >> transpose >> validate (expected rows + melted cell checksum)
"""
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from sas_migration.dag_utils import (
    DEFAULT_ARGS, START_DATE, finish_validation, load_staged, resolve_params, run_dir, sample_path,
    stage_dataset,
)
from sas_migration.io import read_dataset, string_columns_of, write_dataset
from sas_migration.reconcile import reconcile_transpose
from sas_migration.validation import profile
from sas_migration.transforms.transpose import transpose

DAG_NAME = "sas_transpose"
TRANSPOSE_KEYS = ("by", "var", "prefix", "sort", "notsorted", "id", "idlabel", "let", "copy", "where",
                  "col", "name", "label", "labels")


def _transpose_args(params):
    return {k: params[k] for k in TRANSPOSE_KEYS if k in params}


def _extract(**context):
    params = resolve_params(context)
    return stage_dataset(params["data"], run_dir(context, DAG_NAME))


def _transpose(**context):
    params = resolve_params(context)
    staged = context["ti"].xcom_pull(task_ids="extract")
    out = transpose(load_staged(staged), **_transpose_args(params))
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
    result = ti.xcom_pull(task_ids="transpose")
    output = read_dataset(result["path"], string_columns=result["string_columns"])
    rec = reconcile_transpose(load_staged(staged), output, _transpose_args(params))
    return finish_validation(rec, run_dir(context, DAG_NAME), {"input": staged, "output": result})


with DAG(
    dag_id=DAG_NAME,
    description="Port of SAS macro %transpose: wide <-> long reshaping per BY group",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["sas-migration", "macro:transpose"],
    params={
        "data": sample_path("oracle_dw", "PAYMENT_HISTORY.csv"),
        "out": None,
        "by": "ACCOUNT_ID",
        "var": "PMT_ONTIME_12MO PMT_LATE_30_12MO PMT_LATE_60_12MO PMT_LATE_90_12MO",
        "name": "METRIC",
        "label": "",
        "col": "VALUE",
        "prefix": None,
        "sort": "Y",
        "notsorted": "N",
        "id": None,
        "idlabel": None,
        "let": "N",
        "copy": None,
        "where": None,
        "labels": {},
    },
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    reshape = PythonOperator(task_id="transpose", python_callable=_transpose)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    extract >> reshape >> validate
