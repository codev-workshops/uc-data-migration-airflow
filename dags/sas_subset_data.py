"""SAS %subset_data translated to Airflow.

extract (snapshot + profile) >> subset_data >> validate (rows + checksum of selected input rows)
"""
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from sas_migration.dag_utils import (
    DEFAULT_ARGS, START_DATE, finish_validation, load_staged, read_json, resolve_params, run_dir, sample_path,
    stage_dataset, write_json,
)
from sas_migration.io import read_dataset, string_columns_of, write_dataset
from sas_migration.reconcile import reconcile_subset
from sas_migration.validation import profile
from sas_migration.transforms.subset_data import subset_data

DAG_NAME = "sas_subset_data"
SUBSET_KEYS = ("where", "if_", "firstobs", "lastobs", "obs", "keep", "drop", "rename", "drop_blank_rows")


def _extract(**context):
    params = resolve_params(context)
    return stage_dataset(params["data"], run_dir(context, DAG_NAME))


def _subset(**context):
    params = resolve_params(context)
    staged = context["ti"].xcom_pull(task_ids="extract")
    args = {k: params[k] for k in SUBSET_KEYS if k in params}
    if "if" in params:
        args["if_"] = params["if"]
    out, positions = subset_data(load_staged(staged), **args)
    out_dir = run_dir(context, DAG_NAME)
    path = write_dataset(out, params.get("out") or os.path.join(out_dir, "output.csv"))
    positions_path = write_json(positions, os.path.join(out_dir, "source_positions.json"))
    info = profile(out, "output")
    info["string_columns"] = string_columns_of(out)
    info.update({"path": path, "positions_path": positions_path})
    return info


def _validate(**context):
    params = resolve_params(context)
    ti = context["ti"]
    staged = ti.xcom_pull(task_ids="extract")
    result = ti.xcom_pull(task_ids="subset_data")
    source = load_staged(staged)
    output = read_dataset(result["path"], string_columns=result["string_columns"])
    rec = reconcile_subset(source, output, read_json(result["positions_path"]), params.get("rename"))
    return finish_validation(rec, run_dir(context, DAG_NAME), {"input": staged, "output": result})


with DAG(
    dag_id=DAG_NAME,
    description="Port of SAS macro %subset_data: row/column subsetting and renaming",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["sas-migration", "macro:subset_data"],
    params={
        "data": sample_path("oracle_dw", "CUST_ACCOUNTS.csv"),
        "out": None,
        "rename": {"CURRENT_BALANCE": "BALANCE", "ACCOUNT_TYPE": "ACCT_TYPE"},
        "where": "ACCOUNT_STATUS == 'A'",
        "if": "BALANCE > 0",
        "firstobs": None,
        "lastobs": None,
        "obs": None,
        "keep": "ACCOUNT_ID CUSTOMER_ID ACCT_TYPE BALANCE CREDIT_LIMIT BRANCH_ID",
        "drop": None,
        "drop_blank_rows": False,
    },
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    subset = PythonOperator(task_id="subset_data", python_callable=_subset)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    extract >> subset >> validate
