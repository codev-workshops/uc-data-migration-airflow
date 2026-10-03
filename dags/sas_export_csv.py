"""SAS %export_csv (-> %export_dlm dbms=csv) translated to Airflow.

extract (snapshot + profile) >> export_csv >> validate (re-read file, rows + checksum)
"""
import os

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator

from sas_migration.dag_utils import (
    DEFAULT_ARGS, START_DATE, DATA_DIR, finish_validation, load_staged, resolve_params, run_dir, sample_path,
    stage_dataset,
)
from sas_migration.io import read_dataset_as_text
from sas_migration.reconcile import reconcile_export
from sas_migration.sas_compat import parse_bool
from sas_migration.transforms.export_csv import OutputExistsWarning, export_csv

DAG_NAME = "sas_export_csv"


def _extract(**context):
    params = resolve_params(context)
    return stage_dataset(params["data"], run_dir(context, DAG_NAME))


def _export(**context):
    params = resolve_params(context)
    staged = context["ti"].xcom_pull(task_ids="extract")
    df = load_staged(staged)
    path = params.get("path") or os.path.join(DATA_DIR, "output", DAG_NAME)
    if not os.path.splitext(path)[1] and not os.path.isdir(path):
        os.makedirs(path)
    dataset_name = params.get("dataset_name") or os.path.splitext(os.path.basename(params["data"]))[0]
    try:
        return export_csv(
            df, path, dataset_name=dataset_name, where=params.get("where"), replace=params.get("replace"),
            label=params.get("label"), header=params.get("header"), labels=params.get("labels"),
            lrecl=params.get("lrecl", 32767),
        )
    except OutputExistsWarning as exc:
        # SAS: WARNING + SYSCC=4, no output created
        raise AirflowSkipException(str(exc))


def _validate(**context):
    params = resolve_params(context)
    ti = context["ti"]
    staged = ti.xcom_pull(task_ids="extract")
    result = ti.xcom_pull(task_ids="export_csv")
    header = parse_bool(params.get("header"), "HEADER")
    exported = read_dataset_as_text(result["path"], header=header)
    rec = reconcile_export(load_staged(staged), exported, where=params.get("where"), header=header)
    return finish_validation(rec, run_dir(context, DAG_NAME), {"input": staged, "output": result})


with DAG(
    dag_id=DAG_NAME,
    description="Port of SAS macro %export_csv: dataset -> CSV flat file",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["sas-migration", "macro:export_csv"],
    params={
        "data": sample_path("oracle_dw", "CUST_ACCOUNTS.csv"),
        "dataset_name": "cust_accounts",
        "where": "ACCOUNT_STATUS == 'A'",
        "path": os.path.join(DATA_DIR, "output", DAG_NAME),
        "replace": "Y",
        "label": "N",
        "labels": {},
        "header": "Y",
        "lrecl": 32767,
    },
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    export = PythonOperator(task_id="export_csv", python_callable=_export)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    extract >> export >> validate
