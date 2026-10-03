"""SAS %compare (PROC COMPARE of two datasets or two libraries) translated to Airflow.

extract (detect DATA vs LIB, profile inputs) >> compare >> validate (row reconciliation + checksums)

A "library" is a directory of CSV files; members are matched by upper-cased file stem.
"""
import os

import pandas as pd
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.python import PythonOperator

from sas_migration.dag_utils import (
    DEFAULT_ARGS, START_DATE, finish_validation, read_json, resolve_params, run_dir, sample_path, write_json,
)
from sas_migration.io import read_dataset, write_dataset
from sas_migration.reconcile import reconcile_compare
from sas_migration.sas_compat import parse_bool
from sas_migration.validation import Reconciliation, profile
from sas_migration.transforms import compare as cmp

DAG_NAME = "sas_compare"


def _kind(path):
    if os.path.isdir(path):
        return "LIB"
    if os.path.isfile(path):
        return "DATA"
    raise FileNotFoundError("Either %s is not allocated or does not exist" % path)


def _pairs(params):
    """[(memname, base_path, comp_path, by)] for the datasets to compare."""
    base, comp = params["base"], params["comp"]
    if _kind(base) == "DATA":
        memname = os.path.splitext(os.path.basename(base))[0].upper()
        return [(memname, base, comp, cmp.keys_for(params.get("by"), memname))]
    base_members, comp_members = cmp.list_library(base), cmp.list_library(comp)
    report = cmp.library_report(base, comp, params.get("filter"))
    matched = report[report["matched"] == "MATCHED"]["base_memname"].tolist()
    return [(m, base_members[m], comp_members[m], cmp.keys_for(params.get("by"), m)) for m in matched]


def _extract(**context):
    params = resolve_params(context)
    base_kind, comp_kind = _kind(params["base"]), _kind(params["comp"])
    if base_kind != comp_kind:
        raise AirflowFailException("Both the BASE parameter (%s) and COMPARE parameter (%s) must be the same type"
                                   % (base_kind, comp_kind))
    profiles = []
    for memname, base_path, comp_path, _ in _pairs(params):
        profiles.append({"memname": memname,
                         "base": dict(profile(read_dataset(base_path), memname), path=base_path),
                         "compare": dict(profile(read_dataset(comp_path), memname), path=comp_path)})
    info = {"type": base_kind, "members": profiles}
    write_json(info, os.path.join(run_dir(context, DAG_NAME), "input_profiles.json"))
    return info


def _compare(**context):
    params = resolve_params(context)
    out_dir = run_dir(context, DAG_NAME)
    results = []
    if _kind(params["base"]) == "LIB":
        report = cmp.library_report(params["base"], params["comp"], params.get("filter"))
        write_dataset(report, os.path.join(out_dir, "library_report.csv"))
    for memname, base_path, comp_path, by in _pairs(params):
        result = cmp.compare_datasets(
            read_dataset(base_path), read_dataset(comp_path), by=by, method=params.get("method", "EXACT"),
            criterion=params.get("criterion", 0.000001), maxprint=params.get("maxprint"),
            base_name=base_path, comp_name=comp_path,
        )
        member_dir = os.path.join(out_dir, memname)
        if not os.path.isdir(member_dir):
            os.makedirs(member_dir)
        checkobs = params.get("checkobs", 0)
        write_dataset(result["check"], os.path.join(member_dir, "check.csv"))
        write_dataset(result["differences"], os.path.join(member_dir, "differences.csv"))
        write_dataset(result["printed_differences"], os.path.join(member_dir, "differences_printed.csv"))
        write_json(result["summary"], os.path.join(member_dir, "summary.json"))
        if checkobs in (None, "", "max", "MAX") or int(checkobs) > 0:
            limit = None if checkobs in (None, "", "max", "MAX") else int(checkobs)
            print(result["check"].head(limit).to_string() if limit else result["check"].to_string())
        results.append({"memname": memname, "dir": member_dir, "base": base_path, "compare": comp_path,
                        "identical": result["summary"]["identical"]})
    return {"members": results}


def _validate(**context):
    params = resolve_params(context)
    ti = context["ti"]
    profiles = ti.xcom_pull(task_ids="extract")
    outcome = ti.xcom_pull(task_ids="compare")
    rec = Reconciliation("compare reconciliation")
    rec.expect_equal("member_count", len(profiles["members"]), len(outcome["members"]))
    for member in outcome["members"]:
        m = member["memname"]
        base, comp = read_dataset(member["base"]), read_dataset(member["compare"])
        summary = read_json(os.path.join(member["dir"], "summary.json"))
        check = read_dataset(os.path.join(member["dir"], "check.csv"))
        diffs = read_dataset(os.path.join(member["dir"], "differences.csv"))
        prof = [p for p in profiles["members"] if p["memname"] == m][0]
        rec.expect_equal(m + ".base_checksum_unchanged", prof["base"]["checksum"], profile(base, m)["checksum"])
        rec.expect_equal(m + ".compare_checksum_unchanged", prof["compare"]["checksum"],
                         profile(comp, m)["checksum"])
        reconcile_compare(base, comp, summary, check, diffs, rec=rec, prefix=m + ".")
    if profiles["type"] == "LIB":
        report = read_dataset(os.path.join(run_dir(context, DAG_NAME), "library_report.csv"))
        for _, row in report[report["matched"] == "MATCHED"].iterrows():
            p = [x for x in profiles["members"] if x["memname"] == row["base_memname"]][0]
            rec.expect_equal(row["base_memname"] + ".library_report_base_nobs", p["base"]["row_count"],
                             int(row["base_nobs"]))
            rec.expect_equal(row["base_memname"] + ".library_report_comp_nobs", p["compare"]["row_count"],
                             int(row["comp_nobs"]))
    report = finish_validation(rec, run_dir(context, DAG_NAME), {"members": outcome["members"]})
    if parse_bool(params.get("fail_on_difference", False)) and not all(m["identical"] for m in outcome["members"]):
        raise AirflowFailException("Datasets differ: %s" % [m["memname"] for m in outcome["members"]
                                                            if not m["identical"]])
    return report


with DAG(
    dag_id=DAG_NAME,
    description="Port of SAS macro %compare: PROC COMPARE of datasets or libraries",
    default_args=DEFAULT_ARGS,
    start_date=START_DATE,
    schedule_interval=None,
    catchup=False,
    tags=["sas-migration", "macro:compare"],
    params={
        "base": sample_path("compare", "base"),
        "comp": sample_path("compare", "comp"),
        # explicit variable list, or a keys mapping {memname: "var1 var2"} like the macro's keys dataset
        "by": {"CUST_ACCOUNTS": "ACCOUNT_ID", "LOAN_DETAILS": "ACCOUNT_ID", "PAYMENT_HISTORY": "ACCOUNT_ID"},
        "filter": None,
        "checkobs": 0,
        "maxprint": "50,1000",
        "criterion": 0.000001,
        "method": "EXACT",
        "fail_on_difference": False,
    },
    doc_md=__doc__,
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    compare = PythonOperator(task_id="compare", python_callable=_compare)
    validate = PythonOperator(task_id="validate", python_callable=_validate)

    extract >> compare >> validate
