"""Behavioural tests built from the usage examples in the SAS macro headers."""
import os

import numpy as np
import pandas as pd
import pytest

from sas_migration import reconcile
from sas_migration.io import read_dataset, read_dataset_as_text
from sas_migration.transforms.compare import compare_datasets, library_report, values_equal
from sas_migration.transforms.dedup_string import dedup_column, dedup_string
from sas_migration.transforms.export_csv import OutputExistsWarning, export_csv
from sas_migration.transforms.subset_data import parse_obs, subset_data
from sas_migration.transforms.transpose import transpose

SAMPLE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "sample")


@pytest.fixture
def sashelp_class():
    """sashelp.class (19 obs)."""
    rows = [
        ("Alfred", "M", 14, 69.0, 112.5), ("Alice", "F", 13, 56.5, 84.0), ("Barbara", "F", 13, 65.3, 98.0),
        ("Carol", "F", 14, 62.8, 102.5), ("Henry", "M", 14, 63.5, 102.5), ("James", "M", 12, 57.3, 83.0),
        ("Jane", "F", 12, 59.8, 84.5), ("Janet", "F", 15, 62.5, 112.5), ("Jeffrey", "M", 13, 62.5, 84.0),
        ("John", "M", 12, 59.0, 99.5), ("Joyce", "F", 11, 51.3, 50.5), ("Judy", "F", 14, 64.3, 90.0),
        ("Louise", "F", 12, 56.3, 77.0), ("Mary", "F", 15, 66.5, 112.0), ("Philip", "M", 16, 72.0, 150.0),
        ("Robert", "M", 12, 64.8, 128.0), ("Ronald", "M", 15, 67.0, 133.0), ("Thomas", "M", 11, 57.5, 85.0),
        ("William", "M", 15, 66.5, 112.0),
    ]
    return pd.DataFrame(rows, columns=["Name", "Sex", "Age", "Height", "Weight"])


# --- dedup_string ---------------------------------------------------------------------------

def test_dedup_string_doc_example():
    assert dedup_string("C A B B A G E 3 2 1 1 2 3") == "C A B G E 3 2 1"


def test_dedup_string_is_case_insensitive_and_keeps_first_spelling():
    assert dedup_string("Mobile mobile  ATM atm Atm POS") == "Mobile ATM POS"


def test_dedup_string_legacy_ignores_dlm():
    # the macro accepts DLM= but scans on blanks: a pipe-delimited string is one token
    assert dedup_string("C|A|B|B|A", dlm="|") == "C|A|B|B|A"
    assert dedup_string("C|A|B|B|A", dlm="|", honor_dlm=True) == "C|A|B"


def test_dedup_string_missing_and_in_place():
    df = pd.DataFrame({"s": ["B B A", np.nan], "k": [1, 2]})
    out = dedup_column(df, "s")
    assert out["s"].iloc[0] == "B A" and pd.isna(out["s"].iloc[1])
    assert reconcile.reconcile_dedup(df, out, "s").passed


def test_dedup_reconciliation_detects_lost_token():
    df = pd.DataFrame({"s": ["A B A C"]})
    bad = pd.DataFrame({"s": ["A B"]})
    assert not reconcile.reconcile_dedup(df, bad, "s").passed


# --- subset_data ----------------------------------------------------------------------------

def test_parse_obs():
    assert parse_obs("1-5 or 11-15 OR 20 - 30") == [(1, 5), (11, 15), (20, 30)]
    assert parse_obs(" 7 ") == [(7, 7)]


def test_subset_data_doc_example_obs_rename_keep(sashelp_class):
    out, pos = subset_data(sashelp_class, obs="1-5 or 11-15 or 20-30", rename="Sex=Gender",
                           keep="Name Age Gender")
    assert len(out) == 10  # only 19 obs exist
    assert list(out.columns) == ["Name", "Gender", "Age"]  # KEEP preserves PDV order
    assert pos == [0, 1, 2, 3, 4, 10, 11, 12, 13, 14]
    assert reconcile.reconcile_subset(sashelp_class, out, pos, "Sex=Gender").passed


def test_subset_data_where_uses_renamed_vars(sashelp_class):
    out, _ = subset_data(sashelp_class, rename="Sex=Gender", where="Gender == 'F'")
    assert len(out) == 9 and set(out["Gender"]) == {"F"}


def test_subset_data_firstobs_counts_where_qualified_rows(sashelp_class):
    out, _ = subset_data(sashelp_class, where="Sex == 'M'", firstobs=2, lastobs=4)
    assert out["Name"].tolist() == ["Henry", "James", "Jeffrey"]


def test_subset_data_obs_list_is_relative_to_where(sashelp_class):
    # _N_ counts data step iterations, i.e. WHERE-qualified rows
    out, _ = subset_data(sashelp_class, where="Sex == 'F'", obs="1-2")
    assert out["Name"].tolist() == ["Alice", "Barbara"]


def test_subset_data_if_and_drop(sashelp_class):
    out, _ = subset_data(sashelp_class, if_="Age >= 15", drop="Height Weight")
    assert list(out.columns) == ["Name", "Sex", "Age"] and len(out) == 5


def test_subset_data_blank_rows_idiom(sashelp_class):
    df = sashelp_class.copy().astype(object)
    df.loc[df["Sex"] == "F", :] = np.nan
    out, _ = subset_data(df, drop_blank_rows=True)
    assert len(out) == 10


def test_subset_data_varlist_ranges():
    df = pd.DataFrame([[1, 2, 3, 4, 5]], columns=["A", "F1", "F2", "F3", "Z"])
    out, _ = subset_data(df, keep="F1-F2 Z")
    assert list(out.columns) == ["F1", "F2", "Z"]
    out, _ = subset_data(df, keep="F1--Z")
    assert list(out.columns) == ["F1", "F2", "F3", "Z"]


def test_subset_reconciliation_detects_tampering(sashelp_class):
    out, pos = subset_data(sashelp_class, where="Age > 13")
    out.loc[0, "Weight"] = 999
    assert not reconcile.reconcile_subset(sashelp_class, out, pos).passed


# --- transpose ------------------------------------------------------------------------------

@pytest.fixture
def vitals():
    return pd.DataFrame({
        "usubjid": ["S2", "S1", "S1", "S2"],
        "visit": [1, 1, 2, 2],
        "diabp": [80, 70, 72, np.nan],
        "pulse": [60, 65, 66, 61],
        "site": ["X", "Y", "Y", "X"],
    })


def test_transpose_by_var_name_col(vitals):
    out = transpose(vitals, by="usubjid visit", var="diabp pulse", name="name", label="", col="measures")
    assert list(out.columns) == ["usubjid", "visit", "name", "measures"]
    assert out[["usubjid", "visit", "name"]].values.tolist()[:2] == [["S1", 1, "diabp"], ["S1", 1, "pulse"]]
    assert len(out) == 8
    assert reconcile.reconcile_transpose(vitals, out, dict(by="usubjid visit", var="diabp pulse", name="name",
                                                           label="", col="measures")).passed


def test_transpose_default_var_is_all_numeric(vitals):
    out = transpose(vitals, by="usubjid")
    assert out["_NAME_"].tolist() == ["visit", "diabp", "pulse", "visit", "diabp", "pulse"]
    assert list(out.columns) == ["usubjid", "_NAME_", "COL1", "COL2"]
    assert "_LABEL_" not in out.columns  # no labels -> no _LABEL_ (SAS behaviour)


def test_transpose_without_by_transposes_everything(vitals):
    out = transpose(vitals, var="pulse", prefix="obs")
    assert list(out.columns) == ["_NAME_", "obs1", "obs2", "obs3", "obs4"]
    assert out.iloc[0, 1:].tolist() == [60, 65, 66, 61]


def test_transpose_id_and_let():
    df = pd.DataFrame({"k": [1, 1, 1], "q": ["2024Q1", "2024Q2", "2024Q2"], "v": [1.0, 2.0, 3.0]})
    with pytest.raises(ValueError):
        transpose(df, by="k", var="v", id="q")
    out = transpose(df, by="k", var="v", id="q", let="Y")
    assert out.loc[0, "_2024Q2"] == 3.0 and out.loc[0, "_2024Q1"] == 1.0
    assert reconcile.reconcile_transpose(df, out, dict(by="k", var="v", id="q", let="Y")).passed


def test_transpose_notsorted_groups_consecutive_runs():
    df = pd.DataFrame({"g": ["b", "b", "a", "b"], "v": [1, 2, 3, 4]})
    out = transpose(df, by="g", var="v", notsorted="Y")
    assert out["g"].tolist() == ["b", "a", "b"]
    with pytest.raises(ValueError):
        transpose(df, by="g", var="v", sort="N")


def test_transpose_param_conflicts(vitals):
    with pytest.raises(ValueError):
        transpose(vitals, by="usubjid", var="pulse", col="x", id="visit")
    with pytest.raises(ValueError):
        transpose(vitals, by="usubjid", var="pulse", idlabel="site")


def test_transpose_copy_and_labels(vitals):
    out = transpose(vitals, by="usubjid", var="pulse", copy="site", labels={"pulse": "Pulse Rate"})
    assert len(out) == 4  # copy keeps one row per input obs
    assert out["_LABEL_"].dropna().tolist() == ["Pulse Rate", "Pulse Rate"]
    assert reconcile.reconcile_transpose(vitals, out, dict(by="usubjid", var="pulse", copy="site",
                                                           labels={"pulse": "Pulse Rate"})).passed


def test_transpose_reconciliation_detects_dropped_value(vitals):
    params = dict(by="usubjid", var="pulse")
    out = transpose(vitals, **params)
    out.loc[0, "COL1"] = np.nan
    assert not reconcile.reconcile_transpose(vitals, out, params).passed


# --- compare --------------------------------------------------------------------------------

def test_compare_doc_example(sashelp_class):
    base = sashelp_class.sort_values("Name")
    comp = base.copy()
    comp.loc[comp["Name"] == "John", "Age"] = 99
    comp = comp.drop(columns=["Sex"])
    result = compare_datasets(base, comp, by="name")
    s = result["summary"]
    assert s["base_only_vars"] == ["Sex"]
    assert s["total_unequal_values"] == 1 and s["unequal_values_by_var"]["Age"] == 1
    assert result["differences"].iloc[0]["base_value"] == 12
    assert not s["identical"]
    assert reconcile.reconcile_compare(base, comp, s, result["check"], result["differences"]).passed


def test_compare_check_lists_unmatched_keys(sashelp_class):
    comp = sashelp_class[sashelp_class["Name"] != "Alice"]
    comp = pd.concat([comp, pd.DataFrame([{"Name": "Zed", "Sex": "M", "Age": 1, "Height": 1.0,
                                           "Weight": 1.0}])], ignore_index=True)
    result = compare_datasets(sashelp_class, comp, by="Name")
    assert result["check"].values.tolist() == [["BASE record", "Alice"], ["COMP record", "Zed"]]
    assert result["summary"]["matched_obs"] == 18


def test_compare_without_by_is_positional(sashelp_class):
    result = compare_datasets(sashelp_class, sashelp_class.iloc[:17])
    assert result["check"]["_OBS_"].tolist() == [18, 19]
    assert result["summary"]["total_unequal_values"] == 0


def test_compare_methods():
    assert values_equal(1.0, 1.0000000001, True, "RELATIVE", 1e-6)
    assert not values_equal(1.0, 1.0000000001, True, "EXACT")
    assert values_equal(100.0, 100.5, True, "ABSOLUTE", 1)
    assert values_equal(200.0, 201.0, True, "PERCENT", 0.5)
    assert not values_equal(200.0, 202.0, True, "PERCENT", 0.5)
    assert values_equal(np.nan, None, True) and not values_equal(np.nan, 0, True)
    assert values_equal("abc  ", "abc", False)


def test_compare_drops_loaddttm_with_by(sashelp_class):
    comp = sashelp_class.assign(LOADDTTM="x")
    assert compare_datasets(sashelp_class, comp, by="Name")["summary"]["identical"]
    assert not compare_datasets(sashelp_class, comp)["summary"]["identical"]


def test_compare_sample_library():
    report = library_report(os.path.join(SAMPLE, "compare", "base"), os.path.join(SAMPLE, "compare", "comp"))
    matched = dict(zip(report["matched"].tolist(), report["base_memname"].tolist()))
    assert "NO MATCH" in matched
    assert report[report["matched"] == "MATCHED"]["base_memname"].tolist() == [
        "CUST_ACCOUNTS", "LOAN_DETAILS", "PAYMENT_HISTORY"]
    filtered = library_report(os.path.join(SAMPLE, "compare", "base"), os.path.join(SAMPLE, "compare", "comp"),
                              filter="cust|loan")
    assert filtered["base_memname"].tolist() == ["CUST_ACCOUNTS", "LOAN_DETAILS"]

    base = read_dataset(os.path.join(SAMPLE, "compare", "base", "CUST_ACCOUNTS.csv"))
    comp = read_dataset(os.path.join(SAMPLE, "compare", "comp", "CUST_ACCOUNTS.csv"))
    exact = compare_datasets(base, comp, by="ACCOUNT_ID")
    fuzzy = compare_datasets(base, comp, by="ACCOUNT_ID", method="RELATIVE", criterion=1e-6)
    assert exact["summary"]["base_only_obs"] == 1 and exact["summary"]["compare_only_obs"] == 1
    assert exact["summary"]["unequal_values_by_var"]["CURRENT_BALANCE"] == 3
    assert fuzzy["summary"]["unequal_values_by_var"]["CURRENT_BALANCE"] == 2
    assert exact["summary"]["unequal_values_by_var"]["ACCOUNT_STATUS"] == 1
    for r in (exact, fuzzy):
        assert reconcile.reconcile_compare(base, comp, r["summary"], r["check"], r["differences"]).passed


# --- export_csv -----------------------------------------------------------------------------

def test_export_csv_directory_path_and_replace(tmp_path, sashelp_class):
    result = export_csv(sashelp_class, str(tmp_path), dataset_name="sashelp.class")
    assert result["path"] == str(tmp_path / "class.csv") and result["row_count"] == 19
    with pytest.raises(OutputExistsWarning):
        export_csv(sashelp_class, str(tmp_path), dataset_name="sashelp.class")
    export_csv(sashelp_class, str(tmp_path), dataset_name="sashelp.class", replace="Y")


def test_export_csv_dsd_quoting_missing_and_labels(tmp_path):
    df = pd.DataFrame({"a": ['x,y', 'say "hi"', None], "b": [1.0, np.nan, 2.5]})
    path = str(tmp_path / "out.csv")
    export_csv(df, path, label="Y", labels={"a": "Label A"})
    assert open(path).read() == 'Label A,b\n"x,y",1\n"say ""hi""",\n,2.5\n'
    exported = read_dataset_as_text(path)
    assert reconcile.reconcile_export(df, exported).passed


def test_export_csv_no_header_where_and_missing_dir(tmp_path, sashelp_class):
    path = str(tmp_path / "f.csv")
    export_csv(sashelp_class, path, where="Sex == 'F'", header="N")
    exported = read_dataset_as_text(path, header=False)
    assert len(exported) == 9
    assert reconcile.reconcile_export(sashelp_class, exported, where="Sex == 'F'", header=False).passed
    with pytest.raises(FileNotFoundError):
        export_csv(sashelp_class, str(tmp_path / "nope" / "f.csv"))


def test_export_csv_lrecl(tmp_path, sashelp_class):
    with pytest.raises(ValueError):
        export_csv(sashelp_class, str(tmp_path / "f.csv"), lrecl=10)


def test_export_reconciliation_detects_truncation(tmp_path, sashelp_class):
    path = str(tmp_path / "f.csv")
    export_csv(sashelp_class, path)
    exported = read_dataset_as_text(path).iloc[:-1]
    assert not reconcile.reconcile_export(sashelp_class, exported).passed
