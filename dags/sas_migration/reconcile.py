"""Input/output reconciliation (row counts + checksums) for each translated macro.

Every function recomputes its expectation from the *input* dataset and compares it with
the *persisted output*, so the validation task is independent of the transform task's
in-memory state.
"""
from typing import Any, Dict, List, Optional

import pandas as pd

from sas_migration.io import format_value, is_missing
from sas_migration.sas_compat import apply_rename, apply_where, optional
from sas_migration.transforms import compare as cmp
from sas_migration.transforms import dedup_string as dd
from sas_migration.transforms import transpose as tp
from sas_migration.validation import Reconciliation, digest_records, frame_checksum


def reconcile_export(source, exported, where=None, header=True):
    # type: (pd.DataFrame, pd.DataFrame, Any, bool) -> Reconciliation
    """``exported`` is the written file read back as text."""
    expected = apply_where(source, where)
    rec = Reconciliation("export_csv reconciliation")
    rec.expect_equal("row_count", int(len(expected)), int(len(exported)))
    rec.expect_equal("column_count", int(expected.shape[1]), int(exported.shape[1]))
    if header:
        rec.expect_true("header_present", len(exported.columns) == expected.shape[1], list(exported.columns))
    rec.expect_equal(
        "ordered_row_checksum",
        digest_records(expected.itertuples(index=False, name=None), ordered=True),
        digest_records(exported.itertuples(index=False, name=None), ordered=True),
    )
    return rec


def reconcile_transpose(source, output, params):
    # type: (pd.DataFrame, pd.DataFrame, Dict[str, Any]) -> Reconciliation
    where = params.get("where")
    spec_args = {k: v for k, v in params.items() if k != "where"}
    spec = tp.TransposeSpec(source, **spec_args)
    rec = Reconciliation("transpose reconciliation")
    rec.expect_equal("row_count", tp.expected_row_count(source, spec, where), int(len(output)))
    expected_cells = tp.input_cells(source, spec, where)
    actual_cells = tp.output_cells(output, spec)
    rec.expect_equal("non_missing_cell_count", len(expected_cells), len(actual_cells))
    rec.expect_equal("cell_checksum", digest_records(expected_cells), digest_records(actual_cells))
    return rec


def reconcile_subset(source, output, positions, rename=None):
    # type: (pd.DataFrame, pd.DataFrame, List[int], Any) -> Reconciliation
    renamed = apply_rename(source.reset_index(drop=True), rename)
    rec = Reconciliation("subset_data reconciliation")
    rec.expect_true("output_rows_le_input_rows", len(output) <= len(source), "%d <= %d" % (len(output), len(source)))
    rec.expect_equal("row_count", len(positions), int(len(output)))
    rec.expect_true("positions_unique_and_ordered", positions == sorted(set(positions)))
    rec.expect_true("columns_subset_of_input", set(output.columns) <= set(renamed.columns), list(output.columns))
    if set(output.columns) <= set(renamed.columns):
        expected = renamed.iloc[positions][list(output.columns)]
        rec.expect_equal("ordered_row_checksum", frame_checksum(expected, ordered=True),
                         frame_checksum(output, ordered=True))
    return rec


def reconcile_compare(base, comp, summary, check, differences, by=None, rec=None, prefix=""):
    # type: (pd.DataFrame, pd.DataFrame, Dict[str, Any], pd.DataFrame, pd.DataFrame, Any, Optional[Reconciliation], str) -> Reconciliation
    rec = rec or Reconciliation("compare reconciliation")
    p = prefix
    rec.expect_equal(p + "base_row_count", int(len(base)), summary["base_obs"])
    rec.expect_equal(p + "compare_row_count", int(len(comp)), summary["compare_obs"])
    rec.expect_equal(p + "base_rows_reconcile", summary["base_obs"],
                     summary["matched_obs"] + summary["base_only_obs"])
    rec.expect_equal(p + "compare_rows_reconcile", summary["compare_obs"],
                     summary["matched_obs"] + summary["compare_only_obs"])
    rec.expect_equal(p + "check_rows", summary["base_only_obs"] + summary["compare_only_obs"], int(len(check)))
    rec.expect_equal(p + "difference_rows", summary["total_unequal_values"], int(len(differences)))
    rec.expect_equal(p + "difference_rows_by_var", summary["total_unequal_values"],
                     int(sum(summary["unequal_values_by_var"].values())))

    # Checksum cross-check on the paired observations over the compared variables.
    by_vars = summary["by"]
    work_base, work_comp = base, comp
    if by_vars:
        work_base, work_comp = cmp._drop_loaddttm(base), cmp._drop_loaddttm(comp)
        work_comp = work_comp.rename(columns={c: b for c in work_comp.columns for b in by_vars
                                              if str(c).upper() == b.upper()})
    work_comp = work_comp.rename(columns={c: v for c in work_comp.columns for v in summary["compared_vars"]
                                          if str(c).upper() == v.upper()})
    paired_base, paired_comp, _ = cmp.align(work_base, work_comp, by_vars)
    cols = by_vars + summary["compared_vars"]
    base_sum = frame_checksum(paired_base, cols, ordered=True)
    comp_sum = frame_checksum(paired_comp, cols, ordered=True)
    rec.expect_equal(p + "matched_row_count", summary["matched_obs"], int(len(paired_base)))
    if base_sum == comp_sum:
        rec.expect_equal(p + "equal_checksums_imply_no_differences", 0, summary["total_unequal_values"])
    elif summary["method"] == "EXACT":
        rec.expect_true(p + "unequal_checksums_imply_differences", summary["total_unequal_values"] > 0,
                        summary["total_unequal_values"])
    rec.checks.append({"check": p + "paired_checksums", "expected": base_sum, "actual": comp_sum, "passed": True})
    return rec


def _token_set_digest(series, dlm, honor):
    # type: (pd.Series, Optional[str], bool) -> str
    return digest_records(
        [(i, " ".join(sorted({t.upper() for t in dd.tokenize(v, dlm, honor)}))) for i, v in enumerate(series)],
        ordered=True,
    )


def reconcile_dedup(source, output, invar, outvar=None, dlm=None, honor_dlm=False):
    # type: (pd.DataFrame, pd.DataFrame, str, Optional[str], Optional[str], bool) -> Reconciliation
    target = optional(outvar) or invar
    rec = Reconciliation("dedup_string reconciliation")
    rec.expect_equal("row_count", int(len(source)), int(len(output)))
    passthrough = [c for c in source.columns if c != target]
    rec.expect_equal("passthrough_columns_checksum", frame_checksum(source, passthrough, ordered=True),
                     frame_checksum(output, passthrough, ordered=True))
    rec.expect_equal("token_set_checksum", _token_set_digest(source[invar], dlm, honor_dlm),
                     _token_set_digest(output[target], dlm, honor_dlm))
    duplicate_rows = 0
    order_violations = 0
    for before, after in zip(source[invar].tolist(), output[target].tolist()):
        tokens = [t.upper() for t in dd.tokenize(after, dlm, honor_dlm)]
        if len(tokens) != len(set(tokens)):
            duplicate_rows += 1
        firsts = []
        for t in dd.tokenize(before, dlm, honor_dlm):
            if t.upper() not in firsts:
                firsts.append(t.upper())
        if tokens != firsts:
            order_violations += 1
    rec.expect_equal("rows_with_duplicate_tokens", 0, duplicate_rows)
    rec.expect_equal("first_occurrence_order_violations", 0, order_violations)
    in_tokens = sum(len(dd.tokenize(v, dlm, honor_dlm)) for v in source[invar])
    out_tokens = sum(len(dd.tokenize(v, dlm, honor_dlm)) for v in output[target])
    rec.checks.append({"check": "tokens_removed", "expected": None, "actual": in_tokens - out_tokens, "passed": True})
    return rec
