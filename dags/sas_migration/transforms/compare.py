"""%compare -> PROC COMPARE of two datasets, or of two libraries (directories of CSV files)."""
import logging
import os
import re
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import numpy as np
import pandas as pd

from sas_migration.io import format_value, is_missing, read_dataset
from sas_migration.sas_compat import is_numeric, optional, resolve_columns, sas_sort, split_words

log = logging.getLogger(__name__)

METHODS = ("ABSOLUTE", "EXACT", "PERCENT", "RELATIVE")
IN_BASE, IN_COMP = "BASE record", "COMP record"


def parse_maxprint(maxprint):
    # type: (Union[str, Sequence[int], None]) -> Tuple[int, int]
    """MAXPRINT=(per-variable, total) differences to list."""
    if maxprint is None:
        return 50, 1000
    if isinstance(maxprint, str):
        parts = [int(p) for p in re.split(r"[,\s]+", maxprint.strip("() ")) if p]
    else:
        parts = [int(p) for p in maxprint]
    if len(parts) == 1:
        return parts[0], parts[0]
    return parts[0], parts[1]


def values_equal(base, comp, numeric, method="EXACT", criterion=0.000001):
    # type: (Any, Any, bool, str, float) -> bool
    base_missing, comp_missing = is_missing(base), is_missing(comp)
    if base_missing or comp_missing:
        return base_missing and comp_missing
    if not numeric:
        return str(base).rstrip() == str(comp).rstrip()  # SAS ignores trailing blanks
    b, c = float(base), float(comp)
    if method == "EXACT":
        return b == c
    diff = abs(b - c)
    if method == "ABSOLUTE":
        return diff <= criterion
    if method == "RELATIVE":
        denom = (abs(b) + abs(c)) / 2.0
        return diff == 0 if denom == 0 else diff / denom <= criterion
    if method == "PERCENT":
        return diff == 0 if b == 0 else 100.0 * diff / abs(b) <= criterion
    raise ValueError("METHOD must be one of %s" % " ".join(METHODS))


def _drop_loaddttm(df):
    # type: (pd.DataFrame) -> pd.DataFrame
    return df.drop(columns=[c for c in df.columns if str(c).upper() == "LOADDTTM"])


def align(base, comp, by):
    # type: (pd.DataFrame, pd.DataFrame, List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]
    """Pair observations. With BY: sorted match-merge on the keys (duplicates paired
    positionally inside a key, with a warning). Without BY: positional interleave.

    Returns (base_matched, comp_matched, check) where check lists unpaired records.
    """
    if by:
        b = sas_sort(base, by).reset_index(drop=True)
        c = sas_sort(comp, by).reset_index(drop=True)
        for name, frame in (("BASE", b), ("COMPARE", c)):
            if frame.duplicated(by).any():
                log.warning("%s data set has duplicate BY values for %s; duplicates are paired by position",
                            name, by)
        bkey = b[by].apply(lambda s: s.map(format_value))
        ckey = c[by].apply(lambda s: s.map(format_value))
        bkey["__dup"] = bkey.groupby(by).cumcount()
        ckey["__dup"] = ckey.groupby(by).cumcount()
        bkey["__b"] = range(len(b))
        ckey["__c"] = range(len(c))
        merged = bkey.merge(ckey, on=by + ["__dup"], how="outer", indicator=True, sort=False)
        both = merged[merged["_merge"] == "both"].sort_values("__b")
        base_only = b.loc[merged.loc[merged["_merge"] == "left_only", "__b"].astype(int), by]
        comp_only = c.loc[merged.loc[merged["_merge"] == "right_only", "__c"].astype(int), by]
        check = pd.concat([
            base_only.assign(**{"in": IN_BASE}),
            comp_only.assign(**{"in": IN_COMP}),
        ], ignore_index=True)
        check = sas_sort(check, by) if len(check) else check
        check = check[["in"] + by].reset_index(drop=True)
        return (b.loc[both["__b"].astype(int)].reset_index(drop=True),
                c.loc[both["__c"].astype(int)].reset_index(drop=True), check)
    n = min(len(base), len(comp))
    extra_base = pd.DataFrame({"in": IN_BASE, "_OBS_": range(n + 1, len(base) + 1)})
    extra_comp = pd.DataFrame({"in": IN_COMP, "_OBS_": range(n + 1, len(comp) + 1)})
    check = pd.concat([extra_base, extra_comp], ignore_index=True)
    return base.iloc[:n].reset_index(drop=True), comp.iloc[:n].reset_index(drop=True), check


def compare_datasets(base, comp, by=None, method="EXACT", criterion=0.000001, maxprint=None,
                     base_name="BASE", comp_name="COMPARE"):
    # type: (pd.DataFrame, pd.DataFrame, Any, str, float, Any, str, str) -> Dict[str, Any]
    """PROC COMPARE BASE= COMPARE= LISTBASEVAR LISTCOMPVAR WARNING [ID by]."""
    method = (method or "EXACT").upper()
    if method not in METHODS:
        raise ValueError("METHOD must be one of %s" % " ".join(METHODS))
    criterion = float(criterion)
    per_var_max, total_max = parse_maxprint(maxprint)

    by_vars = split_words(by)
    if by_vars:
        base, comp = _drop_loaddttm(base), _drop_loaddttm(comp)
        by_base = resolve_columns(base, by_vars)
        by_comp = resolve_columns(comp, by_vars)
        comp = comp.rename(columns=dict(zip(by_comp, by_base)))
        by_vars = by_base

    upper_comp = {str(c).upper(): c for c in comp.columns}
    common = [c for c in base.columns if str(c).upper() in upper_comp and c not in by_vars]
    comp = comp.rename(columns={upper_comp[str(c).upper()]: c for c in common})
    base_only_vars = [c for c in base.columns if str(c).upper() not in upper_comp]
    comp_only_vars = [c for c in comp.columns if c not in base.columns]
    type_conflicts = [c for c in common if is_numeric(base[c]) != is_numeric(comp[c])]
    compared = [c for c in common if c not in type_conflicts]
    for c in type_conflicts:
        log.warning("Variable %s has conflicting types in %s and %s and is not compared", c, base_name, comp_name)

    b, c, check = align(base, comp, by_vars)

    differences = []  # type: List[Dict[str, Any]]
    unequal_by_var = OrderedDict((v, 0) for v in compared)  # type: OrderedDict
    max_diff = OrderedDict()  # type: OrderedDict
    rows_with_diff = set()
    for v in compared:
        numeric = is_numeric(base[v]) and is_numeric(comp[v])
        for i, (bv, cv) in enumerate(zip(b[v].tolist(), c[v].tolist())):
            if values_equal(bv, cv, numeric, method, criterion):
                continue
            unequal_by_var[v] += 1
            rows_with_diff.add(i)
            rec = OrderedDict()  # type: OrderedDict
            if by_vars:
                for k in by_vars:
                    rec[k] = b.at[i, k]
            else:
                rec["_OBS_"] = i + 1
            rec["variable"] = v
            rec["base_value"] = bv
            rec["compare_value"] = cv
            if numeric and not is_missing(bv) and not is_missing(cv):
                delta = float(cv) - float(bv)
                rec["diff"] = delta
                rec["pct_diff"] = (100.0 * delta / float(bv)) if float(bv) != 0 else np.nan
                max_diff[v] = max(max_diff.get(v, 0.0), abs(delta))
            else:
                rec["diff"] = np.nan
                rec["pct_diff"] = np.nan
            differences.append(rec)

    diff_columns = (by_vars or ["_OBS_"]) + ["variable", "base_value", "compare_value", "diff", "pct_diff"]
    diff_df = pd.DataFrame.from_records(differences, columns=diff_columns) if differences \
        else pd.DataFrame(columns=diff_columns)

    printed = []  # MAXPRINT: what PROC COMPARE would list in its report
    for v in compared:
        subset = diff_df[diff_df["variable"] == v].head(per_var_max)
        printed.append(subset)
    printed_df = pd.concat(printed, ignore_index=True).head(total_max) if printed else diff_df.head(0)

    n_base_only = int((check["in"] == IN_BASE).sum()) if len(check) else 0
    n_comp_only = int((check["in"] == IN_COMP).sum()) if len(check) else 0
    summary = OrderedDict([
        ("base", base_name),
        ("compare", comp_name),
        ("method", method),
        ("criterion", criterion if method != "EXACT" else None),
        ("by", by_vars),
        ("base_obs", int(len(base))),
        ("compare_obs", int(len(comp))),
        ("matched_obs", int(len(b))),
        ("base_only_obs", n_base_only),
        ("compare_only_obs", n_comp_only),
        ("obs_with_unequal_values", len(rows_with_diff)),
        ("total_unequal_values", int(len(diff_df))),
        ("base_only_vars", base_only_vars),
        ("compare_only_vars", comp_only_vars),
        ("type_conflict_vars", type_conflicts),
        ("compared_vars", compared),
        ("unequal_values_by_var", dict(unequal_by_var)),
        ("max_abs_diff_by_var", dict(max_diff)),
    ])
    summary["identical"] = bool(
        n_base_only == 0 and n_comp_only == 0 and not diff_df.shape[0]
        and not base_only_vars and not comp_only_vars and not type_conflicts
    )
    if n_base_only or n_comp_only:
        log.info("There are %d records that are not shared between the %s and %s datasets",
                 n_base_only + n_comp_only, base_name, comp_name)
    return {
        "summary": summary,
        "check": check,
        "differences": diff_df,
        "printed_differences": printed_df,
        "matched_base": b,
        "matched_compare": c,
    }


def list_library(path):
    # type: (str) -> Dict[str, str]
    """Directory of CSV files acts as a SAS library: MEMNAME (upper-cased stem) -> file path."""
    members = OrderedDict()  # type: OrderedDict
    for entry in sorted(os.listdir(path)):
        stem, ext = os.path.splitext(entry)
        if ext.lower() == ".csv":
            members[stem.upper()] = os.path.join(path, entry)
    return members


def library_report(base_dir, comp_dir, filter=None):
    # type: (str, str, Optional[str]) -> pd.DataFrame
    """Equivalent of the dictionary.tables FULL JOIN built by %compare for LIB comparisons."""
    base, comp = list_library(base_dir), list_library(comp_dir)
    rows = []
    for memname in sorted(set(base) | set(comp)):
        in_base, in_comp = memname in base, memname in comp
        # WHERE prxmatch("/filter/io", base.memname): comp-only members have a null base.memname and drop out
        if optional(filter) and not (in_base and re.search(filter, memname, flags=re.IGNORECASE)):
            continue
        base_nobs = len(read_dataset(base[memname])) if in_base else np.nan
        comp_nobs = len(read_dataset(comp[memname])) if in_comp else np.nan
        rows.append(OrderedDict([
            ("in_base", int(in_base)),
            ("in_comp", int(in_comp)),
            ("matched", "MATCHED" if in_base and in_comp else "NO MATCH"),
            ("base_libname", os.path.basename(os.path.normpath(base_dir)).upper()),
            ("base_memname", memname if in_base else np.nan),
            ("base_nobs", base_nobs),
            ("comp_libname", os.path.basename(os.path.normpath(comp_dir)).upper()),
            ("comp_memname", memname if in_comp else np.nan),
            ("comp_nobs", comp_nobs),
            ("obsdiff", "<<<" if base_nobs != comp_nobs else ""),
        ]))
    columns = ["in_base", "in_comp", "matched", "base_libname", "base_memname", "base_nobs",
               "comp_libname", "comp_memname", "comp_nobs", "obsdiff"]
    report = pd.DataFrame.from_records(rows, columns=columns) if rows else pd.DataFrame(columns=columns)
    # ORDER BY calculated matched, base.memname
    return sas_sort(report, ["matched", "base_memname"]).reset_index(drop=True)


def keys_for(by, memname):
    # type: (Any, str) -> List[str]
    """BY may be a variable list or a keys mapping {memname: 'var1 var2'} (the macro's keys dataset)."""
    if isinstance(by, dict):
        lookup = {str(k).upper(): v for k, v in by.items()}
        return split_words(lookup.get(memname.upper()))
    return split_words(by)
