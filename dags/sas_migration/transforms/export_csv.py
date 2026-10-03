"""%export_csv -> %export_dlm(dbms=csv): write a dataset to a delimited flat file."""
import logging
import os
from typing import Any, Dict, Optional

import pandas as pd

from sas_migration.io import format_value, write_delimited
from sas_migration.sas_compat import RowFilter, apply_where, parse_bool

log = logging.getLogger(__name__)

DBMS_DEFAULTS = {"CSV": (",", ".csv"), "TAB": ("\t", ".tdf"), "DLM": (" ", ".txt")}


class OutputExistsWarning(Exception):
    """File exists and REPLACE=N: SAS writes a WARNING, sets SYSCC=4 and creates no output."""


def resolve_output_path(path, dataset_name, dbms="CSV"):
    # type: (str, str, str) -> str
    """A directory PATH derives <dataset>.csv/.tdf/.txt; a file PATH must have an existing directory."""
    extension = DBMS_DEFAULTS[dbms.upper()][1]
    if os.path.isdir(path):
        member = dataset_name.split(".")[-1]
        return os.path.join(path, member + extension)
    parent = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(parent):
        raise FileNotFoundError("The directory %s does not exist.  No output created" % parent)
    return path


def export_csv(df, path, dataset_name="data", where=None, replace=False, label=False, header=True,
               labels=None, lrecl=32767, dbms="CSV", delimiter=None):
    # type: (pd.DataFrame, str, str, RowFilter, Any, Any, Any, Optional[Dict[str, str]], int, str, Optional[str]) -> Dict[str, Any]
    replace = parse_bool(replace, "REPLACE")
    label = parse_bool(label, "LABEL")
    header = parse_bool(header, "HEADER")
    lrecl = int(lrecl)
    if lrecl <= 0:
        raise ValueError("LRECL must be POSITIVE")
    dbms = dbms.upper()
    if dbms not in DBMS_DEFAULTS:
        raise ValueError("DBMS must be one of CSV TAB DLM")
    dlm = delimiter if delimiter else DBMS_DEFAULTS[dbms][0]

    data = apply_where(df, where)
    target = resolve_output_path(path, dataset_name, dbms)
    if os.path.exists(target) and not replace:
        raise OutputExistsWarning('"%s" already exists. Specify REPLACE to overwrite. No output created.' % target)

    _check_lrecl(data, dlm, lrecl, labels if label else None, header)
    header_labels = labels if label else None
    rows = write_delimited(data, target, delimiter=dlm, header=header, labels=header_labels)
    log.info("Exported %d rows x %d columns to %s", rows, data.shape[1], target)
    return {"path": target, "row_count": rows, "columns": list(data.columns), "header": header, "delimiter": dlm}


def _check_lrecl(df, dlm, lrecl, labels, header):
    # type: (pd.DataFrame, str, int, Optional[Dict[str, str]], bool) -> None
    """SAS would silently FLOWOVER records longer than LRECL; fail loudly instead."""
    widths = []
    if header:
        widths.append(len(dlm.join((labels or {}).get(c, c) for c in df.columns)))
    if len(df):
        lengths = df.apply(lambda col: col.map(lambda v: len(format_value(v)) + 2)).sum(axis=1)
        widths.append(int(lengths.max()) + len(dlm) * max(df.shape[1] - 1, 0))
    if widths and max(widths) > lrecl:
        raise ValueError("Record length %d exceeds LRECL=%d" % (max(widths), lrecl))
