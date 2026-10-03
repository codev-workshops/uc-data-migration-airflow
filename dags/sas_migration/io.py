"""Dataset I/O and SAS-compatible value formatting."""
import csv
import math
import os
from typing import Any, Dict, List, Optional

import pandas as pd


def is_missing(value):
    # type: (Any) -> bool
    if value is None:
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def format_value(value):
    # type: (Any) -> str
    """Render a cell the way the CSV writer and the checksums see it.

    Mirrors SAS list-put output for the cases that matter here: integral numerics
    print without a decimal part (BEST. format), missing values print as an empty
    field, and character values have trailing blanks removed (SAS pads them).
    """
    if is_missing(value):
        return ""
    if isinstance(value, bool):
        return str(int(value))
    if isinstance(value, float):
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        if value.is_integer():
            return str(int(value))
        return repr(value)
    if hasattr(value, "item") and not isinstance(value, str):
        return format_value(value.item())
    return str(value).rstrip()


def read_dataset(path, string_columns=None, **kwargs):
    # type: (str, Optional[List[str]], **Any) -> pd.DataFrame
    """Read a CSV "dataset". Only empty fields are treated as missing (not 'NA', 'NULL', ...).

    ``string_columns`` pins character variables to ``str`` so values such as '007'
    survive the round trip between tasks.
    """
    options = dict(keep_default_na=False, na_values=[""])
    if string_columns:
        options["dtype"] = {c: str for c in string_columns}
    options.update(kwargs)
    return pd.read_csv(path, **options)


def string_columns_of(df):
    # type: (pd.DataFrame) -> List[str]
    return [str(c) for c in df.columns if not pd.api.types.is_numeric_dtype(df[c])]


def read_dataset_as_text(path, header=True):
    # type: (str, bool) -> pd.DataFrame
    return pd.read_csv(
        path,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
        header=0 if header else None,
    )


def write_dataset(df, path):
    # type: (pd.DataFrame, str) -> str
    """Persist an intermediate dataset (header row, comma delimited, SAS value formatting)."""
    write_delimited(df, path, delimiter=",", header=True)
    return path


def write_delimited(df, path, delimiter=",", header=True, labels=None):
    # type: (pd.DataFrame, str, str, bool, Optional[Dict[str, str]]) -> int
    """Write ``df`` like a SAS ``file ... dsd dlm=`` + ``put (_all_)(:)`` data step.

    DSD semantics: fields containing the delimiter or a double quote are quoted,
    embedded quotes are doubled, missing values become empty fields.
    Returns the number of data rows written.
    """
    parent = os.path.dirname(os.path.abspath(path))
    if not os.path.isdir(parent):
        os.makedirs(parent)
    labels = labels or {}
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(
            handle, delimiter=delimiter, quotechar='"', quoting=csv.QUOTE_MINIMAL,
            doublequote=True, lineterminator="\n",
        )
        if header:
            writer.writerow([labels.get(col, col) for col in df.columns])
        for record in df.itertuples(index=False, name=None):
            writer.writerow([format_value(v) for v in record])
            rows += 1
    return rows
