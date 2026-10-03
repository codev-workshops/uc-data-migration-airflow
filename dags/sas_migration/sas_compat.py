"""Small translations of SAS language features used by several macros."""
import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import pandas as pd

TRUE_VALUES = {"1", "ON", "Y", "YES", "T", "TRUE"}
FALSE_VALUES = {"0", "OFF", "N", "NO", "F", "FALSE", ""}

RowFilter = Union[str, Callable[[pd.DataFrame], Any], None]


def parse_bool(value, param="value"):
    # type: (Any, str) -> bool
    """%parmv(..., _val=0 1): 0 1 OFF N NO F FALSE / ON Y YES T TRUE, case-insensitive."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    text = str(value).strip().upper()
    if text in TRUE_VALUES:
        return True
    if text in FALSE_VALUES:
        return False
    raise ValueError("%s must be one of 0 1 OFF N NO F FALSE ON Y YES T TRUE, got %r" % (param, value))


def split_words(value):
    # type: (Union[str, Sequence[str], None]) -> List[str]
    if value is None:
        return []
    if isinstance(value, str):
        return value.split()
    return [str(v) for v in value]


def _lookup(df, name):
    # type: (pd.DataFrame, str) -> str
    """SAS variable names are case-insensitive."""
    if name in df.columns:
        return name
    matches = [c for c in df.columns if str(c).upper() == name.upper()]
    if not matches:
        raise KeyError("Variable %s not found in dataset (columns: %s)" % (name, list(df.columns)))
    return matches[0]


def resolve_columns(df, names):
    # type: (pd.DataFrame, Sequence[str]) -> List[str]
    return [_lookup(df, n) for n in names]


def resolve_varlist(df, spec):
    # type: (pd.DataFrame, Union[str, Sequence[str], None]) -> List[str]
    """Expand a SAS variable list: names, X1-X3 numbered ranges, A--C positional ranges,
    _ALL_, _NUMERIC_, _CHARACTER_."""
    words = split_words(spec)
    cols = list(df.columns)
    result = []  # type: List[str]
    for word in words:
        upper = word.upper()
        if upper == "_ALL_":
            picked = cols
        elif upper == "_NUMERIC_":
            picked = [c for c in cols if is_numeric(df[c])]
        elif upper == "_CHARACTER_":
            picked = [c for c in cols if not is_numeric(df[c])]
        elif "--" in word:
            start, end = word.split("--", 1)
            i, j = cols.index(_lookup(df, start)), cols.index(_lookup(df, end))
            picked = cols[i:j + 1]
        elif re.match(r"^[A-Za-z_]\w*?\d+-[A-Za-z_]\w*?\d+$", word):
            left, right = word.split("-", 1)
            m1, m2 = re.match(r"^(.*?)(\d+)$", left), re.match(r"^(.*?)(\d+)$", right)
            if m1.group(1).upper() != m2.group(1).upper():
                raise ValueError("Invalid numbered range list %s" % word)
            picked = [_lookup(df, "%s%d" % (m1.group(1), n))
                      for n in range(int(m1.group(2)), int(m2.group(2)) + 1)]
        else:
            picked = [_lookup(df, word)]
        for c in picked:
            if c not in result:
                result.append(c)
    return result


def parse_rename(spec):
    # type: (Union[str, Dict[str, str], None]) -> Dict[str, str]
    """'Sex=Gender F1=State' or {'Sex': 'Gender'} -> dict."""
    if not spec:
        return {}
    if isinstance(spec, dict):
        return dict(spec)
    pairs = re.findall(r"(\w+)\s*=\s*(\w+)", spec)
    if not pairs:
        raise ValueError("Invalid rename specification %r" % spec)
    return dict(pairs)


def apply_rename(df, spec):
    # type: (pd.DataFrame, Union[str, Dict[str, str], None]) -> pd.DataFrame
    mapping = parse_rename(spec)
    if not mapping:
        return df
    return df.rename(columns={_lookup(df, old): new for old, new in mapping.items()})


def row_mask(df, condition):
    # type: (pd.DataFrame, RowFilter) -> pd.Series
    """Evaluate a WHERE / subsetting-IF condition.

    Strings are pandas ``DataFrame.eval`` expressions (python engine), e.g.
    ``ACCOUNT_STATUS == 'A' and CURRENT_BALANCE > 0``; callables receive the frame
    and return a boolean mask.
    """
    if condition is None or (isinstance(condition, str) and not condition.strip()):
        return pd.Series(True, index=df.index)
    if callable(condition):
        mask = condition(df)
    else:
        mask = df.eval(condition, engine="python")
    mask = pd.Series(mask, index=df.index) if not isinstance(mask, pd.Series) else mask
    return mask.fillna(False).astype(bool)


def apply_where(df, condition):
    # type: (pd.DataFrame, RowFilter) -> pd.DataFrame
    return df[row_mask(df, condition)]


def is_numeric(series):
    # type: (pd.Series) -> bool
    return pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series)


def sas_sort(df, by):
    # type: (pd.DataFrame, Sequence[str]) -> pd.DataFrame
    """PROC SORT: stable (EQUALS) and missing values sort low."""
    if not by:
        return df
    return df.sort_values(list(by), kind="mergesort", na_position="first")


def to_varname(text):
    # type: (Any) -> str
    """VALIDVARNAME=V7 style name derived from an ID value."""
    name = re.sub(r"[^0-9A-Za-z_]", "_", str(text).strip())
    if not name:
        name = "_"
    if name[0].isdigit():
        name = "_" + name
    return name[:32]


def optional(value):
    # type: (Any) -> Optional[Any]
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    return value
