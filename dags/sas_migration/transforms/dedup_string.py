"""%dedup_string -> remove duplicate tokens from a character variable."""
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from sas_migration.io import is_missing
from sas_migration.sas_compat import optional, parse_bool

TOKEN_LENGTH = 200  # length __word $200


def tokenize(value, dlm=None, honor_dlm=False):
    # type: (Any, Optional[str], bool) -> List[str]
    """scan(__temp, i, ' '): consecutive delimiters collapse, tokens truncated to $200.

    The legacy macro accepts DLM= but always scans on a blank; ``honor_dlm=True``
    uses DLM as the macro header documents.
    """
    if is_missing(value):
        return []
    sep = dlm if (honor_dlm and dlm) else " "
    return [t[:TOKEN_LENGTH] for t in str(value).split(sep) if t.strip(" ") != ""]


def dedup_string(value, dlm=None, honor_dlm=False):
    # type: (Any, Optional[str], bool) -> Any
    """Keep the first occurrence of each token; duplicates are matched case-insensitively
    (indexw(upcase(out), upcase(word))) and tokens are re-joined with catx(' ', ...)."""
    if is_missing(value):
        return np.nan
    seen = set()
    kept = []
    for token in tokenize(value, dlm, honor_dlm):
        key = token.upper()
        if key not in seen:
            seen.add(key)
            kept.append(token)
    joiner = dlm if (honor_dlm and dlm) else " "
    return joiner.join(kept) if kept else np.nan


def dedup_column(df, invar, outvar=None, dlm=None, honor_dlm=False):
    # type: (pd.DataFrame, str, Optional[str], Optional[str], Any) -> pd.DataFrame
    """Apply %dedup_string(invar=, outvar=, dlm=) to every row; OUTVAR blank = dedup in place."""
    if invar not in df.columns:
        raise KeyError("INVAR %s not found" % invar)
    honor = parse_bool(honor_dlm, "HONOR_DLM")
    target = optional(outvar) or invar
    out = df.copy()
    out[target] = df[invar].map(lambda v: dedup_string(v, optional(dlm), honor))
    return out
