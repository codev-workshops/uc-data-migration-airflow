"""%subset_data -> data step with SET (FIRSTOBS= OBS= RENAME=), WHERE, subsetting IFs, KEEP/DROP."""
import re
from typing import Any, List, Optional, Sequence, Tuple, Union

import pandas as pd

from sas_migration.sas_compat import RowFilter, apply_rename, optional, resolve_varlist, row_mask


def parse_obs(spec):
    # type: (Optional[str]) -> List[Tuple[int, int]]
    """'1-5 or 11-15 or 20' -> [(1, 5), (11, 15), (20, 20)] (the macro's PRX rewrite of OBS=)."""
    if not optional(spec):
        return []
    ranges = []
    for part in re.split(r"\s+or\s+", spec.strip(), flags=re.IGNORECASE):
        m = re.match(r"^\s*(\d+)\s*(?:-\s*(\d+))?\s*$", part)
        if not m:
            raise ValueError("Invalid OBS specification %r" % spec)
        lo = int(m.group(1))
        hi = int(m.group(2)) if m.group(2) else lo
        ranges.append((lo, hi))
    return ranges


def _positive(value, param):
    # type: (Any, str) -> Optional[int]
    if not optional(value):
        return None
    number = int(value)
    if number <= 0:
        raise ValueError("%s must be POSITIVE" % param)
    return number


def all_missing_mask(df):
    # type: (pd.DataFrame) -> pd.Series
    """The macro's documented idiom ``if=not missing(cats(of _all_))`` with options missing=' '."""
    blank = df.isna() | df.astype(str).apply(lambda col: col.str.strip() == "")
    return ~blank.all(axis=1)


def subset_data(df, where=None, if_=None, firstobs=None, lastobs=None, obs=None, keep=None, drop=None,
                rename=None, drop_blank_rows=False):
    # type: (pd.DataFrame, RowFilter, RowFilter, Any, Any, Optional[str], Any, Any, Any, bool) -> Tuple[pd.DataFrame, List[int]]
    """Return (subset, source_positions).

    Order of operations follows the generated data step:
      1. RENAME= on the input (WHERE/IF/KEEP/DROP use the new names)
      2. WHERE (applied as rows are read)
      3. FIRSTOBS=/OBS= - counted over WHERE-qualified rows
      4. OBS= list - subsetting IF on _N_ (data step iteration number)
      5. IF= subsetting if
      6. KEEP / DROP on the output
    ``source_positions`` are 0-based row positions in the input, used by validation.
    """
    first = _positive(firstobs, "FIRSTOBS")
    last = _positive(lastobs, "LASTOBS")
    data = apply_rename(df.reset_index(drop=True), rename)
    positions = pd.Series(range(len(data)), index=data.index)

    keep_mask = row_mask(data, where)
    data, positions = data[keep_mask], positions[keep_mask]

    start = (first or 1) - 1
    data, positions = data.iloc[start:last], positions.iloc[start:last]

    ranges = parse_obs(obs)
    if ranges:
        n = pd.Series(range(1, len(data) + 1), index=data.index)
        mask = pd.Series(False, index=data.index)
        for lo, hi in ranges:
            mask |= (n >= lo) & (n <= hi)
        data, positions = data[mask], positions[mask]

    if optional(if_) is not None:
        mask = row_mask(data, if_)
        data, positions = data[mask], positions[mask]
    if drop_blank_rows:
        mask = all_missing_mask(data)
        data, positions = data[mask], positions[mask]

    columns = list(data.columns)
    if optional(keep):
        kept = resolve_varlist(data, keep)
        columns = [c for c in columns if c in kept]
    if optional(drop):
        dropped = set(resolve_varlist(data, drop))
        columns = [c for c in columns if c not in dropped]
    return data[columns].reset_index(drop=True), [int(p) for p in positions]
