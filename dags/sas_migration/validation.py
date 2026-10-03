"""Row-count and checksum helpers shared by every DAG's validation task."""
import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence

import pandas as pd

from sas_migration.io import format_value

log = logging.getLogger(__name__)

FIELD_SEP = "\x1f"


class ValidationError(Exception):
    """Raised when input/output reconciliation fails (fails the Airflow task)."""


def row_digest(values):
    # type: (Iterable[Any]) -> str
    joined = FIELD_SEP.join(format_value(v) for v in values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


def digest_records(records, ordered=False):
    # type: (Iterable[Sequence[Any]], bool) -> str
    """SHA-256 over a collection of records.

    ``ordered=False`` (default) sorts the per-row digests first, giving a multiset
    checksum that is independent of row order - transformations such as PROC SORT
    inside PROC TRANSPOSE legitimately reorder rows.
    """
    digests = [row_digest(r) for r in records]
    if not ordered:
        digests.sort()
    outer = hashlib.sha256()
    for d in digests:
        outer.update(d.encode("ascii"))
    return outer.hexdigest()


def frame_checksum(df, columns=None, ordered=False):
    # type: (pd.DataFrame, Optional[List[str]], bool) -> str
    cols = list(columns) if columns is not None else list(df.columns)
    header = FIELD_SEP.join(cols)
    body = digest_records(df[cols].itertuples(index=False, name=None), ordered=ordered)
    return hashlib.sha256((header + "|" + body).encode("utf-8")).hexdigest()


def profile(df, name, columns=None):
    # type: (pd.DataFrame, str, Optional[List[str]]) -> Dict[str, Any]
    cols = list(columns) if columns is not None else list(df.columns)
    return {
        "dataset": name,
        "row_count": int(len(df)),
        "column_count": len(cols),
        "columns": cols,
        "checksum": frame_checksum(df, cols),
    }


class Reconciliation(object):
    """Collects named checks and raises one ValidationError listing every failure."""

    def __init__(self, title):
        # type: (str) -> None
        self.title = title
        self.checks = []  # type: List[Dict[str, Any]]

    def expect_equal(self, name, expected, actual):
        # type: (str, Any, Any) -> bool
        ok = expected == actual
        self.checks.append({"check": name, "expected": expected, "actual": actual, "passed": ok})
        return ok

    def expect_true(self, name, condition, detail=None):
        # type: (str, bool, Any) -> bool
        ok = bool(condition)
        self.checks.append({"check": name, "expected": True, "actual": detail if detail is not None else ok, "passed": ok})
        return ok

    @property
    def passed(self):
        # type: () -> bool
        return all(c["passed"] for c in self.checks)

    def report(self):
        # type: () -> Dict[str, Any]
        return {"title": self.title, "passed": self.passed, "checks": self.checks}

    def raise_if_failed(self):
        # type: () -> Dict[str, Any]
        for c in self.checks:
            level = logging.INFO if c["passed"] else logging.ERROR
            log.log(level, "[%s] %s expected=%s actual=%s", "PASS" if c["passed"] else "FAIL",
                    c["check"], c["expected"], c["actual"])
        if not self.passed:
            failed = [c["check"] for c in self.checks if not c["passed"]]
            raise ValidationError("%s failed: %s" % (self.title, ", ".join(failed)))
        return self.report()
