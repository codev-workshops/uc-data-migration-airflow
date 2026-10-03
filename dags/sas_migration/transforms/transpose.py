"""%transpose -> PROC TRANSPOSE semantics in pandas."""
import logging
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from sas_migration.io import format_value, is_missing
from sas_migration.sas_compat import (
    RowFilter, apply_where, is_numeric, optional, parse_bool, resolve_columns, resolve_varlist,
    sas_sort, split_words, to_varname,
)

log = logging.getLogger(__name__)


class TransposeSpec(object):
    """Normalised %transpose parameters (shared by the transform and its validation)."""

    def __init__(self, df, by=None, var=None, prefix=None, sort=True, notsorted=False, id=None,
                 idlabel=None, let=False, copy=None, col=None, name="_NAME_", label="_LABEL_",
                 labels=None):
        if optional(col) and optional(id):
            raise ValueError("Only one of COL or ID parameters can be specified - you cannot specify both")
        if optional(idlabel) and not optional(id):
            raise ValueError("The IDLABEL parameter requires that the ID parameter is also specified")
        self.by = resolve_columns(df, split_words(by))
        self.copy = resolve_columns(df, split_words(copy))
        self.id = resolve_columns(df, split_words(id))
        self.idlabel = resolve_columns(df, split_words(idlabel))
        if len(self.id) > 1:
            raise ValueError("Only a single ID variable is supported")
        self.notsorted = parse_bool(notsorted, "NOTSORTED")
        self.sort = parse_bool(sort, "SORT") and not self.notsorted
        self.let = parse_bool(let, "LET")
        self.prefix = optional(prefix)
        self.name = optional(name)
        self.label = optional(label)
        self.col = split_words(col)
        self.labels = dict(labels or {})
        excluded = set(self.by + self.copy + self.id + self.idlabel)
        if optional(var):
            self.var = resolve_varlist(df, var)
        else:
            # VAR omitted: every numeric variable not named in another statement
            self.var = [c for c in df.columns if c not in excluded and is_numeric(df[c])]
        self.character_output = any(not is_numeric(df[v]) for v in self.var)
        self.has_labels = any(v in self.labels for v in self.var)

    def id_column(self, value):
        # type: (Any) -> str
        if self.prefix:
            return to_varname(str(self.prefix) + format_value(value))
        return to_varname(format_value(value))


def group_rows(df, by, notsorted):
    # type: (pd.DataFrame, List[str], bool) -> List[Tuple[Tuple, List[int]]]
    """BY-group processing over consecutive rows; errors like SAS if data is not sorted."""
    if not by:
        return [((), list(range(len(df))))]
    keys = [tuple(format_value(v) for v in row) for row in df[by].itertuples(index=False, name=None)]
    groups = []  # type: List[Tuple[Tuple, List[int]]]
    seen = set()
    for pos, key in enumerate(keys):
        if groups and groups[-1][0] == key:
            groups[-1][1].append(pos)
            continue
        if key in seen and not notsorted:
            raise ValueError("Data set is not sorted in ascending sequence for BY %s (key %s)" % (by, key))
        seen.add(key)
        groups.append((key, [pos]))
    return groups


def prepare_input(df, spec, where=None):
    # type: (pd.DataFrame, TransposeSpec, RowFilter) -> pd.DataFrame
    data = apply_where(df, where)
    if spec.sort and spec.by:
        data = sas_sort(data, spec.by)
    return data.reset_index(drop=True)


def id_rows(data, rows, spec):
    # type: (pd.DataFrame, List[int], TransposeSpec) -> "OrderedDict[str, int]"
    """Map ID column name -> row position for one BY group (LET keeps the last duplicate)."""
    mapping = OrderedDict()  # type: OrderedDict
    id_col = spec.id[0]
    for pos in rows:
        value = data.at[pos, id_col]
        if is_missing(value):
            log.warning("Observation %d has a missing ID value and is skipped", pos + 1)
            continue
        colname = spec.id_column(value)
        if colname in mapping and not spec.let:
            raise ValueError("The ID value %r occurs twice in the same BY group" % format_value(value))
        mapping[colname] = pos
    return mapping


def transpose(df, by=None, var=None, prefix=None, sort=True, notsorted=False, id=None, idlabel=None,
              let=False, copy=None, where=None, col=None, name="_NAME_", label="_LABEL_", labels=None):
    # type: (...) -> pd.DataFrame
    """Equivalent of %transpose / PROC TRANSPOSE.

    ``labels`` replaces SAS variable labels (CSV has no label metadata); _LABEL_ is
    only created when at least one transposed variable has a label, as in SAS.
    The IDLABEL mapping is returned in ``result.attrs['column_labels']``.
    """
    spec = TransposeSpec(df, by=by, var=var, prefix=prefix, sort=sort, notsorted=notsorted, id=id,
                         idlabel=idlabel, let=let, copy=copy, col=col, name=name, label=label, labels=labels)
    data = prepare_input(df, spec, where)
    groups = group_rows(data, spec.by, spec.notsorted)

    value_prefix = spec.prefix or "COL"
    if spec.id:
        value_columns = []  # type: List[str]
        for _, rows in groups:
            for c in id_rows(data, rows, spec):
                if c not in value_columns:
                    value_columns.append(c)
    else:
        width = max([len(rows) for _, rows in groups] + [0])
        value_columns = ["%s%d" % (value_prefix, i + 1) for i in range(width)]

    def cell(value):
        if spec.character_output:
            return format_value(value) if not is_missing(value) else ""
        return value

    records = []  # type: List[Dict[str, Any]]
    for _, rows in groups:
        first = rows[0]
        by_values = OrderedDict((b, data.at[first, b]) for b in spec.by)
        n_out = max(len(spec.var), len(rows) if spec.copy else 0)
        positions = id_rows(data, rows, spec) if spec.id else None
        for i in range(n_out):
            rec = OrderedDict(by_values)  # type: OrderedDict
            for c in spec.copy:
                rec[c] = data.at[rows[i], c] if i < len(rows) else np.nan
            if i < len(spec.var):
                v = spec.var[i]
                rec["_NAME_"] = v
                rec["_LABEL_"] = spec.labels.get(v, v)
                if positions is not None:
                    for colname, pos in positions.items():
                        rec[colname] = cell(data.at[pos, v])
                else:
                    for j, pos in enumerate(rows):
                        rec[value_columns[j]] = cell(data.at[pos, v])
            records.append(rec)

    columns = spec.by + spec.copy + ["_NAME_"] + (["_LABEL_"] if spec.has_labels else []) + value_columns
    out = pd.DataFrame.from_records(records, columns=columns) if records else pd.DataFrame(columns=columns)
    if spec.character_output:
        out[value_columns] = out[value_columns].fillna("")

    # NAME= / LABEL= blank drops the column, otherwise renames it
    rename = {}
    drop = []
    if spec.name is None:
        drop.append("_NAME_")
    elif spec.name.upper() != "_NAME_":
        rename["_NAME_"] = spec.name
    if spec.has_labels:
        if spec.label is None:
            drop.append("_LABEL_")
        elif spec.label.upper() != "_LABEL_":
            rename["_LABEL_"] = spec.label
    if spec.col:
        if len(spec.col) > len(value_columns):
            log.warning("COL lists %d names but only %d %s# columns exist", len(spec.col), len(value_columns),
                        value_prefix)
        rename.update({"%s%d" % (value_prefix, i + 1): new for i, new in enumerate(spec.col)
                       if i < len(value_columns)})
    out = out.drop(columns=drop).rename(columns=rename)

    if spec.idlabel:
        labels_map = OrderedDict()
        for _, rows in groups:
            for colname, pos in id_rows(data, rows, spec).items():
                labels_map.setdefault(colname, format_value(data.at[pos, spec.idlabel[0]]))
        out.attrs["column_labels"] = dict(labels_map)
    return out


def output_layout(out, spec):
    # type: (pd.DataFrame, TransposeSpec) -> Dict[str, Any]
    name_col = None
    if spec.name is not None:
        name_col = "_NAME_" if spec.name.upper() == "_NAME_" else spec.name
    label_col = None
    if spec.has_labels and spec.label is not None:
        label_col = "_LABEL_" if spec.label.upper() == "_LABEL_" else spec.label
    fixed = set(spec.by + spec.copy + [c for c in (name_col, label_col) if c])
    return {"name_col": name_col, "value_cols": [c for c in out.columns if c not in fixed]}


def input_cells(df, spec, where=None):
    # type: (pd.DataFrame, TransposeSpec, RowFilter) -> List[Tuple]
    """Every non-missing transposed cell as (by..., var, slot, value), derived from the input."""
    data = prepare_input(df, spec, where)
    cells = []
    for key, rows in group_rows(data, spec.by, spec.notsorted):
        if spec.id:
            slots = list(id_rows(data, rows, spec).items())
        else:
            slots = [(str(j + 1), pos) for j, pos in enumerate(rows)]
        for v in spec.var:
            for slot, pos in slots:
                value = data.at[pos, v]
                if not is_missing(value) and format_value(value) != "":
                    cells.append(key + (v if spec.name else "", slot, format_value(value)))
    return cells


def output_cells(out, spec):
    # type: (pd.DataFrame, TransposeSpec) -> List[Tuple]
    """Melt the transposed output back to (by..., var, slot, value) cells."""
    layout = output_layout(out, spec)
    cells = []
    for _, row in out.iterrows():
        key = tuple(format_value(row[b]) for b in spec.by)
        var = format_value(row[layout["name_col"]]) if layout["name_col"] else ""
        if spec.name and var == "":
            continue  # COPY padding rows carry no transposed values
        for j, c in enumerate(layout["value_cols"]):
            value = row[c]
            if not is_missing(value) and format_value(value) != "":
                slot = c if spec.id else str(j + 1)
                cells.append(key + (var, slot, format_value(value)))
    return cells


def expected_row_count(df, spec, where=None):
    # type: (pd.DataFrame, TransposeSpec, RowFilter) -> int
    data = prepare_input(df, spec, where)
    return sum(max(len(spec.var), len(rows) if spec.copy else 0)
               for _, rows in group_rows(data, spec.by, spec.notsorted))
