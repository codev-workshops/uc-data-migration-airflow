# ETL Migration Notes: SAS macros to Airflow

This document records how five data-transformation macros from
`ts-sas-legacy-analytics/Macro/` were translated into Airflow 2.0 DAGs, and why
each translation decision was made.

| SAS macro | DAG id | DAG file | Python implementation |
|---|---|---|---|
| `%export_csv` (→ `%export_dlm`) | `sas_export_csv` | `dags/sas_export_csv.py` | `dags/sas_migration/transforms/export_csv.py` |
| `%transpose` | `sas_transpose` | `dags/sas_transpose.py` | `dags/sas_migration/transforms/transpose.py` |
| `%subset_data` | `sas_subset_data` | `dags/sas_subset_data.py` | `dags/sas_migration/transforms/subset_data.py` |
| `%compare` | `sas_compare` | `dags/sas_compare.py` | `dags/sas_migration/transforms/compare.py` |
| `%dedup_string` | `sas_dedup_string` | `dags/sas_dedup_string.py` | `dags/sas_migration/transforms/dedup_string.py` |

---

## 1. Cross-cutting decisions

### 1.1 Layout: framework-agnostic transforms + thin DAGs
* The macro logic lives in `dags/sas_migration/transforms/` as plain pandas
  functions with no Airflow imports. DAG files only stage data, call the
  transform and run validation.
* **Why:** SAS macros are reusable library code called from many programs. Keeping
  the Python equivalents framework-free lets them be reused the same way (other
  DAGs, notebooks, batch scripts) and unit-tested without a scheduler
  (`tests/test_transforms.py`).

### 1.2 Task graph
Every DAG has the same three `PythonOperator` tasks:

```
extract  >>  <macro>  >>  validate
```

| Task | SAS equivalent | What it does |
|---|---|---|
| `extract` | `DATA=` dataset reference / `LIBNAME` | Copies the source CSV(s) into the run directory and writes a JSON profile (row count, columns, checksum). |
| `<macro>` | The macro body | Runs the transform on the staged copy and persists the output plus a JSON summary. |
| `validate` | (no SAS equivalent — new control) | Re-reads persisted input and output from disk and reconciles row counts and checksums. Raises `ValidationError` on any failed check, so the run goes red. |

### 1.3 Datasets → CSV files, `WORK` → per-run directory
* A SAS dataset becomes a CSV file; a SAS library (`LIBNAME`) becomes a directory
  of CSVs where the file stem is the member name (used by `%compare`).
* The SAS `WORK` library (temporary per-session datasets) maps to
  `data/work/<dag_id>/<run_id>/`. Each run is isolated, can be re-run
  idempotently, and the artefacts remain for audit/debugging.
* Tasks exchange **file paths** (small JSON via XCom/return values), never
  DataFrames. **Why:** XCom is stored in the metadata DB and is not meant for
  data; files also make every intermediate step inspectable like a `WORK`
  dataset.
* The data root defaults to `<repo>/data` and can be overridden with the
  `SAS_MIGRATION_DATA_DIR` environment variable (set to `/opt/airflow/data` in
  `docker-compose.yml`, which also mounts `./data`).

### 1.4 Macro parameters → `params` / `dag_run.conf`
* Each DAG declares the macro's parameters as `DEFAULT_PARAMS`; any key can be
  overridden at trigger time with `dag_run.conf`
  (`airflow dags trigger sas_subset_data -c '{"obs": "1-5 or 11-15"}'`).
* Parameter names match the SAS macro parameters (`by`, `var`, `where`,
  `keep`, `replace`, ...) so existing SAS calls can be translated mechanically.
* SAS boolean spellings (`Y/N`, `YES/NO`, `1/0`, `TRUE/FALSE`) are accepted via
  `sas_compat.parse_bool`, matching the macros' own `%upcase(%substr(...))`
  handling.
* All DAGs use `schedule_interval=None` (triggered on demand) because the macros
  are utilities invoked by callers, not scheduled jobs.

### 1.5 SAS semantics emulated in `sas_compat.py`
| SAS behaviour | Python decision |
|---|---|
| Variable names are case-insensitive | `resolve_columns` matches case-insensitively and returns the real column name. |
| Variable lists: `_ALL_`, `_NUMERIC_`, `_CHARACTER_`, `A--C`, `F1-F3` | `resolve_varlist` expands all forms. |
| `PROC SORT` is stable | `sas_sort` uses `kind="mergesort"`; missing values sort first, as in SAS. |
| `WHERE` / `IF` expressions | Expressed as pandas `DataFrame.query()` strings (`ACCOUNT_STATUS == 'A'`). SAS expression syntax is **not** parsed — callers translate expressions once during migration. This avoids building a SAS expression interpreter. |
| `RENAME=(old=new ...)` | Accepts either the SAS string `"old=new old2=new2"` or a dict. |
| Missing values (`.` / blank) | Read as `NaN`/`None`; written as empty fields. `io.is_missing` treats `NaN`, `None` and blank strings as missing (SAS has no distinction between empty and blank character values). |

### 1.6 Type handling
* Columns are read with pandas type inference, but identifier columns and any
  column a macro treats as text can be forced to string (`read_options` /
  `string_columns`). **Why:** SAS character variables like `ACCOUNT_ID` or
  `BRANCH_ID` can look numeric; inference would strip leading zeros or turn them
  into floats.
* Numeric values are formatted with `repr`-style shortest round-trip formatting
  (integers without `.0`) for both output and checksums, so a value written and
  read back hashes identically.

### 1.7 Validation and checksum design (`validation.py`, `reconcile.py`)
* **Row digest:** each row is serialised as its normalised cell values joined by
  a unit-separator (`\x1f`), with missing values as a fixed sentinel, then hashed
  with SHA-256.
* **Dataset checksum:** SHA-256 over the row digests. By default the row digests
  are **sorted** first (order-insensitive checksum); `ordered=True` keeps row
  order and is used where order is part of the macro contract (`%export_csv`,
  `%subset_data`).
* **Independence:** the validate task never reuses objects from the transform
  task; it reloads the staged input and the written output from disk. That way it
  also catches serialisation problems (quoting, type coercion, truncation).
* **What "row count" and "checksum" mean per macro** differs because not every
  macro preserves rows; the specific invariants are listed in each macro section.
* Every check is logged as `[PASS]`/`[FAIL] <name> expected=... actual=...` and
  written to `validation.json` in the run directory.

### 1.8 Python / Airflow version
* `docker-compose.yml` runs `apache/airflow:2.0.0` (Python 3.6.15, pandas 1.1.5,
  numpy 1.19.5). Code avoids f-string `=` specifiers, dataclasses, walrus,
  `DataFrame.map`, etc. so it runs on that image and on current pandas
  (tested on 1.1.5 and 3.x).
* Only `PythonOperator` from core Airflow is used; no extra providers or pip
  packages are required.

---

## 2. `%export_csv` → `sas_export_csv`

### SAS data flow
`%export_csv(data=, path=, replace=, label=, header=, lrecl=)` is a thin wrapper
that calls `%export_dlm(..., dbms=csv)`. `%export_dlm` resolves the output path
(a directory gets `<dataset>.csv` appended), then runs `PROC EXPORT` (or a
`DATA _NULL_` writer when labels/LRECL are involved). Input dataset options such
as `(where=(...))` are allowed on `DATA=`.

### Translation
| SAS | Python |
|---|---|
| `DATA=lib.ds(where=(...))` | `data` path + separate `where` param (pandas query). |
| `PATH=` dir or file | `resolve_output_path`: directory → `<member>.csv` (`.tdf`/`.txt` for TAB/DLM; member = part after the libref); file path kept as is, but its directory must exist (SAS errors with "No output created"). |
| `DBMS=CSV` / `TAB` / `DLM` | `dbms` + `delimiter` params (default CSV, comma). |
| `REPLACE=N` (default) | Raises `OutputExistsWarning` if the file exists. SAS only emits a WARNING and sets `SYSCC`; in Airflow a failed task is the equivalent visible signal. The DAG default is `replace=True` so re-runs are idempotent. |
| `LABEL=Y` | Header uses the column label from a `labels` mapping (CSV has no label metadata), falling back to the name. |
| `HEADER=N` | No header row. |
| `LRECL=` | Each output record length is checked; exceeding it raises `ValueError` instead of SAS's silent line wrapping/truncation, because silent truncation is a data-loss bug. |
| DSD quoting | Python `csv.writer` with `QUOTE_MINIMAL`: fields containing the delimiter, quotes or newlines are quoted and embedded quotes doubled — identical to SAS DSD output. |
| Missing values | Written as empty fields (SAS `.`/blank → empty with DSD). |

### Validation
* `row_count`: exported data rows == input rows after `WHERE`.
* `column_count` and `header_present` (header matches names/labels).
* `ordered_row_checksum`: exported file re-read as text equals the filtered input
  formatted as text, in order.

---

## 3. `%transpose` → `sas_transpose`

### SAS data flow
Optional `PROC SORT` by `BY` (skipped with `NOTSORTED`), then `PROC TRANSPOSE`
with `BY`, `VAR`, `ID`, `IDLABEL`, `COPY`, `LET`, `PREFIX`; the result is then
post-processed to rename `COL1..n` (`COL=`), rename or drop `_NAME_` (`NAME=`) and
`_LABEL_` (`LABEL=`). `WHERE=` filters the input.

### Translation
| SAS | Python |
|---|---|
| `PROC SORT` | `sas_sort` (stable) unless `sort=N` or `notsorted=Y`. |
| `BY ... NOTSORTED` | Groups are runs of consecutive equal BY values (`group_rows`), not global groups. |
| `VAR` omitted | Defaults to all numeric variables not in BY/ID/COPY, like `PROC TRANSPOSE`. |
| `ID` / `IDLABEL` / `PREFIX` | Output column names are `prefix + ID value`, normalised by `to_varname` (invalid characters → `_`, leading digit prefixed with `_`) as SAS does with `VALIDVARNAME=V7`. |
| Duplicate ID within a BY group | Error, unless `LET=Y` in which case the **last** value wins — same as SAS. |
| Missing ID value | Row skipped (SAS writes a note and drops it). |
| `COPY` | Output row *i* of a BY group gets the COPY values of input row *i* of that group; the group yields `max(#VAR, #rows)` output rows, extra rows have missing transposed values — same as `PROC TRANSPOSE` `COPY`. |
| `COL=` | Renames `COL1..n` (or `COL` when a single column). |
| `NAME=` / `LABEL=` blank | `_NAME_` / `_LABEL_` dropped; a value renames them. |
| Mixed character/numeric `VAR` | Output cells become character, as in SAS. |

### Validation
Transposition reshapes rows, so the invariant is on **cells**, not rows:
* `row_count`: output rows == expected count computed independently from the
  input (`#BY groups × #VAR variables`, or `#BY groups` when `ID` is used).
* `non_missing_cell_count`: every non-missing input cell `(BY keys, variable, ID,
  value)` appears exactly once in the output.
* `cell_checksum`: SHA-256 over the sorted set of those tuples, computed from the
  input (`input_cells`) and by melting the output back (`output_cells`). Equal
  checksums prove no value was lost, duplicated or moved to another key.

---

## 4. `%subset_data` → `sas_subset_data`

### SAS data flow
A single data step: `SET data(firstobs= obs= rename=(...))`, then `WHERE`,
subsetting `IF`, `KEEP`/`DROP`. `OBS=` accepts a range expression such as
`1-5 or 11-15`, which the macro turns into `if (1<=_N_<=5) or (11<=_N_<=15)`.

### Translation
| SAS | Python |
|---|---|
| Order of operations | Follows SAS execution of the generated step, not the textual order: `RENAME=` (dataset option) → `WHERE` (filters as rows are read) → `FIRSTOBS=`/`OBS=` (counted over WHERE-qualified rows) → `OBS` list (`_N_`) → `IF` → `KEEP`/`DROP`. `WHERE`/`IF`/`KEEP` therefore use the **new** names. |
| `FIRSTOBS`/`LASTOBS` | 1-based inclusive positions; `LASTOBS` maps to SAS `OBS=` (an absolute last observation number, not a count). Both must be positive, as `%parmv(_val=POSITIVE)` enforces. |
| `OBS=1-5 or 11-15 or 20` | `parse_obs` reproduces the macro's PRX rewrite: inclusive ranges, a single number means that one observation (`20` → `20-20`), `or` is case-insensitive. Ranges beyond the data are not an error. |
| `_N_` | Data-step iteration number = 1-based position among rows that survived `WHERE`/`FIRSTOBS`/`LASTOBS`. |
| `KEEP` and `DROP` together | Both applied (keep, then drop), with SAS variable lists supported. |
| "Delete all-blank rows" idiom from the macro header | `drop_blank_rows=True` (`all_missing_mask`). |

### Validation
* `output_rows_le_input_rows` and `row_count`.
* The transform persists the 1-based input position of each output row;
  `positions_unique_and_ordered` checks they are strictly increasing (subsetting
  must not reorder or duplicate).
* `columns_subset_of_input` (after rename).
* `ordered_row_checksum`: input rows selected by those positions, renamed and
  projected to the output columns, must hash identically to the output.

---

## 5. `%compare` → `sas_compare`

### SAS data flow
* **Library mode** (`BASE=` and `COMP=` are librefs): reads
  `DICTIONARY.TABLES` for both libraries, builds a member report (in base?, in
  compare?, nobs each, nobs differ?), then recursively calls itself for every
  member present in both.
* **Dataset mode:** sorts both by `BY`, builds a "check" dataset of keys present
  in only one side, then runs `PROC COMPARE` (`METHOD=`, `CRITERION=`,
  `MAXPRINT=`). `LOADDTTM` is dropped from BY comparisons because it always
  differs between loads.

### Translation
| SAS | Python |
|---|---|
| Libref | Directory of CSVs; member name = upper-cased file stem (`list_library`). Mode is auto-detected: directory → library, file → dataset. |
| `DICTIONARY.TABLES` full join | `library_report` → `library_report.csv` with the macro's columns `in_base, in_comp, matched (MATCHED/NO MATCH), base_libname, base_memname, base_nobs, comp_libname, comp_memname, comp_nobs, obsdiff ("<<<")`. `FILTER=` (regex on member name, case-insensitive) is supported. |
| Per-member `BY` | `by` accepts a string (same keys for all) or a `{member: keys}` dict, because different tables have different keys. |
| No `BY` | Positional (observation-number) matching, same as `PROC COMPARE` without `ID`. |
| `METHOD=EXACT/ABSOLUTE/PERCENT/RELATIVE`, `CRITERION=` | `values_equal` implements the `PROC COMPARE` formulas: ABSOLUTE `\|b-c\| <= c`; PERCENT `100·\|b-c\|/\|b\| <= c`; RELATIVE `\|b-c\|/((\|b\|+\|c\|)/2) <= c`. Missing equals missing only. Character values compare exactly, ignoring trailing blanks (SAS padding). |
| `MAXPRINT=(a,b)` | Limits rows in `differences_printed.csv` per variable / in total (the printed listing); `differences.csv` and the summary counts are always complete. |
| Check dataset (keys in one side only) | `check.csv` with an `in` column (`BASE record`/`COMP record`), as the macro builds it. Without `BY`, surplus observations are listed by `_OBS_`. |
| Variables only in base / only in compare | Listed in `summary.json`. |
| `LOADDTTM` | Dropped before BY comparisons, as in the macro. |
| Result signalling | SAS `PROC COMPARE` sets `SYSINFO`; the macro does not abort. The DAG mirrors that by default (`fail_on_difference=False`): differences are reported, the run succeeds. Set `fail_on_difference=True` to make differences fail the `compare` task (useful as a migration gate). |
| Output | `PROC COMPARE` listing → machine-readable files: `summary.json`, `differences.csv`, `differences_printed.csv`, `check.csv` per member under `<run dir>/<MEMBER>/`, plus `library_report.csv` in library mode. |

### Validation
`%compare` reads but does not change data, so the checks prove the report is
complete and consistent with the inputs:
* `base_checksum_unchanged` / `compare_checksum_unchanged`: inputs were not altered.
* `base_row_count` / `compare_row_count` equal the summary counts.
* `base_rows_reconcile` / `compare_rows_reconcile`: `matched + only-in-this-side == input rows`.
* `check_rows` equals the number of unmatched keys; `difference_rows` (and per
  variable) equal the reported unequal-value counts.
* `paired_checksums`: checksums of the matched base and compare rows over common
  columns. If they are equal, the reported difference count must be 0; with
  `METHOD=EXACT` unequal checksums must coincide with at least one reported
  difference (fuzzy methods may legitimately report 0). The pair is logged for
  audit either way.
* Library mode additionally reconciles `member_count` and each member's `nobs`
  in the library report.

---

## 6. `%dedup_string` → `sas_dedup_string`

### SAS data flow
A data-step code fragment: for each word of `INVAR` (via `scan`), append it to
`OUTVAR` with `catx(' ', ...)` if `indexw(upcase(OUTVAR), upcase(word))` is 0.
`OUTVAR` defaults to `INVAR` (in place).

### Translation
| SAS | Python |
|---|---|
| Fragment inside caller's data step | `dedup_column(df, invar, outvar)` applied to a column; `dedup_string(value)` for scalars. |
| Case-insensitive match, first spelling kept | Tokens compared upper-cased; the first occurrence's original case is kept. |
| Order | First-occurrence order preserved. |
| `catx(' ', ...)` | Output joined with a single blank; leading/trailing/multiple blanks collapse. |
| Missing input | Output missing. |
| **`DLM=` discrepancy** | The macro documents `DLM=` but the body hard-codes `scan(__temp, __i, ' ')`, so in SAS the parameter has **no effect** — words are always split on blanks. The migration preserves that legacy behaviour by default (`honor_dlm=False`) so outputs match production SAS. `honor_dlm=True` splits on the `DLM=` value instead (empty tokens skipped) for callers that relied on the documented intent. Flagged for business confirmation. |
| `length __word $200` | Tokens are truncated to 200 characters (`TOKEN_LENGTH`), as the SAS variable length would. |

### Validation
* `row_count`: output rows == input rows (row-preserving transform).
* `passthrough_columns_checksum`: all columns other than the output column are
  unchanged.
* `token_set_checksum`: per row, the case-insensitive *set* of tokens is the same
  before and after (nothing lost, nothing invented).
* `rows_with_duplicate_tokens` == 0 and `first_occurrence_order_violations` == 0.
* `tokens_removed` is logged for information.

---

## 7. Sample data and tests
* `data/sample/generate_samples.py` builds deterministic fixtures from the SAS
  repo's seed data (`ts-sas-legacy-analytics/Data/csv`):
  `oracle_dw/*.csv`, `dedup_string/ACCOUNT_CHANNELS.csv` (duplicate channel
  tokens in mixed case) and `compare/base|comp/` (dropped and added rows, changed
  values, float noise, an added `LOADDTTM`, a dropped column, a member present in
  only one library).
* `tests/test_transforms.py` covers the examples from each macro's header
  (`sashelp.class` cases), edge cases above, and that reconciliation detects
  tampered outputs. Run with `python -m pytest tests`.
* All five DAGs were executed end-to-end with `airflow dags test` in
  `apache/airflow:2.0.0`.

## 8. Known differences / limitations
* `WHERE`/`IF` use pandas query syntax, not SAS syntax (see 1.5).
* CSV carries no SAS metadata (formats, informats, labels, lengths). Labels for
  `%export_csv LABEL=Y` and `%transpose IDLABEL` must be supplied as a mapping.
  Date values are carried as text in their source representation.
* `PROC COMPARE`'s printed listing is replaced by structured files; formatting of
  the SAS listing is not reproduced.
* `%export_csv` errors on `LRECL` overflow instead of wrapping.
* `REPLACE=N` fails the task rather than only warning.
