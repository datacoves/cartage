# Database extraction tuning

Advanced options for large database sources.

## Advanced: database extraction tuning

Cartage has no database drivers of its own: a dlt source's `ref` is your Python, so ConnectorX, `sql_table`,
SQLAlchemy, Oracle or Teradata clients, and every per-database fix are plain code in your project. Cartage calls the
function with `with:` (secrets resolved), adds `incremental:`, and hands what it yields to dlt. Keep tuning helpers in a
project package (e.g. `utils/`) and use them from source functions. Where each kind of tuning goes:

| Need                                                        | Where in Cartage                                        |
| ----------------------------------------------------------- | ------------------------------------------------------- |
| Driver and backend (ConnectorX, pyarrow, pandas, SQLAlchemy) | the source function                                     |
| SQL casts per database (timestamps, LOBs, decimals, enums)  | the source function builds the `SELECT`                 |
| Per-batch type fixes (epoch ms, ints → float, rounding)     | `resource.add_map(...)` in the source function          |
| Row-count / truncation checks                               | a generator wrapper in the source function              |
| Credentials                                                 | `with:` + `${secret:...}`                               |
| dlt performance and load settings                           | `dlt_config:` on the destination                        |
| Identifier casing (`direct`, upper, lower, keep quotes)     | `naming:` on the destination                            |
| Column types, nullability                                   | `columns:` on the destination, or hints in the source   |
| `pipelines_dir`, bucket URL, write disposition, file format | Cartage state, the connection, destination options      |

### Driver, casts and fallback

The source function picks the driver and can fall back when a type isn't supported, then build its query from
metadata-driven casts (for example a `build_select_with_casts`-style helper that reads `ALL_TAB_COLUMNS` or
`INFORMATION_SCHEMA`):

```python
# sources/erp.py
from urllib.parse import quote_plus

import connectorx as cx
import dlt

from utils.data_types import build_select  # your helper, e.g. adapted from build_select_with_casts


@dlt.resource(name="materials", max_table_nesting=0)
def materials(db: str, user: str, password: str, dsn: str, table: str, conversions: dict | None = None,
              updated_at=dlt.sources.incremental("UPDATED_AT")):
    conn = f"{db}://{quote_plus(user)}:{quote_plus(password)}@{dsn}"
    where = f" WHERE UPDATED_AT >= '{updated_at.start_value}'" if updated_at.start_value else ""
    query = f"SELECT {build_select(conn, table, conversions or {})} FROM {table}{where}"
    try:
        yield from cx.read_sql(conn, query, return_type="arrow_stream", batch_size=10_000)
    except BaseException:  # Arrow stream lacks some types (e.g. Oracle NCLOB): fall back to a DataFrame
        yield cx.read_sql(conn, query)
```

```yaml
source:
  ref: sources.erp:materials
  with:
    db: oracle
    user: "${secret:erp.user}"
    password: "${secret:erp.password}"
    dsn: "${secret:erp.dsn}"
    table: ERP.MATERIALS
    conversions: { CREATED_ON: timestamp, NOTES: lob_text }
  incremental: { cursor: UPDATED_AT, initial: "2024-01-01" }
```

Cartage's `incremental:` binds to the function's `dlt.sources.incremental` argument, so `start_value` is the stored
cursor: put it in the `WHERE` to read only new rows. dlt's `sql_table(..., backend="connectorx" | "pyarrow" |
"sqlalchemy")` does that pushdown for you, and takes `query_adapter_callback` (extra filters or suffixes),
`table_adapter_callback` (e.g. `remove_nullability_adapter`) and `chunk_size`. Pass `with:` values such as `backend`
or `connector: sqlalchemy` to switch drivers per pipeline when ConnectorX can't read a table.

### Per-batch type normalization

Attach Arrow, pandas or row transforms with `add_map`. They run inside dlt on every batch, before normalization, and
keep batches columnar:

```python
from utils.data_types import normalize_types  # timestamps → epoch ms, decimals/ints → float64, rounding

def orders(...):
    return sql_table(credentials=..., table="ORDERS", backend="pyarrow").add_map(normalize_types)
```

Prefer `add_map` in the source over Cartage `transforms` for bulk copies: Cartage transforms see row dicts, so Arrow
batches are converted to rows first.

### Row-count checks

ConnectorX can silently return fewer rows (dropped batches, timeouts). Count what you yield and raise at the end; the
run then fails with exit code 3 before anything is loaded, and no state is saved:

```python
def checked(batches, expected: int, tolerance: int = 10):
    n = 0
    for batch in batches:
        n += len(batch)  # Arrow tables, DataFrames and lists all have len()
        yield batch
    if n < expected - tolerance:
        raise RuntimeError(f"extracted {n} rows, source had {expected}: try connector: sqlalchemy")

@dlt.resource(name="materials")
def materials(conn: str, table: str):
    expected = int(cx.read_sql(conn, f"SELECT COUNT(*) FROM {table}").iloc[0, 0])
    yield from checked(cx.read_sql(conn, f"SELECT * FROM {table}", return_type="arrow_stream"), expected)
```

### dlt settings and naming

`dlt_config:` takes any dlt config key (as in `dlt.config[...]`), on the connection as a default or on the pipeline
destination as an override, applied only while that run executes:

```yaml
destination:
  connection: lake
  loader_file_format: parquet
  write_disposition: replace
  naming: utils.sql_upper             # a module with a NamingConvention class, as dlt's schema.naming expects
  columns: { AMOUNT: { data_type: double, nullable: true } }
  dlt_config:
    sources.data_writer.file_max_items: 1000000
    sources.data_writer.file_max_bytes: 100000000
    data_writer.buffer_max_items: 100000
    extract.max_parallel_items: 15
    normalize.data_writer.disable_compression: false
    load.truncate_staging_dataset: true
```

`naming` takes dlt's built-ins (`direct`, `snake_case`, `sql_cs_v1`, `sql_ci_v1`) or a project module, so naming
classes such as upper, lower or keep-quotes variants work as they are. Two settings are Cartage's: leave
`restore_from_destination` on (the default; it is how a run gets its state back from the destination) and
`load.delete_completed_jobs` true (the SAP and file export state archive must not keep loaded data).

Process-wide patches to dlt internals (for example keeping all-null columns) are not run settings: apply them in the
source module (at import, or at the start of the source function), knowing they affect every pipeline in that process.
