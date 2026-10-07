# Configuration

The three YAML files of a project, pipeline dependencies, engines and batches.

## YAML configuration

Cartage uses three YAML layers: `.cartage/config.yaml` sets project-wide defaults, `.cartage/connections.yaml` defines named services
per environment, and each `pipelines/*.yaml` file describes one flow from a source to one or more destinations.

### `.cartage/config.yaml`

```yaml
project: inventory
environments: [dev, prd]
default_env: dev

defaults:
  engine: dlt # or python

state:
  dev: { path: .cartage/state }

orchestrators:
  airflow:
    dags_dir: dags
    default_args: { owner: data-team, retries: 1 }
```

The `default_env` must be listed in `environments`. A pipeline can override the default engine with its own `engine`.
State settings are optional; when omitted, Cartage stores local state under `.cartage/state`.

### `.cartage/connections.yaml`

Connections have a `type` and an `envs` map. Put service-specific settings under the environment where they apply;
pipelines refer to the connection by name. Keep credentials out of the file and use secret or environment references.

```yaml
connections:
  local_files:
    type: filesystem                  # a local folder, s3://, gs://, az://, https://, ...
    envs:
      dev: { bucket_url: ./data }

  warehouse:
    type: snowflake                   # any dlt destination: duckdb, postgres, bigquery, ...
    envs:
      dev: { type: duckdb, credentials: dev.duckdb }     # an environment may use another system
      prd: { credentials: "${secret:snowflake.connection_string}" }

  sap:
    type: sap_bapi
    envs:
      dev: { transport: mock, client: "100" }
      prd:
        transport: rfc
        ashost: sap.example.com
        sysnr: "00"
        client: "100"
        user: "${secret:sap.user}"
        passwd: "${secret:sap.passwd}"
```

`${secret:key}` resolves from `CARTAGE_SECRET__<KEY>` (dots become double underscores and names are uppercased),
then `.cartage/secrets.yaml`, then `~/.cartage/secrets.yaml`. `${env:NAME}` reads an environment variable directly. See [Secrets](secrets-and-state.md#secrets) for details.

**[docs/connections.md](connections.md) lists every connection type** (`filesystem`, the dlt destinations,
`file_export`, `sap_bapi`): which settings go in `.cartage/connections.yaml` and which options go in the pipeline.

### `pipelines/*.yaml`

Each pipeline names a source connection, applies an ordered list of transforms, and writes to a destination (or a
list of `destinations`, see [Multiple destinations](#multiple-destinations)). A step
must have exactly one of `map`, `filter`, or `batch`; each reference uses the `module:function` format.

```yaml
name: materials
source:
  connection: local_files
  format: csv
  path: materials/*.csv
  incremental: true

transforms:
  - map: transforms.materials:normalize_uom
  - filter: transforms.materials:is_active
  - batch: transforms.materials:dedupe
    with: { key: material }

destination:
  connection: sap
  bapi: BAPI_MATERIAL_SAVEDATA
  mapping:
    HEADDATA.MATERIAL: material
    MATERIALDESCRIPTION[].MATL_DESC: description
    CLIENTDATA.BASE_UOM: uom
  constants:
    MATERIALDESCRIPTION[].LANGU_ISO: EN
  commit: per_record

schedule:
  airflow:
    schedule: "0 3 * * *"
    tags: [sap, materials]
```

Transforms run top to bottom, and each kind can appear any number of times and in any order, so several maps,
filters or batches chain naturally (each step with its own `with:`):

```yaml
transforms:
  - map: transforms.materials:normalize_uom
  - map: transforms.materials:trim_text
  - filter: transforms.materials:is_active
  - map: transforms.materials:add_defaults
    with: { plant: "1000" }
  - filter: transforms.materials:has_description
  - batch: transforms.materials:dedupe
    with: { key: material }
```

Source and destination fields other than `connection` are adapter options. Transform functions live in your `transforms/`
package; `with` passes keyword arguments to the function, and `on_error: fail` stops the run on that step's first
error instead of rejecting the record. `cartage scaffold transform <name>` writes `transforms/<name>.py` with a `map`,
a `filter` and a `batch` example, and `cartage scaffold source <name>` writes `sources/<name>.py`, a dlt resource to
fill in and reference as `ref: sources.<name>:<name>`. The optional `schedule.airflow`, `schedule.dagster` and `schedule.prefect`
blocks control generation with `cartage generate` (see [Orchestrators](orchestrators.md#orchestrators)). Run `cartage validate` after
editing YAML to check the project and references.

#### Sources

Full reference for every type: [docs/connections.md](connections.md).

| Source                         | Connection settings                         | Pipeline options                                              |
| ------------------------------ | ------------------------------------------- | ------------------------------------------------------------- |
| `filesystem` connection        | `bucket_url`, `credentials`                 | `path` (glob), `format` (`csv`, `jsonl`, `parquet`), `reader_options`, `incremental`, `batch_size` |
| `ref:` (a dlt source, no connection) | —                                     | `ref` (`module:function`), `with`, `resources`, `incremental`, `batch_size` |

A `filesystem` source reads every file matching `path` with dlt's typed readers (pandas for CSV), so numbers arrive as
numbers. `reader_options` go to the reader, e.g. `{ dtype: str, keep_default_na: false }` to keep codes such as
`000123` as text and empty cells as `""`. With `incremental: true` only files modified since the last run are read
(a changed file is read again in full). The same connection can be a destination (a dlt data lake) and the state
store for SAP and file export pipelines.

A dlt source's `ref` returns a dlt source or resource; `incremental: { cursor: updated_at, initial: "2024-01-01" }`
adds a dlt cursor. Incremental sources need the dlt engine (the default).
Other keys go to `dlt.sources.incremental`: `lag` (re-read a window before the last value, e.g. for late updates),
`end_value`, `primary_key`, `row_order`, `last_value_func`, `on_cursor_value_missing`, `range_start`, `range_end`.
When `ref` returns a dlt source with several resources, `resources: [name, ...]` picks the ones to read (default: the
source's selected resources).

#### Generic sources (no Python)

Files need no code: point a `filesystem` connection at a folder, a bucket or a web server (`bucket_url:
https://raw.githubusercontent.com/datasets/population/master/data`, then `path: population.csv`). `ref` also takes
installed modules, so dlt's built-in sources need only YAML, e.g. a REST API:

```yaml
# dlt's declarative rest_api source; `config` is its configuration dict.
source:
  ref: dlt.sources.rest_api:rest_api_source
  with:
    config:
      client: { base_url: "https://earthquake.usgs.gov/fdsnws/event/1/" }
      resources:
        - name: earthquakes
          endpoint:
            path: query
            params: { format: geojson, starttime: "2026-10-01", endtime: "2026-10-02" }
            data_selector: features      # where the records are in the response
            paginator: single_page
```

Remote files need the matching dlt extra: `dlt[http]` for `https://`, `dlt[s3]`, `dlt[gs]`, `dlt[az]`. CSV and JSON
Lines are read as records, so a column that is empty in every row is not created; declare it in the destination's
`columns` (`{ my_column: { data_type: double } }`) if it must exist. See dlt's
[REST API](https://dlthub.com/docs/dlt-ecosystem/verified-sources/rest_api/basic) docs for every option
(authentication, pagination, incremental parameters). Reshape records with `transforms`; write a Python source only
for logic the configuration can't express.

#### SAP destination (`sap_bapi`)

`bapi` names the BAPI, `mapping` fills BAPI parameters (`HEADDATA.MATERIAL`, `TABLE[].FIELD`) from record fields and
`constants` fills them with fixed values; both are keyed by the BAPI parameter. `commit` is `per_record` (default), `per_batch` or `none`. `cartage validate` checks
mapped parameters and fields against the bundled BAPI metadata; the mock SAP also rejects values that are too long,
not allowed, or missing when required, as SAP would.

#### File export destination (`file_export`)

`type: file_export` writes each run to one local file, in `json` (an array), `jsonl` (default), `xml` or `csv`. It works with
both engines and needs no extra dependencies. `cartage plan` shows each record as it will be written.

```yaml
connections:
  exports:
    type: file_export
    envs:
      dev: { path: ./output }       # folder, relative to the project
```

```yaml
destinations:
  - { connection: exports, format: json }                                 # output/<pipeline>.json
  - { connection: exports, name: as_xml, format: xml, root: materials, record: material }
  - { connection: exports, name: as_csv, format: csv, file: "materials_{date}.csv" }
```

`file` is the name inside the folder (`{pipeline}`, `{date}` as `YYYYMMDD` UTC, `{ext}`; default `{pipeline}.{ext}`).
The file is written to `<file>.partial` and moved into place only when the run succeeds, so a failed run never leaves
a half-written file and the previous one stays. Nested values become nested elements in XML (lists as `<item>`) and
JSON text in CSV; XML names that start with a digit get a leading `_`. A CSV's columns come from its first record;
records with other columns are rejected. Records that fail transforms go to rejects as usual, and the file holds the
rest.

#### dlt destinations

Every [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) is a connection type of its own:
`snowflake`, `duckdb`, `postgres`, `bigquery`, `databricks`, `filesystem` (a data lake), ... (`cartage plugins` lists
them). The settings are that destination's dlt settings (`credentials`, `database`, `bucket_url`, ...). A project
`module:function` returning a dlt destination works as a type too. Install the driver extra, e.g.
`pip install "dlt[snowflake]"`. dlt destinations need the dlt engine (the default).

```yaml
connections:
  warehouse:
    type: snowflake
    envs:
      prd:
        credentials: "${secret:snowflake.connection_string}"
        naming: sql_ci_v1                              # connection-level default
  lake:
    type: filesystem                                   # also a source and a state store for sinks
    envs:
      dev: { bucket_url: ./lake }
```

```yaml
destination:
  connection: warehouse
  dataset_name: raw                 # default: the pipeline name
  table_name: materials             # default: the source resource name
  write_disposition: merge          # append | replace | merge; unset: the source resource's hint, else append
  primary_key: material             # merge key: here, or the source resource's own hints
  columns: { price: { data_type: decimal, precision: 18, scale: 4 } }
  loader_file_format: parquet
  naming: direct                    # dlt naming convention, or a project module (e.g. naming.upper)
  dlt_config:                       # any dlt config key, applied to this run only
    data_writer.buffer_max_items: 100000
    extract.max_parallel_items: 15
  after_load:                       # called as f(pipeline, **with) after a successful load
    - { ref: utils.snowflake:enable_change_tracking, with: { tables: [materials] } }
```

The dlt engine loads straight into the destination: bulk loads, staging and merge are dlt's, run once per run.
Without `transforms`, source batches reach dlt untouched, so Arrow/pandas batches (e.g. `sql_database` with the
pyarrow backend, ConnectorX) stay columnar. Hints the source resource sets itself (`write_disposition`,
`primary_key`, `columns`, ...) are kept unless the destination sets them. `after_load` hooks get the dlt pipeline, so
they can run SQL with `pipeline.sql_client()` (grants, tags, change tracking); if one fails the data stays loaded, the
run exits with code 3 and state is not saved. dlt fails whole load jobs, not single records: a failed load stops the
run and state is not saved. Database-specific extraction (casts for Oracle LOBs and NUMBERs, MSSQL datetime2, Postgres
enums and timestamptz, time zones) belongs in your dlt source factory, not in Cartage.

Example: an incremental table copy from a database into Snowflake, kept columnar end to end.

```python
# sources/erp.py
from dlt.sources.sql_database import sql_table

def materials(credentials: str, schema: str):
    return sql_table(credentials=credentials, schema=schema, table="materials", backend="pyarrow")
```

```yaml
name: erp_materials
source:
  ref: sources.erp:materials
  with: { credentials: "${secret:erp.url}", schema: ERP }
  incremental: { cursor: updated_at, initial: "2024-01-01" }
destination:
  connection: warehouse
  dataset_name: raw_erp
  write_disposition: merge
  primary_key: material
engine: dlt
```

#### Multiple destinations

Use `destinations:` instead of `destination:`. Each entry is its own run, in order, with its own state, rejects and
dlt pipeline (`<pipeline>__<name>`; `name` defaults to the connection and tells apart destinations that share
one), so a failed destination retries without reloading the others. The source is
read once per destination; to read it once, land it in a `filesystem` connection first and load from there.

```yaml
destinations:
  - { connection: warehouse, table_name: materials, write_disposition: merge, primary_key: material }
  - { connection: sap, bapi: BAPI_MATERIAL_SAVEDATA, mapping: { ... } }
```

## Dependencies

A pipeline can list the packages it needs, and `.cartage/config.yaml` the ones every pipeline needs:

```yaml
# .cartage/config.yaml
defaults:
  dependencies: ["dlt[snowflake,parquet]"]

# pipelines/loans.yaml
dependencies: ["dlt[http]"]           # reads CSVs over https://
```

`cartage run`, `plan`, `validate`, `state` and `connections test` then run again under
`uv run --with <dependencies>`, like a script with inline dependencies: uv builds the environment once, caches it
(well under a second after that), and layers it on the Python running Cartage, so nothing is installed into it. The
same works on a laptop and in an Airflow task, as long as [uv](https://docs.astral.sh/uv/) is on `PATH`.
`--no-deps` (or `CARTAGE_NO_DEPS=1`) uses the installed packages instead.

## Engines

The engine runs a pipeline: it reads from the source, applies your transforms, and hands records to the destination.
Set it in `.cartage/config.yaml` (`defaults.engine`), per pipeline (`engine:`), or per run (`--engine`).

- **`dlt`** (default) runs every pipeline as a dlt pipeline: dlt's extraction, normalization, incremental state and
  loading. dlt destinations load natively (bulk loads, staging, merge); record destinations (`sap_bapi`,
  `file_export`) plug in as a dlt custom destination, so Cartage can still report each record's result (rejects, warnings).
- **`python`** is a plain loop: the reference implementation of the engine contract, for tests and framework
  development. It reads sources and writes record destinations; dlt destinations and `incremental` sources need the
  dlt engine, which keeps the cursor.

The engines differ in two ways today: dlt drops fields whose value is `None` before a record destination sees them,
and the dlt engine hands record destinations batches of 100 (which is what SAP `commit: per_batch` groups).

Engines are plug-ins (the `cartage.engines` entry point group, see [Extending](extending.md#extending)), so another load tool can
become an engine without changing pipelines, as long as it can run Python transforms between reading and writing.

## Batches

- `batch` transforms see one source batch at a time (default 100 rows, `batch_size` source option).
- `commit: per_batch` commits each batch the engine hands to the destination (the dlt engine re-chunks at 100).
- dlt destinations are not fed in batches: dlt extracts, normalizes and loads the whole run. Size its files and
  buffers with `dlt_config` (e.g. `data_writer.buffer_max_items`, `data_writer.file_max_items`).
