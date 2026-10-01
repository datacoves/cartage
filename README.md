# Cartage

Declarative data migrations. Describe sources, destinations and pipelines in YAML, put custom logic in plain
Python, run locally to test, and generate thin Airflow DAGs, Dagster jobs or Prefect flows for production.

![Cartage terminal demo](https://raw.githubusercontent.com/datacoves/cartage/main/docs/cartage-demo.gif)

- **Sources:** local CSV folders, S3, and any [dlt](https://dlthub.com) source.
- **Destinations:** SAP via BAPIs (ships a mock SAP; the RFC transport is planned) and any
  [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) (filesystem/S3, DuckDB, Postgres, Snowflake,
  BigQuery, ...), loaded natively by dlt. One pipeline can write to several destinations.
- **Engines:** `python` (a plain loop) and `dlt`.
- **Orchestrators:** Airflow, Dagster and Prefect. Generated code only calls `cartage run`, so local and production
  run the same code.

## Quickstart

```bash
pip install "cartage[dlt]"
cartage init demo && cd demo
cartage validate
cartage plan materials        # dry run: records before/after transforms and the BAPI payloads
cartage run materials         # reads 20 rows, filters 2, sends 18: 16 load, 2 fail on purpose (exit 1)
cartage run materials --advance-state
cartage run materials         # incremental: nothing new to load
cartage generate              # dags/materials_to_sap.py
cartage generate -t dagster   # orchestration/dagster/<pipeline>.py, for pipelines with schedule.dagster
cartage generate -t prefect   # orchestration/prefect/<pipeline>.py, for pipelines with schedule.prefect
```

Split-screen demo: run `cartage sap mock` in one terminal, set `url: http://localhost:8765` on `sap_erp.dev`
in `connections.yaml`, and run the pipeline in another terminal.

## Install

Cartage needs Python 3.11.4 or newer. The core install runs the `python` engine over CSV folders; extras add the rest.

| Install                       | Adds                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `pip install cartage`         | CLI, filesystem source, SAP destination, `python` engine, all generators     |
| `pip install "cartage[dlt]"`  | `dlt` engine, dlt sources and destinations                                   |
| `pip install "cartage[s3]"`   | S3 source and S3 state store                                                 |
| `pip install "cartage[all]"`  | `dlt`, `s3` and `airflow`                                                    |
| `pip install "dlt[snowflake]"`| the driver for a dlt destination or source (`postgres`, `bigquery`, ...)     |

Generating Airflow, Dagster or Prefect files needs nothing extra; the orchestrator itself runs them.

## Commands

| Command                                           | Does                                                                  |
| ------------------------------------------------- | --------------------------------------------------------------------- |
| `cartage init <dir>`                              | create a demo project (CSV → transforms → mock SAP)                   |
| `cartage validate [pipelines...]`                 | check YAML, connections, secrets, transforms and mappings; moves no data |
| `cartage plan <pipeline> [-n 3]`                  | dry run: files to process, first records before/after transforms, payloads |
| `cartage run <pipeline>`                          | run it; `--env`, `--engine`, `--full-refresh`, `--advance-state`, `--json` |
| `cartage generate [-t airflow\|dagster\|prefect]` | write orchestrator files; `--check`, `--output`, `--show-context <pipeline>` |
| `cartage connections list\|test <name>`           | list connections (secrets as references) or check one                 |
| `cartage state show\|reset <pipeline>`            | inspect or delete incremental state                                   |
| `cartage plugins`                                 | list installed sources, destinations, engines and orchestrators       |
| `cartage sap mock [--port 8765]`                  | run a mock SAP server that validates BAPI calls                       |

Global options go before the command: `-C/--project-dir`, `-v/--verbose`, `--debug` (tracebacks), `--no-color`.

## Project layout

| Path                          | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `cartage.yaml`                | environments, default engine, state location, orchestrator settings          |
| `connections.yaml`            | named connections with settings per environment — secrets only as references |
| `pipelines/*.yaml`            | source → transforms → destination(s) (+ schedule)                            |
| `transforms/*.py`             | `map` / `filter` / `batch` functions referenced as `module:function`         |
| `templates/airflow/dag.py.j2` | optional DAG template override (`{% extends "cartage/airflow_dag.py.j2" %}`) |
| `templates/<target>/...`      | optional Dagster/Prefect overrides (`dagster.py.j2`, `prefect.py.j2`)        |
| `.cartage/`                   | git-ignored: `secrets.yaml`, `state/`, `rejects/`                            |

## YAML configuration

Cartage uses three YAML layers: `cartage.yaml` sets project-wide defaults, `connections.yaml` defines named services
per environment, and each `pipelines/*.yaml` file describes one flow from a source to one or more destinations.

### `cartage.yaml`

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

### `connections.yaml`

Connections have a `type` and an `envs` map. Put service-specific settings under the environment where they apply;
pipelines refer to the connection by name. Keep credentials out of the file and use secret or environment references.

```yaml
connections:
  local_files:
    type: filesystem
    envs:
      dev: { path: ./data }

  sap_erp:
    type: sap
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
then `.cartage/secrets.yaml`. `${env:NAME}` reads an environment variable directly. See [Secrets](#secrets) for details.

### `pipelines/*.yaml`

Each pipeline names a source connection, applies an ordered list of transforms, and writes to a destination (or a
list of `destinations`, see [Multiple destinations](#multiple-destinations)). A step
must have exactly one of `map`, `filter`, or `batch`; each reference uses the `module:function` format.

```yaml
name: materials_to_sap
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
  connection: sap_erp
  bapi: BAPI_MATERIAL_SAVEDATA
  mapping:
    material: HEADDATA.MATERIAL
    description: MATERIALDESCRIPTION[].MATL_DESC
    uom: CLIENTDATA.BASE_UOM
  constants:
    MATERIALDESCRIPTION[].LANGU_ISO: EN
  commit: per_record

schedule:
  airflow:
    schedule: "0 3 * * *"
    tags: [sap, materials]
```

Source and destination fields other than `connection` are adapter options. Transform functions live in your `transforms/`
package; `with` passes keyword arguments to the function, and `on_error: fail` stops the run on that step's first
error instead of rejecting the record. The optional `schedule.airflow`, `schedule.dagster` and `schedule.prefect`
blocks control generation with `cartage generate` (see [Orchestrators](#orchestrators)). Run `cartage validate` after
editing YAML to check the project and references.

#### Sources

| Source       | Connection settings                                              | Pipeline options                                              |
| ------------ | ---------------------------------------------------------------- | ------------------------------------------------------------- |
| `filesystem` | `path`                                                           | `path` (glob), `format: csv`, `incremental`, `batch_size`     |
| `s3`         | `bucket`, `prefix`, `region`, `endpoint_url`, access keys        | `path` (glob), `format: csv`, `incremental`, `batch_size`     |
| `dlt`        | none: use `type: dlt` on the source                              | `ref` (`module:function`), `with`, `incremental`, `batch_size` |

File sources with `incremental: true` skip files already processed. A dlt source's `ref` returns a dlt source or
resource; `incremental: { cursor: updated_at, initial: "2024-01-01" }` adds a dlt cursor and needs `engine: dlt`.

#### SAP destination

`bapi` names the BAPI, `mapping` maps record fields to BAPI parameters (`HEADDATA.MATERIAL`, `TABLE[].FIELD`) and
`constants` sets fixed values. `commit` is `per_record` (default), `per_batch` or `none`. `cartage validate` checks
mapped parameters and fields against the bundled BAPI metadata; the mock SAP also rejects values that are too long,
not allowed, or missing when required, as SAP would.

#### dlt destinations

Any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) (filesystem/S3, DuckDB, Postgres,
Snowflake, BigQuery, ...) is a `type: dlt` connection. `destination` names a `dlt.destinations` factory, or a project
`module:function` returning one; the other settings are passed to it. Install the driver extra too, e.g.
`pip install "cartage[dlt]" "dlt[snowflake]"`. dlt destinations need `engine: dlt`.

```yaml
connections:
  warehouse:
    type: dlt
    envs:
      dev: { destination: filesystem, bucket_url: "file:///tmp/warehouse" }
      prd:
        destination: snowflake
        credentials: "${secret:snowflake.connection_string}"
        naming: sql_ci_v1                              # connection-level default
```

```yaml
destination:
  connection: warehouse
  dataset_name: raw                 # default: the pipeline name
  table_name: materials             # default: the source resource name
  write_disposition: merge          # append (default) | replace | merge
  primary_key: material             # merge needs primary_key or merge_key
  columns: { price: { data_type: decimal, precision: 18, scale: 4 } }
  loader_file_format: parquet
  naming: direct                    # dlt naming convention, or a project module (e.g. naming.upper)
  dlt_config:                       # any dlt config key, applied to this run only
    data_writer.buffer_max_items: 100000
    extract.max_parallel_items: 15
```

The dlt engine loads straight into the destination: bulk loads, staging and merge are dlt's, run once per run.
Without `transforms`, source batches reach dlt untouched, so Arrow/pandas batches (e.g. `sql_database` with the
pyarrow backend, ConnectorX) stay columnar. dlt fails whole load jobs, not single records: a failed load stops the
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
  type: dlt
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
dlt pipeline (`<pipeline>__<connection>`), so a failed destination retries without reloading the others. The source is
read once per destination; to read it once, land it in filesystem/S3 first and load from there.

```yaml
destinations:
  - { connection: warehouse, table_name: materials, write_disposition: merge, primary_key: material }
  - { connection: sap_erp, bapi: BAPI_MATERIAL_SAVEDATA, mapping: { ... } }
```

## Orchestrators

`cartage generate --target airflow|dagster|prefect` writes one file per pipeline that has a `schedule.<target>` block.
Each file only runs `cartage --project-dir <project> run <pipeline> --env <env>`, so deploy the Cartage project (with
`transforms/`) next to it. Settings merge: built-in defaults ← `orchestrators.<target>` in `cartage.yaml` ← the
pipeline's `schedule.<target>` (dicts merge by key). Add `--check` in CI to fail on stale files.

```yaml
schedule:
  airflow: { schedule: "0 3 * * *", tags: [sap] }
  dagster: { schedule: "0 3 * * *", tags: { team: data }, timezone: Europe/Brussels }
  prefect: { schedule: "0 3 * * *", tags: [sap], retries: 2 }
```

| Target  | Output (default)                        | Settings                                                                    | Environment at run time  |
| ------- | --------------------------------------- | --------------------------------------------------------------------------- | ------------------------ |
| airflow | `dags/<dag_id>.py`                      | `dags_dir`, `schedule`, `tags`, `default_args`, `operator`, `image`, ...    | Airflow var `cartage_env` |
| dagster | `orchestration/dagster/<name>.py`       | `out_dir`, `name`, `schedule`, `timezone`, `tags`, `env`, `command`, `project_dir` | `CARTAGE_ENV` env var  |
| prefect | `orchestration/prefect/<name>.py`       | `out_dir`, `name`, `schedule`, `retries`, `tags`, `env`, `command`, `project_dir`  | `CARTAGE_ENV` env var  |

- **Dagster:** each file defines a job, its schedule, and `defs = dg.Definitions(...)`. Load it with
  `dagster dev -f orchestration/dagster/<name>.py` (repeat `-f` per pipeline), or merge the `defs` objects.
- **Prefect:** each file defines the `cartage_run` flow. `python orchestration/prefect/<name>.py` serves it on the
  schedule, or point `prefect deploy` at `orchestration/prefect/<name>.py:cartage_run`.
- `env` defaults to `prd`; `command` (default `cartage`) may include a launcher, e.g. `uv run cartage`.

## Batches

- `batch` transforms see one source batch at a time (default 100 rows, `batch_size` source option), never across files.
- `commit: per_batch` commits each batch the engine hands to the destination (the dlt engine re-chunks at 100).
- dlt destinations are not fed in batches: dlt extracts, normalizes and loads the whole run. Size its files and
  buffers with `dlt_config` (e.g. `data_writer.buffer_max_items`, `data_writer.file_max_items`).

## Secrets

`${secret:sap.passwd}` reads `CARTAGE_SECRET__SAP__PASSWD`, then `.cartage/secrets.yaml`. `${env:VAR}` reads an
environment variable. Resolved values are never printed.

## State and rejects

Incremental state is saved only when a run has no record errors (or with `--advance-state`). Rejected records
go to `.cartage/rejects/<pipeline>/<run_id>.jsonl`. `cartage state show|reset <pipeline>` inspects or clears state.

With several destinations, each destination is its own run named `<pipeline>__<connection>`: state, rejects and the
dlt pipeline are kept per destination, and `state show|reset` covers all of them. `cartage run` goes through the
destinations in order and stops at the first fatal error; destinations that already finished keep their state, so a
rerun only retries the rest. With `--json`, `run` prints one result line per destination and `plan` prints a list.

Cartage's state store is the source of truth for dlt state: dlt's restore-from-destination is disabled, so
`--full-refresh` really starts from scratch.

## Exit codes

`0` ok · `1` record errors · `2` configuration error · `3` fatal run error (connection, transport, failed dlt load,
`on_error: fail`).

## Extending

Adapters are entry points in the groups `cartage.sources`, `cartage.destinations`, `cartage.engines` and
`cartage.orchestrators`. `cartage plugins` lists what is installed.

A destination is a record sink (`preview` + `write`, like SAP) by default. To have the dlt engine load into it natively
instead, expose `dlt_destination()` returning a dlt destination, plus `hints` (resource hints such as
`write_disposition`), `dataset_name`, `loader_file_format` and `dlt_env` (env vars applied to the run), as the
built-in `dlt` destination does.

## License

Apache-2.0
