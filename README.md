# Cartage

Declarative data migrations. Describe sources, destinations and pipelines in YAML, put custom logic in plain
Python, run locally to test, and generate thin Airflow DAGs, Dagster jobs or Prefect flows for production.

![Cartage terminal demo](https://raw.githubusercontent.com/datacoves/cartage/main/docs/cartage-demo.gif)

- **Sources:** local CSV folders, S3, and any [dlt](https://dlthub.com) source.
- **Destinations:** SAP via BAPIs (v0.1 ships a mock SAP; RFC is planned) and any
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

## Project layout

| Path                          | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `cartage.yaml`                | environments, default engine, state location, orchestrator settings          |
| `connections.yaml`            | named connections with settings per environment — secrets only as references |
| `pipelines/*.yaml`            | source → transforms → destination(s) (+ schedule)                            |
| `transforms/*.py`             | `map` / `filter` / `batch` functions referenced as `module:function`         |
| `templates/airflow/dag.py.j2` | optional DAG template override (`{% extends "cartage/airflow_dag.py.j2" %}`) |
| `templates/dagster/dagster.py.j2`, `templates/prefect/prefect.py.j2` | optional overrides (`{% extends "cartage/dagster.py.j2" %}`) |
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
package; `with` passes keyword arguments to the function. The optional `schedule.airflow`, `schedule.dagster` and
`schedule.prefect` blocks control generation with `cartage generate` (see [Orchestrators](#orchestrators)). Run `cartage validate` after editing YAML to check the project and references.

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
