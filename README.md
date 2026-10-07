# Cartage

Declarative data migrations. Describe sources, destinations and pipelines in YAML, put custom logic in plain
Python, run locally to test, and generate thin Airflow DAGs, Dagster jobs or Prefect flows for production.

Cartage is an abstraction and simplification layer over data load tools. You describe what to move and how to
reshape it; an engine moves the data. [dlt](https://dlthub.com) is the default engine. The engine is pluggable, so
other load tools can be supported later without changing your pipelines.

```text
  what you write      .cartage/config.yaml · .cartage/connections.yaml · pipelines/*.yaml · transforms/*.py
                                              │
                                              ▼
                ┌───────────────────────── Cartage ─────────────────────────┐
                │  validate · plan · run · state · rejects                  │
                │  secrets: ${secret:…} ${env:…} ${airflow:…}               │
                │  generate: Airflow · Dagster · Prefect  (they call `run`) │
                └─────────────────────────────┬─────────────────────────────┘
                                              │ runs each pipeline on
                ┌───────────────────────── engine ──────────────────────────┐
                │  dlt (default)  │  python (reference)  │  future: others  │
                └───────┬─────────────────────────────────────────┬─────────┘
                        │ reads                                   │ writes
  files (local · S3 · GCS · HTTP) · dlt sources   SAP BAPIs · file exports · dlt destinations
                        └──── your Python transforms in between ──┘
```

![Cartage terminal demo](https://raw.githubusercontent.com/datacoves/cartage/main/docs/cartage-demo.gif)

- **Sources:** files (CSV, JSON Lines, Parquet) in a local folder, S3, GCS, Azure or over HTTP, and any
  [dlt source](https://dlthub.com/docs/dlt-ecosystem/verified-sources/) (REST APIs, databases, SaaS) from YAML or Python.
- **Destinations:** any [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) (Snowflake, DuckDB,
  Postgres, BigQuery, a filesystem data lake, ...), loaded natively by dlt; SAP via BAPIs (ships a mock SAP; the RFC
  transport is planned); and file exports (JSON, JSON Lines, XML, CSV). One pipeline can write to several destinations.
- **Connections** are named after the system (`type: snowflake`, `type: filesystem`, `type: sap_bapi`), never after
  the engine: see [docs/connections.md](docs/connections.md).
- **Engines:** `dlt` (default) and `python`, a plain-loop reference engine for tests (see [Engines](docs/configuration.md#engines)).
- **Orchestrators:** Airflow, Dagster and Prefect. Generated code only calls `cartage run`, so local and production
  run the same code.

## Quickstart

```bash
pip install cartage
cartage init demo --answers https://raw.githubusercontent.com/datacoves/cartage/main/examples/sap/answers.yaml --yes
cd demo
cartage validate
cartage plan materials        # dry run: records before/after transforms and the BAPI payloads
cartage run materials         # reads 20 rows, filters 2, sends 18: 16 load, 2 fail on purpose (exit 1)
cartage run materials --advance-state
cartage run materials         # incremental: nothing new to load
cartage generate              # dags/materials.py
```

The demo runs in `dev` on a sample file and a mock SAP; its `prd` environment reads S3 and calls SAP, once the
`"<fill me>"` placeholders are filled. Run `cartage init <dir>` without `--answers` to create your own project; see
[Creating a project](#creating-a-project).

Split-screen demo: run `cartage sap mock` in one terminal, set `url: http://localhost:8765` on `sap.dev`
in `.cartage/connections.yaml`, and run the pipeline in another terminal.

## Creating a project

`cartage init <dir>` asks a few questions and writes a project that validates straight away:

1. the project name and environments (`dev, prd`; the first is the default),
2. the source system: files (a folder, `s3://`, `gs://`, `az://`, `https://`), a REST API, a SQL database or a Python
   function, plus a few details for it,
3. the destination: any dlt destination (`snowflake`, `duckdb`, `postgres`, ...), `file_export` or `sap_bapi`,
4. an optional sample file: with one, the first environment is fully local (the sample replaces the source through
   `source.envs`, and the destination becomes DuckDB, the SAP mock or a local folder), so `cartage run` works before
   any credentials exist,
5. the pipeline name and an optional schedule (Airflow, Dagster or Prefect).

Credentials become placeholders: settings in `.cartage/connections.yaml` and `${secret:<connection>.<env>.<field>}`
references to `.cartage/secrets.yaml`, both `"<fill me>"`. The fields come from dlt's own credential classes, so every
dlt destination is covered; optional fields are listed as comments. `cartage validate --env <env>` names every
placeholder still to fill.

`--answers <path-or-url>` answers questions from a file (the rest are asked, or take their defaults with `--yes`).
An answers file can also carry pipeline content: `transforms`, `destination_options` and `copy` (files to copy, relative
to the answers file). [`examples/sap/answers.yaml`](examples/sap/answers.yaml) builds the demo above this way.

## Install

Cartage needs Python 3.11.4 or newer. dlt is included; extras add the rest.

| Install                       | Adds                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `pip install cartage`         | CLI, both engines, local files, file exports, SAP (mock), all generators     |
| `pip install "cartage[s3]"`   | `s3://` filesystem connections and state (same as `dlt[s3]`)                 |
| `pip install "cartage[all]"`  | `s3` and `airflow`                                                           |
| `pip install "dlt[snowflake]"`| the driver for a dlt destination (`postgres`, `bigquery`, ...)               |
| `pip install "dlt[gs]"`       | other file locations: `dlt[gs]`, `dlt[az]`, `dlt[http]` (`https://`), `dlt[sftp]` |

`cartage[dlt]` still works; since 0.6 it adds nothing, as dlt is a core dependency.

Generating Airflow, Dagster or Prefect files needs nothing extra; the orchestrator itself runs them. A pipeline can
also list the packages it needs, and `cartage run` adds them with uv: see
[Dependencies](docs/configuration.md#dependencies).

## Commands

| Command                                           | Does                                                                  |
| ------------------------------------------------- | --------------------------------------------------------------------- |
| `cartage init <dir> [--answers <file-or-url>] [--yes]` | create a project from questions or an answers file           |
| `cartage validate [pipelines...]`                 | check YAML, connections, secrets, transforms and mappings; moves no data |
| `cartage plan <pipeline> [-n 3]`                  | dry run: each record before/after transforms (or why it was dropped), and the payloads |
| `cartage run <pipeline>`                          | run it; `--env`, `--engine`, `--full-refresh`, `--advance-state`, `--no-deps`, `--json` |
| `cartage generate [-t airflow\|dagster\|prefect]` | write orchestrator files; `--check`, `--output`, `--show-context <pipeline>` |
| `cartage connections list\|test <name>`           | list connections (secrets as references) or check one                 |
| `cartage state show\|reset <pipeline>`            | inspect or delete incremental state                                   |
| `cartage scaffold airflow\|dagster\|prefect`      | write a template override to customize generated files                |
| `cartage scaffold transform\|source <name>`       | write a transforms or sources module with examples                    |
| `cartage plugins`                                 | list installed adapters and the connection types dlt provides         |
| `cartage sap mock [--port 8765]`                  | run a mock SAP server that validates BAPI calls                       |

Global options go before the command: `-C/--project-dir`, `-v/--verbose`, `--debug` (tracebacks), `--no-color`.

## Project layout

| Path                          | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `.cartage/config.yaml`        | environments, default engine, state location, orchestrator settings          |
| `.cartage/connections.yaml`   | named connections with settings per environment — secrets only as references |
| `pipelines/*.yaml`            | source → transforms → destination(s) (+ schedule)                            |
| `transforms/*.py`             | `map` / `filter` / `batch` functions referenced as `module:function`         |
| `templates/airflow/dag.py.j2` | optional DAG template override (`{% extends "cartage/airflow_dag.py.j2" %}`) |
| `templates/<target>/...`      | optional Dagster/Prefect overrides (`dagster.py.j2`, `prefect.py.j2`)        |
| `.cartage/secrets.yaml`, `state/`, `rejects/` | git-ignored: secrets, local state, rejected records             |
| `~/.cartage/secrets.yaml`     | optional secrets shared by every project (like dlt's `~/.dlt/secrets.toml`) |

## Exit codes

`0` ok · `1` record errors · `2` configuration error · `3` fatal run error (connection, transport, failed dlt load,
`on_error: fail`).

## Documentation

| Guide                                                       | Covers                                                                  |
| ----------------------------------------------------------- | ----------------------------------------------------------------------- |
| [Configuration](docs/configuration.md)                      | `.cartage/config.yaml`, `.cartage/connections.yaml`, pipelines, sources, destinations, dependencies, engines |
| [Connections](docs/connections.md)                          | every connection type: settings vs pipeline options, installs           |
| [Orchestrators](docs/orchestrators.md)                      | Airflow, Dagster and Prefect files, Airflow connections, template overrides |
| [Secrets and state](docs/secrets-and-state.md)              | `${secret:...}`, `${env:...}`, `${airflow:...}`, incremental state, rejects |
| [Database extraction tuning](docs/database-extraction.md)   | drivers, casts, type normalization, row-count checks, dlt settings      |
| [Extending](docs/extending.md)                              | writing your own sources, destinations, engines and orchestrators       |

## License

Apache-2.0
