# Cartage

Declarative data migrations. Describe sources, destinations and pipelines in YAML, put custom logic in plain
Python, run locally to test, and generate thin Airflow DAGs for production.

![Cartage terminal demo](docs/cartage-demo.gif)

- **Sources:** local CSV folders, S3, and any [dlt](https://dlthub.com) source.
- **Destinations:** SAP via BAPIs (v0.1 ships a mock SAP; RFC is planned).
- **Engines:** `python` (a plain loop) and `dlt`.
- **Orchestrators:** Airflow. The DAG only calls `cartage run`, so local and production run the same code.

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
```

Split-screen demo: run `cartage sap mock` in one terminal, set `url: http://localhost:8765` on `sap_erp.dev`
in `connections.yaml`, and run the pipeline in another terminal.

## Project layout

| Path                          | Purpose                                                                      |
| ----------------------------- | ---------------------------------------------------------------------------- |
| `cartage.yaml`                | environments, default engine, state location, orchestrator settings          |
| `connections.yaml`            | named connections with settings per environment — secrets only as references |
| `pipelines/*.yaml`            | source → transforms → destination (+ schedule)                               |
| `transforms/*.py`             | `map` / `filter` / `batch` functions referenced as `module:function`         |
| `templates/airflow/dag.py.j2` | optional DAG template override (`{% extends "cartage/airflow_dag.py.j2" %}`) |
| `.cartage/`                   | git-ignored: `secrets.yaml`, `state/`, `rejects/`                            |

## YAML configuration

Cartage uses three YAML layers: `cartage.yaml` sets project-wide defaults, `connections.yaml` defines named services
per environment, and each `pipelines/*.yaml` file describes one source-to-destination flow.

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

Each pipeline names a source connection, applies an ordered list of transforms, and writes to a destination. A step
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
package; `with` passes keyword arguments to the function. The optional `schedule.airflow` block controls DAG generation
with `cartage generate`. Run `cartage validate` after editing YAML to check the project and references.

## Batches

- `batch` transforms see one source batch at a time (default 100 rows, `batch_size` source option), never across files.
- `commit: per_batch` commits each batch the engine hands to the destination (the dlt engine re-chunks at 100).

## Secrets

`${secret:sap.passwd}` reads `CARTAGE_SECRET__SAP__PASSWD`, then `.cartage/secrets.yaml`. `${env:VAR}` reads an
environment variable. Resolved values are never printed.

## State and rejects

Incremental state is saved only when a run has no record errors (or with `--advance-state`). Rejected records
go to `.cartage/rejects/<pipeline>/<run_id>.jsonl`. `cartage state show|reset <pipeline>` inspects or clears state.

## Exit codes

`0` ok · `1` record errors · `2` configuration error · `3` fatal run error (connection, transport, `on_error: fail`).

## Extending

Adapters are entry points in the groups `cartage.sources`, `cartage.destinations`, `cartage.engines` and
`cartage.orchestrators`. `cartage plugins` lists what is installed.

## License

Apache-2.0
