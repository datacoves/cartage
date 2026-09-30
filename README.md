# Cartage

Declarative data migrations. Describe sources, destinations and pipelines in YAML, put custom logic in plain
Python, run locally to test, and generate thin Airflow DAGs for production.

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
cartage run materials         # loads 20 materials into the mock SAP; 2 fail on purpose
cartage run materials --advance-state
cartage run materials         # incremental: nothing new to load
cartage generate              # dags/materials_to_sap.py
```

Split-screen demo: run `cartage sap mock` in one terminal, set `url: http://localhost:8765` on `sap_erp.dev`
in `connections.yaml`, and run the pipeline in another terminal.

## Project layout

| Path | Purpose |
|---|---|
| `cartage.yaml` | environments, default engine, state location, orchestrator settings |
| `connections.yaml` | named connections with settings per environment — secrets only as references |
| `pipelines/*.yaml` | source → transforms → destination (+ schedule) |
| `transforms/*.py` | `map` / `filter` / `batch` functions referenced as `module:function` |
| `templates/airflow/dag.py.j2` | optional DAG template override (`{% extends "cartage/airflow_dag.py.j2" %}`) |
| `.cartage/` | git-ignored: `secrets.yaml`, `state/`, `rejects/` |

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
