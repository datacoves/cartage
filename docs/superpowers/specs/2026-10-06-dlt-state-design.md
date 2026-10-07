# State: let dlt keep it

Status: implemented for 0.9 (breaking). Decided: record errors do not hold dlt state back; `--full-refresh` drops tables.

## Today

Every run restores an archived dlt pipelines dir (plus a small `state.json`) from Cartage's state store, runs with
`RESTORE_FROM_DESTINATION=false`, and archives the dir back. The store is a local folder unless `state:` points an
env at a filesystem connection.

All real state is already dlt state: source cursors (`incremental`), the filesystem source's file tracking
(`modification_date` incremental) and schemas. `state.json` only holds `last_run` bookkeeping.

## Change

**dlt destinations** (`snowflake`, `duckdb`, `postgres`, `filesystem`, ...): dlt owns state, as in any dlt project.

- Restore-from-destination on; no archive, no `state.json`. A fresh worker gets its state from `_dlt_pipeline_state`.
- The dlt pipelines dir is a throwaway temp dir per run.
- `last_run` is dropped (dlt's `_dlt_loads` and the run's JSON output cover it).

**Sinks** (`sap_bapi`, `file_export`; any engine): dlt cannot sync state to a sink (`DestinationClient` has no
`WithStateSync`), and SAP cannot hold it. These keep today's archive, stored through `state:`.

- `state:` only matters for pipelines with a sink destination; docs say so.
- `cartage generate` fails when a generated pipeline has a sink destination and its env has no `state: { connection }`
  (local state on an orchestrator worker is lost). Local `.cartage/state` stays the default for laptop runs.

## Commands

| | dlt destination | sink |
|---|---|---|
| `state show` | the dlt pipeline state, synced from the destination | the archive's dlt state (as today) |
| `state reset` | `dlt.pipeline.helpers.drop(..., state_only=True)`: writes a cleared state to the destination | delete the archive (as today) |
| `--full-refresh` | dlt `refresh="drop_resources"`: drops the pipeline's tables and their state, then loads | start from an empty archive (as today) |

## Behaviour that changes

- **Record errors no longer hold state back on dlt destinations.** State is loaded in the same package as the data,
  so the good rows and the new cursor land together; rejected records go to the rejects file and are not retried by
  the next run. Today the next run re-reads everything (and duplicates the good rows on `append`).
  `--advance-state` becomes a sink-only option.
- **`--full-refresh` drops the target tables** on dlt destinations (today it only ignores state, so `append` tables
  get the data twice).
- **`after_load` failures:** data and state are loaded; the run still exits 3, but the next run does not reload.

## Not changing

Rejects files, exit codes, the `python` engine (sinks only, so it uses the sink path), multi-destination run names
(`<pipeline>__<name>` stays the dlt pipeline name, so each destination keeps its own state).
