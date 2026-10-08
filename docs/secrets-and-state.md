# Secrets and state

Where secrets come from, and how incremental state and rejected records are kept.

## Secrets

`${secret:sap.passwd}` reads `CARTAGE_SECRET__SAP__PASSWD`, then the project's `.cartage/secrets.yaml` (git-ignored),
then `~/.cartage/secrets.yaml` in your home folder, for secrets several projects share (the project file wins on a
key both have). The dots are nesting levels. Resolved values are never printed. `cartage init --secrets-home` writes
its placeholders to the home file, adding only the keys it doesn't have yet.

`${env:VAR}` reads an environment variable, and `${env:VAR:-default}` falls back to a default when it is not set. It
works in every file: `.cartage/config.yaml` and `answers.yaml` (filled in when the file is read), connection settings,
pipeline options, transform `with:` values and `schedule` settings (filled in when used, so a variable only `prd`
needs doesn't break `dev`), and secret values (`token: "${env:API_TOKEN}"` in `secrets.yaml`).

```yaml
# .cartage/secrets.yaml
sap: { user: rfc_user, passwd: "..." }
snowflake: { connection_string: "snowflake://user:...@account/db?warehouse=wh&role=r" }
```

dlt destinations can also leave credentials out of `.cartage/connections.yaml` entirely: dlt then reads its own
`.dlt/secrets.toml` (in the working directory) or `~/.dlt/secrets.toml` (e.g. `destination_name: my_snowflake` reads
`[destination.my_snowflake.credentials]`).

`${airflow:<conn_id>.<field>}` reads a field of an Airflow connection inside DAGs from `cartage generate`; see
[Airflow connections](orchestrators.md#airflow-connections).

## State and rejects

dlt destinations (`snowflake`, `duckdb`, `postgres`, `filesystem`, ...) keep their state the way any dlt pipeline
does: in the destination (`_dlt_pipeline_state`), loaded together with the data and restored from there on the next
run, so a fresh Airflow worker needs no state settings. Record errors don't hold that state back: the good rows and
the new cursor land together, and rejected records stay in the rejects file. `--full-refresh` drops the pipeline's
tables and state, then loads again.

dlt cannot keep state in SAP or in a file export, so those pipelines use Cartage's state store. It is saved only when a
run has no record errors (or with `--advance-state`), and it lives in `.cartage/state` unless `.cartage/config.yaml` points an
environment at a `filesystem` connection: `state: { prd: { connection: landing, prefix: cartage/state/ } }` (S3, GCS,
Azure, ...). Local state is lost on orchestrator workers, so `cartage generate` refuses a SAP or file export pipeline
whose environment keeps it on local disk. `--full-refresh` ignores the stored state.

Rejected records go to `.cartage/rejects/<pipeline>/<run_id>.jsonl`. `cartage state show|reset <pipeline>` inspects
or clears state, wherever it lives.

With several destinations, each destination is its own run named `<pipeline>__<name>`: state, rejects and the
dlt pipeline are kept per destination, and `state show|reset` covers all of them. `cartage run` goes through the
destinations in order and stops at the first fatal error; destinations that already finished keep their state, so a
rerun only retries the rest. With `--json`, `run` prints one result line per destination and `plan` prints a list.
