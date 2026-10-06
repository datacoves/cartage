# `cartage init`: a question-driven project creator

Status: design approved in conversation, awaiting spec review · Target: cartage 0.8.0 (unreleased)

## Goal

Replace today's `cartage init` (a copy of the fixed SAP demo in `project_template/`) with a generic creator. It asks
about environments, the source system, the destination and scheduling, then writes a project that validates
immediately. The only work left is filling in placeholders. The SAP materials demo becomes an ordinary example: an
answers file plus its data, with nothing SAP-specific in `init`.

### Success criteria

- `cartage init <dir>`, answered interactively or from an answers file, writes `cartage.yaml`, `connections.yaml`, one
  pipeline, `.cartage/secrets.yaml` with placeholders, and any supporting files.
- With a sample, `cartage validate --env dev` passes. Without one, `validate` reports only `"<fill me>"` placeholders
  (and passes outright when nothing needs filling, e.g. local files into DuckDB).
- With a sample file, `cartage run` works in `dev` straight away with no credentials, because `dev` is fully local.
- In the real environments, every unfilled value is reported by `validate`/`run` by name and file.
- `cartage init demo --answers <url-or-path of examples/sap/answers.yaml> --yes` reproduces today's demo behaviour
  (20 read, 2 filtered, 18 sent, 16 ok, 2 rejected; then nothing new after `--advance-state`).

### Non-goals (v1)

- Adding pipelines or connections to an existing project (`init` refuses a folder with a `cartage.yaml`). A later
  `cartage add pipeline` can reuse the same engine.
- Generating `${airflow:...}` references. Secrets placeholders only.
- Hand-maintained credential catalogs for dlt destinations.

## Decisions

| Topic | Decision |
| --- | --- |
| Interaction | Interactive prompts with `rich.prompt`; every answer can come from `--answers <path-or-url>`; `--yes` takes defaults for the rest |
| Sample data | `sample_data` is a path (or empty). With a sample, the first environment is fully local: the source is replaced by the sample, and the destination becomes local |
| Source kinds | Files (`filesystem`), REST API (`dlt.sources.rest_api:rest_api_source`), SQL database (`dlt.sources.sql_database:sql_table`), custom Python (`ref:` to a generated stub) |
| Destinations | Any dlt destination, `file_export`, `sap_bapi` |
| Credentials | Derived at run time from dlt's credential classes; Cartage's own types (`sap_bapi`, REST auth) hand-listed |
| Secrets format | Cartage's: `.cartage/secrets.yaml`, keys `<connection>.<env>.<field>`, referenced explicitly with `${secret:...}` |
| Scheduling | One question: none / Airflow / Dagster / Prefect, plus a cron expression |
| SAP demo | `examples/sap/` in the repo: `answers.yaml`, `materials.csv`, `transforms/materials.py` |
| Implementation | Answers → pure `plan_project()` → Jinja templates; no new dependencies |

## Question flow

`cartage init <dir> [--answers <path-or-url>] [--yes]`. Questions already answered by the answers file are skipped;
with `--yes`, unanswered questions take their defaults without prompting. Ids in parentheses are the answers-file keys.

1. **Project name** (`project`) [the folder name]
2. **Environments** (`environments`) [`[dev, prd]`]. The first is the default environment and, with a sample, the
   local one.
3. **Source system** (`source`): `files` / `rest_api` / `sql_database` / `python`, then:
   - `files`: location for the real environments (`files.location`) [`./data`]: a local path, `s3://`, `gs://`,
     `az://`, `https://` or `sftp://`; path glob (`files.path`) [`*.csv`]; format (`files.format`) [`csv`]
   - `rest_api`: base URL (`rest_api.base_url`), endpoint path (`rest_api.path`), resource name
     (`rest_api.resource`) [last path segment], records location (`rest_api.data_selector`) [empty: auto],
     auth (`rest_api.auth`) [`none`]: `none` / `bearer` / `api_key` / `http_basic`
   - `sql_database`: dialect (`sql_database.dialect`) [`postgresql`]: any SQLAlchemy driver name; table
     (`sql_database.table`); schema (`sql_database.schema`) [empty]; incremental cursor column
     (`sql_database.cursor`) [empty: full load]
   - `python`: function name (`python.function`) [`rows`], written to `sources/<pipeline>.py`
4. **Destination** (`destination`): a dlt destination name, `file_export` or `sap_bapi`, then:
   - dlt destination: auth variant (`auth_variant`) when dlt offers several (asked only then); dataset
     (`dataset`) [the pipeline name]; write disposition (`write_disposition`) [`append`]; primary key
     (`primary_key`) when `merge`
   - `file_export`: format (`file_export.format`) [`jsonl`]
   - `sap_bapi`: BAPI (`bapi`) from the bundled BAPI metadata [the first]
5. **Sample file to test with** (`sample_data`) [empty]: a path (or URL) to a CSV, JSONL or Parquet file
6. **Pipeline name** (`pipeline`) [`<source>_to_<destination>`]
7. **Schedule** (`schedule.target`) [`none`]: `none` / `airflow` / `dagster` / `prefect`; cron (`schedule.cron`)
   [`0 3 * * *`]

Answers-file-only keys, for examples that carry pipeline content:

- `transforms`: a list written into the pipeline as-is.
- `destination_options`: merged into the pipeline's destination (e.g. `bapi`, `mapping`, `constants`, `commit`).
- `copy`: extra files to copy into the project, relative to the answers file.
- `sample_data` may be a mapping `{path, format, reader_options, incremental}` instead of a path.

## Generated project

Connection names: the source is `landing` (files); the destination is `warehouse` (dlt SQL and vector destinations),
`lake` (dlt `filesystem`), `exports` (`file_export`) or `sap` (`sap_bapi`); the sample is `samples`. Secret keys for
`ref:` sources use `api` (REST) and `database` (SQL).

| File | Contents |
| --- | --- |
| `cartage.yaml` | `project`, `environments`, `default_env` (the first), `defaults.engine: dlt`, local `state` for every environment, `orchestrators.<target>` when scheduled |
| `connections.yaml` | the source connection (files only), the destination connection, and `samples` (with a sample) |
| `pipelines/<pipeline>.yaml` | source (with `envs.<first>` when sampled), a `transforms:` block (from the answers, else a commented example), the destination with its options, `schedule.<target>` |
| `.cartage/secrets.yaml` | placeholders `"<fill me>"` per `<connection>.<env>.<field>`, with comments for required fields, optional fields and alternatives |
| `data/sample/<file>` | the sample, copied (with a sample) |
| `transforms/__init__.py`, `transforms/<pipeline>.py` | an example `map`, referenced (commented out) in the pipeline; skipped when `copy` provides `transforms/` |
| `sources/__init__.py`, `sources/<pipeline>.py` | `python` sources only: a documented `@dlt.resource` stub |
| any `copy` files | copied as listed |
| `.gitignore` | `.cartage/`, `*.duckdb`, `output/` |

`init` refuses to overwrite any existing file, and refuses a folder that already holds a `cartage.yaml`. After writing,
it prints the files, the placeholders left to fill (per environment) and the next commands: `cartage validate`,
`cartage plan <pipeline>`, `cartage run <pipeline>`.

### Environments and the local first environment

Every real environment gets the same settings, each with its own placeholders. With a sample, the first environment
instead gets:

- **Source:** the pipeline's `source.envs.<first>` replaces the whole source with
  `{connection: samples, path: <file>, format: <from the extension>, ...}` (plus `reader_options`/`incremental`
  from a mapping-style `sample_data`). `samples` is a `filesystem` connection with `bucket_url: ./data/sample`.
- **Destination:** dlt destinations get `<first>: {type: duckdb, credentials: <project>.duckdb}`; `sap_bapi` gets
  `{transport: mock, client: "100"}`; `file_export` gets `{path: output}` (the same in every environment).

Without a sample, the first environment is a real one like the others.

### Example: files on S3 → Snowflake, sample, `[dev, prd]`, Airflow

```yaml
# connections.yaml
connections:
  landing:
    type: filesystem
    envs:
      prd:
        bucket_url: "s3://my-bucket/orders"
        credentials:
          aws_access_key_id: "<fill me>"
          aws_secret_access_key: "${secret:landing.prd.aws_secret_access_key}"
          # aws_session_token: "${secret:landing.prd.aws_session_token}"   # optional
  warehouse:
    type: snowflake
    envs:
      dev: {type: duckdb, credentials: demo.duckdb}
      prd:
        credentials:
          host: "<fill me>"
          database: "<fill me>"
          # username: ...   warehouse: ...   role: ...                     # optional (dlt defaults)
          # auth: fill one of password / private_key (+ private_key_passphrase)
          password: "${secret:warehouse.prd.password}"
          # private_key: "${secret:warehouse.prd.private_key}"
  samples:
    type: filesystem
    envs:
      dev: {bucket_url: ./data/sample}
```

## Credentials from dlt

`cartage/init/credentials.py` provides `credential_fields(spec_source, variant=None, scheme=None)`:

1. **Class.** For a dlt destination: `getattr(dlt.destinations, name)().spec`, the hint of its `credentials` field.
   Unions expand into variants (each a dlt credentials class). SQL sources don't use this: they get one
   connection-string secret (see below).
2. **Variant.** Several variants → the wizard asks (`auth_variant`), labelled from the class name
   (`GcpServiceAccountCredentials` → "GCP service account"). `filesystem` picks by URL scheme: `s3` → AWS,
   `gs`/`gcs` → GCP service account, `az`/`abfss` → Azure, `sftp` → SFTP; local paths and `http(s)` get none.
3. **Fields.** Skip names starting with `_` and defaulted connection-string internals (`drivername`, `query`).
   Required (no default, not optional) → active; optional → commented, showing dlt's default. Secret (dlt's secret
   hint) → value in `.cartage/secrets.yaml`, `${secret:...}` in the setting; non-secret → `"<fill me>"` in
   `connections.yaml`.
4. **Alternatives.** Optional secret fields of one variant are written together under
   `# auth: fill one of ...`, with the first active and the rest commented.
5. **Fallback.** Any introspection failure → a single `credentials: ${secret:<conn>.<env>.credentials}` plus a comment
   linking that destination's dlt docs. Never an error.

Hand-listed (Cartage's own types): `sap_bapi` `prd` settings `transport: rfc`, `ashost`, `sysnr`, `client`, secrets
`user`, `passwd`. REST auth: `bearer` → `token`; `api_key` → `name`, `location`, secret `api_key`; `http_basic` →
`username`, secret `password`.

### `ref:` sources and per-environment secrets

A `ref:` source has one `with:` block for every environment, so its secrets name the environment through a new
`{env}` placeholder in secret references: `${secret:database.{env}.credentials}` resolves with the environment being
run. SQL sources use one connection-string secret per environment, with a commented example built from the dialect
(`postgresql://<username>:<password>@<host>:<port>/<database>`). REST auth secrets use `api.{env}.<field>`.

## Changes outside `init`

1. **`source.envs`.** A pipeline source may hold `envs: {<env>: <source>}`. For that environment, `prepare()` uses the
   given source in place of the whole source block. Validation: the replacement must itself be a valid source.
2. **`{env}` in secret references.** `Secrets.resolve` substitutes `{env}` in `${secret:...}` keys with the run's
   environment.
3. **Unfilled placeholders.** Resolving any setting or secret whose value is `"<fill me>"` raises a configuration
   error naming the key and file, e.g. `warehouse.prd.password is still "<fill me>" (.cartage/secrets.yaml)`.
4. **Removed:** `src/cartage/scaffold.py` and `src/cartage/project_template/`. The test fixture and the README use
   the SAP example instead.

## Answers files

- Model: `InitAnswers` (pydantic, `extra="forbid"`), so a misspelled key fails with its name.
- Source: a path or an `http(s)://` URL. Relative `sample_data` and `copy` entries resolve against the answers file's
  location (`urljoin` for URLs) and are fetched with `urllib.request` (30 s timeout; errors name the URL).
- Safety: `copy` targets and the sample's file name must stay inside the project (no absolute paths, no `..`).
  Answers from a URL print what was fetched and copied, since copied transforms run later.

## SAP example

`examples/sap/` in the repo:

- `answers.yaml`: `environments: [dev, prd]`, `source: files` (`location: s3://my-bucket/materials`,
  `path: materials/*.csv`), `destination: sap_bapi`, `pipeline: materials`,
  `sample_data: {path: materials.csv, reader_options: {dtype: str, keep_default_na: false}, incremental: true}`,
  `transforms` (normalize_uom, is_active, dedupe by material), `destination_options` (bapi, the demo mapping and
  constants, `commit: per_record`), `copy: [transforms/materials.py]`, `schedule: {target: airflow, cron: "0 3 * * *"}`.
- `materials.csv`, `transforms/materials.py`: today's demo files, unchanged.

Without `destination_options`, a `sap_bapi` pipeline gets a commented mapping skeleton from the chosen BAPI's bundled
metadata (`# HEADDATA.MATERIAL: <record field>`), which is adapter knowledge and works for any bundled BAPI.

## Code layout

`src/cartage/init/` replaces `scaffold.py`:

| Module | Responsibility |
| --- | --- |
| `answers.py` | `InitAnswers`, loading from path or URL, defaults, fetching referenced files |
| `questions.py` | interactive prompts; asks only what is unanswered; nothing when `--yes` |
| `credentials.py` | dlt credential introspection and the hand-listed types |
| `plan.py` | `plan_project(answers, fetched_files) -> dict[str, bytes]`, pure |
| `templates/*.j2` | `cartage.yaml`, `connections.yaml`, `pipeline.yaml`, `secrets.yaml`, `gitignore`, `source.py`, `transforms.py` |

`cli.init` wires them: load answers → ask → plan → refuse overwrites → write → print the summary.

## Documentation and demo

- README: the Quickstart becomes `cartage init demo --answers
  https://raw.githubusercontent.com/datacoves/cartage/main/examples/sap/answers.yaml --yes`, then the existing
  commands; a new "Creating a project" section covers the questions, answers files, samples and placeholders; the
  Commands table updates `init`.
- `docs/cartage-demo.tape`: types `cartage init demo` and answers the prompts on screen (files, `sap_bapi`, the sample
  path, Airflow), then runs `validate`, `plan materials -n 3`, `generate`, `cat` the DAG and `run`. Re-recorded with
  `vhs` when available.

## Testing

- **Matrix:** sources {files, rest_api, sql_database, python} × destinations {duckdb, snowflake, file_export,
  sap_bapi} × {with sample, without}. With a sample, `cartage validate --env dev` and `cartage run` succeed in `dev`.
  Without, `validate` in each environment either passes or fails only with `"<fill me>"` errors.
- **SAP example:** the `project` fixture runs `init --answers examples/sap/answers.yaml --yes`; the existing demo tests
  (counts, rejects, incremental, plan, generate) keep proving it.
- **Prompts:** `CliRunner` input, partial answers files, `--yes`.
- **Answers from a URL:** a local `http.server` thread serving `examples/sap/`.
- **Safety:** `copy` with `..` or an absolute path is rejected; an existing `cartage.yaml` is refused.
- **Credentials:** postgres (flat), snowflake (alternatives), bigquery (variants), filesystem `s3://` (scheme), and the
  fallback with a spec that raises.
- **Core changes:** `source.envs`, `{env}` in secret references, and the `"<fill me>"` error, each on its own.
