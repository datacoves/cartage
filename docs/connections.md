# Connection types

A connection's `type` names the system it reaches: a file location, a warehouse, SAP. It never names the engine that
moves the data, so pipelines keep working if the engine changes.

Settings live in one of two places:

- **Connection settings** go in `connections.yaml`, under each environment of a named connection. They say *where*
  and *with which credentials*, and change between environments (a local folder in dev, a bucket in Airflow).
- **Pipeline options** go in `pipelines/*.yaml`, next to `connection:` in `source:` or `destination:`. They say *what*
  to read or write, and stay the same in every environment.

```yaml
# connections.yaml                          # pipelines/materials.yaml
connections:                                source:
  landing:                                    connection: landing       # → connection settings
    type: filesystem                          path: materials/*.csv     # → pipeline options
    envs:                                     format: csv
      dev: { bucket_url: ./data }
      airflow: { bucket_url: "s3://bucket/landing" }
```

An environment can set its own `type`, so one connection can be DuckDB on a laptop and Snowflake in Airflow while
pipelines stay the same: `dev: { type: duckdb, credentials: dev.duckdb }`.

Any setting can be a reference instead of a literal: `${secret:key}`, `${env:NAME}` or `${airflow:<conn_id>.<field>}`
(see the README's [Secrets](../README.md#secrets) and [Airflow connections](../README.md#airflow-connections)).
Pipeline `with:` values can use them too.

| Type                                         | Provided by | Source | Destination              | State store | Install                         |
| -------------------------------------------- | ----------- | :----: | :----------------------: | :---------: | ------------------------------- |
| [`filesystem`](#filesystem)                  | dlt         |   ✔    | ✔ data lake              |      ✔      | included; `dlt[s3]`, `dlt[gs]`, `dlt[az]`, `dlt[http]` for remote locations |
| [`snowflake`, `duckdb`, `postgres`, ...](#dlt-destinations) | dlt |  | ✔                     |             | the driver: `dlt[snowflake]`, ... |
| [`file_export`](#file_export)                | Cartage     |        | ✔ one file per run       |             | included                        |
| [`sap_bapi`](#sap_bapi)                      | Cartage     |        | ✔ a BAPI call per record |             | included (mock transport)       |
| [`ref:`](#dlt-sources-ref) (no connection)   | dlt         |   ✔    |                          |             | whatever the source needs       |

`cartage plugins` lists the installed Cartage types and every dlt destination type; `cartage connections list` shows a
project's connections and `cartage connections test <name>` checks one. dlt destination names are reserved, so a
Cartage plug-in can never shadow one.

Every destination also takes `name` in the pipeline: it tells apart two destinations on the same connection (see
[Multiple destinations](../README.md#multiple-destinations)).

## `filesystem`

A folder, a bucket or a web server, through dlt's filesystem support (fsspec). One connection can be a source, a dlt
destination (a data lake) and Cartage's state store (`state: { airflow: { connection: landing, prefix: state/ } }`).

| Connection setting | Required | Meaning                                                                                   |
| ------------------ | :------: | ----------------------------------------------------------------------------------------- |
| `bucket_url`       |    ✔     | `./data` (relative to the project), `/abs/path`, `s3://bucket/prefix`, `gs://...`, `az://...`, `https://host/path`, `sftp://...` |
| `credentials`      |          | for remote locations, as dlt expects them: S3 `{ aws_access_key_id, aws_secret_access_key, region_name, endpoint_url }`, GCS service account fields, Azure `{ azure_storage_account_name, azure_storage_account_key }`, ...; omit to use the platform's default chain (env vars, profile, role) |
| `kwargs`, `client_kwargs` |   | extra fsspec arguments, e.g. `client_kwargs: { endpoint_url: ... }` for S3-compatible stores |
| anything else      |          | as a destination: [dlt filesystem destination](https://dlthub.com/docs/dlt-ecosystem/destinations/filesystem) settings (`layout`, ...) |

| Pipeline option (source) | Default | Meaning                                                                          |
| ------------------------ | ------- | -------------------------------------------------------------------------------- |
| `path`                   | —       | required: a glob under `bucket_url`, e.g. `materials/*.csv` or `population.csv`  |
| `format`                 | `csv`   | `csv`, `jsonl` or `parquet`, read by dlt's typed readers (CSV with pandas)       |
| `reader_options`         | `{}`    | arguments for the reader; for CSV, `pandas.read_csv` arguments such as `{ dtype: str, keep_default_na: false, sep: ";" }` |
| `incremental`            | `false` | `true` reads only files modified since the last run (dlt engine)                 |
| `batch_size`             | `100`   | records per batch (python engine and record destinations)                        |

As a destination, the pipeline options are those of [dlt destinations](#dlt-destinations).

## dlt destinations

Every [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/) is a type: `snowflake`, `duckdb`,
`motherduck`, `postgres`, `redshift`, `bigquery`, `databricks`, `mssql`, `synapse`, `fabric`, `clickhouse`, `athena`,
`dremio`, `sqlalchemy`, `ducklake`, `lancedb`, `qdrant`, `weaviate`, and `filesystem` above. A project
`module:function` returning a dlt destination is a type too (`type: destinations.lake:factory`). They need the dlt
engine (the default).

| Connection setting | Required | Meaning                                                                        |
| ------------------ | :------: | ------------------------------------------------------------------------------ |
| that destination's dlt settings | | `credentials`, `database`, `destination_name`, ... as in the destination's dlt docs; without `credentials`, dlt reads its own `.dlt/secrets.toml` (working directory) or `~/.dlt/secrets.toml` |
| `dataset_name`     |          | default dataset (schema) for every pipeline on this connection                 |
| `naming`           |          | default naming convention (`direct`, `snake_case`, `sql_cs_v1`, `sql_ci_v1` or a project module) |
| `dlt_config`       |          | default dlt config keys for every run, e.g. `{ data_writer.buffer_max_items: 100000 }` |

| Pipeline option (destination) | Default                  | Meaning                                                        |
| ----------------------------- | ------------------------ | -------------------------------------------------------------- |
| `dataset_name`                | connection's, else the pipeline name | dataset (schema) to load into                       |
| `table_name`                  | the source resource name (the pipeline name for `filesystem` sources) | table to load into |
| `write_disposition`           | the source's hint, else `append` | `append`, `replace` or `merge`                         |
| `primary_key`, `merge_key`    | the source's hints       | keys for `merge`                                               |
| `columns`                     | inferred                 | column hints, e.g. `{ price: { data_type: decimal, precision: 18, scale: 4 } }`; also creates columns that are always empty |
| `loader_file_format`          | the destination's        | `parquet`, `jsonl`, `csv`, ...                                 |
| `naming`                      | connection's             | overrides the naming convention                                |
| `dlt_config`                  | connection's             | dlt config keys for this pipeline, merged over the connection's |
| `after_load`                  | none                     | hooks run after a successful load: `module:function` or `{ ref: module:function, with: {...} }`, called as `f(pipeline, **with)` |

## dlt sources (`ref:`)

A source written in Python, or one of dlt's built-in sources, has no connection: set `ref:` instead of `connection:`.
Everything goes in the pipeline; credentials go in `with:` as references.

| Pipeline option (source) | Default      | Meaning                                                                       |
| ------------------------ | ------------ | ----------------------------------------------------------------------------- |
| `ref`                    | —            | required: `module:function` returning a dlt source or resource; your project's (`sources.erp:materials`) or an installed package's (`dlt.sources.rest_api:rest_api_source`, `dlt.sources.sql_database:sql_table`) |
| `with`                   | `{}`         | keyword arguments for that function                                           |
| `resources`              | all selected | the resources to read from a dlt source                                       |
| `incremental`            | none         | `{ cursor: updated_at, initial: "2024-01-01" }`, plus any `dlt.sources.incremental` option: `lag`, `end_value`, `primary_key`, `row_order`, `last_value_func`, `on_cursor_value_missing`, `range_start`, `range_end` (dlt engine) |
| `batch_size`             | `100`        | records per batch (python engine and record destinations)                     |

See the README's [Generic sources](../README.md#generic-sources-no-python) for REST APIs without Python.

## `file_export`

Writes each run to one local file, named and formatted for people or other systems to pick up. Works with both
engines. Unlike a `filesystem` destination (a data lake of load files), it writes one file with a fixed name,
atomically, and supports JSON arrays and XML.

| Connection setting | Required | Meaning                                                |
| ------------------ | :------: | ------------------------------------------------------ |
| `path`             |    ✔     | the output folder, relative to the project or absolute |

| Pipeline option (destination) | Default            | Meaning                                                          |
| ----------------------------- | ------------------ | ---------------------------------------------------------------- |
| `format`                      | `jsonl`            | `json` (an array), `jsonl`, `xml` or `csv`                       |
| `file`                        | `{pipeline}.{ext}` | file name; placeholders `{pipeline}`, `{date}` (`YYYYMMDD`, UTC), `{ext}` |
| `root`                        | `records`          | XML root element                                                 |
| `record`                      | `record`           | XML element per record                                           |

## `sap_bapi`

Calls a BAPI per record and reports each record's result (rejects, warnings). The `mock` transport validates calls
against bundled BAPI metadata; `rfc` is planned.

| Connection setting | Required | Meaning                                                              |
| ------------------ | :------: | -------------------------------------------------------------------- |
| `transport`        |    ✔     | `mock` (in process, or over HTTP with `url`) or `rfc` (not available yet) |
| `url`              |          | `mock` only: a `cartage sap mock` server, e.g. `http://localhost:8765` |
| `client`           |          | SAP client, e.g. `"100"`                                             |
| `ashost`, `sysnr`, `user`, `passwd` |  | `rfc`: application server, system number and credentials          |

| Pipeline option (destination) | Default      | Meaning                                                            |
| ----------------------------- | ------------ | ------------------------------------------------------------------ |
| `bapi`                        | —            | required: the BAPI, e.g. `BAPI_MATERIAL_SAVEDATA`                  |
| `mapping`                     | —            | required: `{ BAPI_PARAMETER: record_field }`, e.g. `HEADDATA.MATERIAL: material`; tables as `PARAM[].FIELD` |
| `constants`                   | `{}`         | `{ BAPI_PARAMETER: value }`, fixed values                          |
| `commit`                      | `per_record` | `per_record`, `per_batch` or `none`                                |

## Migrating from cartage 0.7

| 0.7                                                    | 0.8                                                           |
| ------------------------------------------------------ | ------------------------------------------------------------- |
| `type: dlt` + `destination: snowflake`                 | `type: snowflake` (no `destination:`)                         |
| `type: dlt` + `destination: filesystem`                | `type: filesystem`                                            |
| `type: filesystem` + `path: ./data`                    | `type: filesystem` + `bucket_url: ./data`                     |
| `type: s3` + `bucket`, `prefix`, access keys           | `type: filesystem` + `bucket_url: s3://bucket/prefix` + `credentials` |
| `type: file`                                           | `type: file_export`                                           |
| `type: sap`                                            | `type: sap_bapi`                                              |
| `ref: dlt.sources.filesystem:readers` + `resources: [read_csv]` | a `filesystem` connection + `path` + `format`        |
| CSV values read as text                                | typed; `reader_options: { dtype: str }` for text              |
| rejects name the file and line (`materials.csv:9`)     | rejects carry the record; file sources don't track the line   |
