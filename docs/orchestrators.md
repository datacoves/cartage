# Orchestrators

Generating Airflow DAGs, Dagster jobs and Prefect flows, and customizing them.

## Orchestrators

`cartage generate --target airflow|dagster|prefect` writes one file per pipeline that has a `schedule.<target>` block.
Each file only runs `cartage --project-dir <project> run <pipeline> --env <env>`, so deploy the Cartage project (with
`transforms/`) next to it. Settings merge: built-in defaults ← `orchestrators.<target>` in `.cartage/config.yaml` ← the
pipeline's `schedule.<target>` (dicts merge by key). Add `--check` in CI to fail on stale files.

```yaml
schedule:
  airflow: { schedule: "0 3 * * *", tags: [sap] }
  dagster: { schedule: "0 3 * * *", tags: { team: data }, timezone: Europe/Brussels }
  prefect: { schedule: "0 3 * * *", tags: [sap], retries: 2 }
```

| Target  | Output (default)                        | Settings                                                                    | Environment at run time  |
| ------- | --------------------------------------- | --------------------------------------------------------------------------- | ------------------------ |
| airflow | `dags/<dag_id>.py`                      | `dags_dir`, `schedule`, `tags`, `default_args`, `task_decorator`, `operator_args`, `task_env`, `image`, ... | Airflow var `cartage_env` |
| dagster | `orchestration/dagster/<name>.py`       | `out_dir`, `name`, `schedule`, `timezone`, `tags`, `env`, `command`, `project_dir` | `CARTAGE_ENV` env var  |
| prefect | `orchestration/prefect/<name>.py`       | `out_dir`, `name`, `schedule`, `retries`, `tags`, `env`, `command`, `project_dir`  | `CARTAGE_ENV` env var  |

- **Dagster:** each file defines a job, its schedule, and `defs = dg.Definitions(...)`. Load it with
  `dagster dev -f orchestration/dagster/<name>.py` (repeat `-f` per pipeline), or merge the `defs` objects.
- **Prefect:** each file defines the `cartage_run` flow. `python orchestration/prefect/<name>.py` serves it on the
  schedule, or point `prefect deploy` at `orchestration/prefect/<name>.py:cartage_run`.
- `env` defaults to `prd`; `command` (default `cartage`) may include a launcher, e.g. `uv run cartage`.
- **Airflow:** DAGs use the TaskFlow API (`@dag`, `@task.bash`). `task_decorator` picks the task decorator, e.g.
  `datacoves_bash` for `@task.datacoves_bash`; `operator_args` are its keyword arguments. `task_env` adds
  environment variables to the task (e.g. `UV_CACHE_DIR`). See [Airflow settings](#airflow-settings) for
  owner/email and functions that build `default_args` or the schedule.

### Airflow settings

```yaml
# .cartage/config.yaml
orchestrators:
  airflow:
    task_decorator: datacoves_bash
    command: uvx --from "cartage>=0.11.0" cartage
    default_args: { owner: data-team, email: [data@example.com], retries: 1 }

# pipelines/loans.yaml
schedule:
  airflow:
    schedule: "0 3 * * *"
    default_args: { owner: Noel Gomez, email: [noel@example.com] }
```

- Packages are not an Airflow setting: the DAG runs `cartage run`, which adds the pipeline's
  [dependencies](configuration.md#dependencies) itself.
- `default_args` merge by key, so the owner and email can be set once and overridden per pipeline.
- Values are written into the DAG as Python literals. When `default_args` need Python (timedeltas, callbacks), point
  `default_args_from` at a function that builds them; `default_args` become its keyword arguments.
  `schedule_from` wraps the schedule the same way, e.g. to turn it off outside production:

  ```yaml
  default_args_from: orchestrate.utils.datacoves_utils:set_default_args
  default_args: { owner: Noel Gomez, owner_email: noel@example.com }
  schedule_from: orchestrate.utils.datacoves_utils:set_schedule
  ```

  renders `default_args = set_default_args(owner='Noel Gomez', owner_email='noel@example.com')` and
  `schedule=set_schedule('0 3 * * *')`.
- For anything else, override the template: see [Customizing the generated files](#customizing-the-generated-files).

### Customizing the generated files

Settings cover most needs. For the rest, a project template overrides parts of Cartage's own, and
`cartage scaffold <target>` writes one to start from:

| Command                     | Writes                            | Blocks                                                          |
| --------------------------- | --------------------------------- | --------------------------------------------------------------- |
| `cartage scaffold airflow`  | `templates/airflow/dag.py.j2`     | `header`, `imports`, `default_args`, `dag`, `schedule`, `task`, `extra` |
| `cartage scaffold dagster`  | `templates/dagster/dagster.py.j2` | `header`, `imports`, `op`, `definitions`, `extra`               |
| `cartage scaffold prefect`  | `templates/prefect/prefect.py.j2` | `header`, `imports`, `flow`, `serve`, `extra`                   |

The file extends Cartage's template and lists every block with `{{ super() }}`, so `cartage generate` writes the
same files as before until you change a block. Replace a block's `{{ super() }}`, or add lines before or after it,
and delete the blocks you don't change. `cartage generate --target <target> --show-context <pipeline>` prints the
variables a block can use:

```jinja
{% extends "cartage/airflow_dag.py.j2" %}

{% block extra %}
{{ super() -}}
# Runs after the DAG is defined, e.g. to add a sensor or a second task.
{% endblock %}
```

### Airflow connections

In Airflow, credentials usually live in Airflow connections. Cartage reads them with `${airflow:...}` references, so
nothing has to copy them into variables by hand and they never appear in YAML or in the generated DAG.

**1. Reference connection fields** in `.cartage/connections.yaml`, in the environment the DAGs run (the `cartage_env` Airflow
variable, `prd` by default):

```yaml
warehouse:
  type: snowflake
  envs:
    dev: { destination_name: datacoves_snowflake }   # credentials from ~/.dlt/secrets.toml locally
    prd:
      credentials:
        host: "${airflow:main_load_keypair.extra.account}"
        username: "${airflow:main_load_keypair.login}"
        private_key: "${airflow:main_load_keypair.extra.private_key_content}"   # PEM, as stored
        warehouse: "${airflow:main_load_keypair.extra.warehouse}"
        role: "${airflow:main_load_keypair.extra.role}"
```

| Reference                          | Airflow connection field              |
| ---------------------------------- | ------------------------------------- |
| `${airflow:<conn_id>.login}`       | Login (also `password`, `host`, `schema`, `port`) |
| `${airflow:<conn_id>.extra.<key>}` | a key of the Extra JSON; nest with dots (`extra.auth.token`) |

**2. Generate the DAGs.** `cartage generate` collects every `${airflow:...}` used by the pipeline (its source,
destinations, state store and options, in every environment) and passes them to the task as environment variables
whose values are Airflow templates:

```python
@task.bash(
    env={"CARTAGE_AIRFLOW__MAIN_LOAD_KEYPAIR__LOGIN": "{{ conn.get('main_load_keypair').login or '' }}", ...},
    append_env=True,
)
def cartage_run():
    return "cartage --project-dir ... run pipelines/loans.yaml --env {{ var.value.get('cartage_env', 'prd') }}"
```

**3. Airflow fills them in when the task runs**, from wherever it keeps connections (metadata database, a secrets
backend such as Vault or AWS Secrets Manager, or `AIRFLOW_CONN_<ID>` variables), and masks passwords in the task log.

**4. `cartage run` resolves each reference** from its `CARTAGE_AIRFLOW__<CONN_ID>__<FIELD>` variable (dots and dashes
become `__` and `_`). Values are never printed.

Notes:

- Rerun `cartage generate` when references change; `cartage generate --check` in CI catches stale DAGs.
- A field that is empty in the connection resolves to `""`. A missing `extra` key fails the task when Airflow renders
  it (Airflow's `StrictUndefined`), naming the key.
- Outside Airflow, `cartage run`/`validate` stop at the first `${airflow:...}` with a hint naming the variable. Use
  another environment locally (`${secret:...}`, `${env:...}` or the driver's own config), or export the variable.
- To test a DAG without a real connection, define one inline:
  `AIRFLOW_CONN_MAIN_LOAD_KEYPAIR='{"conn_type": "snowflake", "login": "...", "extra": {...}}' airflow dags test <dag_id>`.
- Dagster and Prefect pass secrets to tasks as environment variables; read them with `${env:...}`.
