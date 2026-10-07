# Extending Cartage

Writing your own sources, destinations, engines and orchestrators.

## Extending

Adapters are entry points in the groups `cartage.sources`, `cartage.destinations`, `cartage.engines` and
`cartage.orchestrators`. `cartage plugins` lists what is installed.

A destination is a record sink (`preview` + `write`, like `sap_bapi` and `file_export`) by default. Optional: `finish(ok)` is called
after the run (to finalize or discard output), and `preview_label`/`preview_syntax` name and highlight `plan` output. To have the dlt engine load into it natively
instead, expose `dlt_destination()` returning a dlt destination, plus `hints` (resource hints such as
`write_disposition`), `dataset_name`, `loader_file_format` and `dlt_env` (env vars applied to the run), as the dlt
destination adapter does. dlt destination names (`snowflake`, `filesystem`, ...) are reserved: a plug-in with one of
those names is an error.
