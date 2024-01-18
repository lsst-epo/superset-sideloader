# Superset Sideloader

Basically a DB table "sideloader" into the Superset world with the intent of programmatically uploading data into Superset.

## DB Config

Set environment variables:

* `DB_HOST`
* `DB_PORT`
* `DB_USER`
* `DB_PASS`
* `TARGET_DB`
* `SUPERSET_DB`

## API

See: `test-payload.json` for an example of the expected request body structure.

`POST`

Required fields:
* `dataset`
* * The tabular data

* `dataset_name`
* * The table name