# Spark

`plpipes` provides an integration layer for Spark/Databricks via the
package `plpipes.spark` and also a set of database backends and
drivers for using the Spark DB engine as a regular database.

**Spark support should still be considered experimental.**


## SparkSession

SparkSession setup is performed using a set of drivers, which handle
different environments.

Currently, the supported drivers are as follow:

- `embedded`: initializes a Spark environment without any external
  infrastructure. Once the Python program ends, the Spark system also
  shuts down.

- `databricks`: connects to a remote Databricks cluster using
  [`databricks-connect`](https://docs.databricks.com/en/dev-tools/databricks-connect/python/index.html).

Every driver accepts a different set of configuration options which
are described below.

### Usage

A Spark session can be accesed as follows:

```python
from plpipes.spark import spark_session

spark = spark_session()
...
```

Any action needed to initialize the session or any other subsystem is
performed automatically.

## Configuration

The Spark layer is configured through the `spark` entry. A `driver`
setting is used to pick which driver to use, any other accepted
configuration is driver-dependent.

Example:

```yaml
spark:
  driver: embedded
  log_level: WARN
  extra:
    spark:
      foo: footomal
```

The entries supported by the different drivers are as follows:

### `embedded` driver

- `app_name`: application name (defaults to `work`).
- `home`: spark working directory (defaults to `work/spark`).
- `extra`: subtree containing extra configuration options which are
  passed verbatim to the `SparkSession.builder.config` method. Note
  that the options accepted by that method usually start by `spark`.
  See the [Spark
  documentation](https://spark.apache.org/docs/latest/configuration.html#available-properties)
  for details.

### `databricks` driver

- `profile`: profile name as defined in the file `~/.databrickscfg`
  (defaults to `DEFAULT`).
- `extra` subtree containing extra configuration options which are
  passed verbatim to the `SparkSession.builder.config` method. Note
  that the options accepted by that method usually start by `spark`.
  See the [Spark
  documentation](https://spark.apache.org/docs/latest/configuration.html#available-properties)
  for details.

## Database integration

Spark database engine can be accessed through the `plpipes.database`
package as any other database.

See [Databases](databases.md) for the details.

## Databricks integration

Access to Spark clusters inside Databricks is provided through the
`databricks` driver. It expects to find a `databricks-connect`
profile already configured in the file `~/.databrickscfg`.

That file can be created automatically using the [Databricks
CLI](https://docs.databricks.com/en/dev-tools/cli/index.html) which
accepts several authentication mechanisms.

The following command can be used to initialize a profile:

```sh
databricks auth login --host <workspace-url>
```

The workspace-url can be taken the host part of the URL from the web
browser when inside the Databricks environment. It usually looks like
`https://adb-xxxxxxxxxxx.azuredatabricks.net`.

Also, the `cluster_id` must be added by hand to the profile. It
appears on the URL when inspecting the cluster inside the Databricks
environment.

Sample `.databrickscfg` file:

```
; The profile defined in the DEFAULT section is to be used as a fallback when no profile is explicitly specified.
[DEFAULT]
host       = https://adb-xxxxxxxxxxxxxxxx.azuredatabricks.net
auth_type  = databricks-cli
cluster_id = 0123-123456-ht3tynfg
```
