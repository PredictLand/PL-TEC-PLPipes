# Database

`plpipes` provides a simple way to declare and use multiple database
connections and a set of shortcuts for simplifying some procedures
common in a Data Science context (i.e. running a query and getting
back a DataFrame or creating a new table from a DataFrame).

## Default database

One of the key points of the framework is that a locally stored
[SQLite](https://sqlite.org/) database is always available for usage
with zero setup work.

Also, as most things in PLPipes, that default database (AKA as `work`
database) is also configurable and it can be changed to be, for
instance, a PostgreSQL one running in AWS just for the production
environment or to use a [DuckDB](https://duckdb.org/) one because of
its native [polars](https://www.pola.rs/) support or whatever.

## Database configuration

Database configuration goes under the `db.instance` sub-tree where the
different database connections can be defined.

For instance, a `input` database connection backed by a SQL Server
database running in Azure can be declared as follows:

```yaml
db:
  instance:
    input:
      driver: azure_sql
      server: my-sql-server.database.windows.net
      database: customer-db
      user: predictland
```

The `db.instance.*.driver` key is used to find out which driver to use
to establish the connection.

The `db.instance.*.backend` key is used to stablish the DataFrame
library backend used for the database instance. See
[Database-backends](#database-backends).

The remaining configuration entries are driver specific and as follow:

### DuckDB configuration

- `driver`: `duckdb`
- `file`: name of the database file. Defaults to
    `{instance_name}.duckdb`.

If the instance is named `input` or `output`, the database file is
placed inside the matching directory (for instance,
`input/input.duckdb`).

Otherwise it is placed in the `work` directory (example:
`work/other.duckdb`).

### SQLite configuration

- `driver`: `sqlite`
- `file`: database file name.

Works in exactly the same way as DuckDB but using `sqlite` as the
database file extension.

### Spatialite configuration

Spatialite is an extension of SQLite designed to facilitate the
manipulation of geographic data.

- `driver`: `spatialite`
- `file`: database file name.

The extension must be installed. If you are using Conda it is
available from the `conda-forge` repository and can be installed as
follows:

```bash
conda install libspatialite -c conda-forge
```

Note that Spatialite database files also use the `sqlite` extension.


### SQLServer configuration

- `driver`: `sql_server`
- `server`
- `database`
- `user`
- `password`
- `encrypt`: defaults to `true`.
- `trusted_server_certificate`: defaults to `true`.
- `timeout`: defaults to 60s.

Also, in order to connect to a SQL-Server database the ODBC driver
must be installed. It is available from
[here](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

### AzureSQL configuration

- `driver`: `azure_sql`
- `server`: full qualified server name. It can be seen at the Summary
    page for the database in [Azure
    Portal](https://portal.azure.com). It usually has a name like
    `foo.database.windows.net`).
- `database`: Database instance name. It is usually (always?) also the
    name of the Azure resource.
- `credentials`: Name of the Azure credential group to be used for
    authentication. See the [Azure](#Azure) chapter below.

Example:

```yaml

db:
  instance:
    input:
      driver: azure_sql
      server: foo.database.windows.net
      database: megadb-2000
      credentials: bar

cloud:
  azure:
    auth:
      bar:
        driver: azure_cli

```


Again, the SQL-Server ODBC driver must also be installed.

### InfluxDB

So far, only InfluxDB version 1 is supported. There is nothing wrong
with versions 2 and 3, just that nobody have requested them yet!

- `driver`: `influxdb1`
- `host`
- `port`: defaults to 8086.
- `database`
- `user`
- `password`
- `ssl`: whether to connect using TLS. Defaults to `true`.
- `verify_ssl`: whether to check the remote server TLS certificate. Defaults to `true`.

Note also that connections to InfluxDB v1 databases are handled in a
special way because its python drivers are not compatible with
SQLAlchemy. Some functionality may not be available. For instance,
only the `pandas` backend is currently supported.

### Spark

Spark can be used as a regular database through `plpipes.database`
with the following configuration:

- `driver`: `spark`
- `default_database`: name of the default database, usually composed
  of a catalog name and a database name joined by a dot (for instance,
  `my_catalog.my_database`).

Besides the database configuration, Spark must be configured on its
own. See [Spark](spark.md).

Note that Spark database uses a `spark` backend returning Spark
dataframes by default. A `pandas` backend is also available.

### Other databases configuration

*Not implemented yet, but just ask for them!!!*

## Database usage

[`plpipes.database`](reference/plpipes/database.md) provides a set of
functions for accessing the databases declared in the configuration.

Most of the functions provided accept an optional `db` argument, for
selecting the database instance. When `db` is omitted, `work` is used
as the default.

For example:

```python
from plpipes.database import query, create_table

df = query("select * from order when date >= :ld", {'ld': '2018-01-01'}, db='input')
create_table('recent_orders', df, db='output')
```

A list of the most commonly used functions from `plpipes.database`
follows:

#### `query`

```python
query(sql, parameters=None, db='work')
```

Submits the query to the database and returns a pandas dataframe as
the result.

### `read_table`

```python
read_table(table_name, db="work", columns=None)
```

Reads the contents of the table as a dataframe.

The columns to be loaded can be specified with the `columns` optional
argument.

### `execute`

```python
execute(sql, parameters=None, db='work')
```
Runs a SQL sentence that does not generate a result set.

### `execute_script`

```python
execute_script(sql_script, db='work')
```

Runs a sequence of SQL sentences.

*This method is an unstable state, waiting for a proper implementation to happen :-)*

### `create_table`

```python
create_table(table_name, df, db="work",
             if_exists="replace")

create_table(table_name, sql,
             parameters=None,
             db="work",
             if_exists="replace)
```

This method can be used to create a new table both from a dataframe or
from a SQL sentence.

### `copy_table`

```python
copy_table(source_table_name, dest_table_name=source_table_name,
           source_db="work", dest_db="work", db="work",
           if_exists="replace", **kws)
```
Copies table `source_table_name` from database `source_db` into
`dest_table_name` at database `dest_db`.

### `update_table`

```python
update_table(source_table_name, dest_table_name=source_table_name,
             source_db="work", dest_db="work", db="work",
             key=None, key_dir=">=")
```
Updates table `dest_table_name` at database `dest_db` with the
missing rows `from source_table_name` at `source_db`.

`key` points to a column with monotonic values which is used to
identify the new rows in the source table.

`key_dir` indicates whether the `key` column monotony is strictly
ascending (`>`), ascending (`>=`), descending (`<=`) or strictly
descending (`<`).

For instance, for a date column, whose values always increase, but
which may have duplicates, the right value is `>=`. In other words,
the operator used answers to the question "how are the new values in
the table?"

### `begin`

```python
with begin(db='work') as conn:
    df = conn.query(sql1)
    df = conn.execute(sql2)
    ...
```

This method returns a database connection with an open transaction.

The transaction is automatically commited when the with block is done
unless an exception is raised. In that case, a rollback is performed.


## Connection class

The connection class is returned by calling `begin`.


```python
connection(db='work')
```

Returns a SQLAlchemy connection (created by `begin`).

Also useful for integrating `plpipes` with other third party modules
or for using other `SQLAlchemy` methods not directly wrapped by
`plpipes`.

## Database backends

Besides pandas, which is the de-facto standard in the Python
Data-Science context for representing tabular data, there are other
libraries that for certain problems may be more suitable (for
instance, [geopandas](https://geopandas.org/en/stable/) for the
manipulation of geo-referenced data).

PLPipes has a set of plugable backends controlling how data from the
database is serialized/deserialized into the different DataFrame
implementations.

So far, backends for `pandas` and `geopandas` are provided. Others for
[polars](https://www.pola.rs/), [vaex](https://vaex.io/) or
[dassk](https://www.dask.org/) will be added as the need arises.

A `spark` backend is also available for Spark databases.

In any case, note that changing the backend, usually also requires
changing the code that uses the dataframes as every library provides
its own similar but incompatible API.

Every backend may also accept custom keyword arguments. See [Backend
specifics](#Backend-specifics) bellow.

### Picking the backend

For database write operations (i.e. `create_table`), `plpipes` can
infer which backend to use just looking at the dataframe object type,
so as long as the backend is loaded, `plpipes` will use the right one
automatically.

The function `plpipes.database.load_backend` can be used to load a
specific backend into a database driver:

```python
plpipes.database.load_backend("geopandas", db="input")
```

*Currently, under the hood, backends are attached to the driver
class. Once a backend is loaded, for instance, for a `azure_sql`
database, every other database using such driver will have the backend
available for write operations.*

In the case of read operations, there is no way for `plpipes` to infer
the desired backend and so it must be stated explicitly in one of the
following ways:

1. Passing it as an argument in database read functions
    (i.e. `read_table`, `query`, `query_chunked` and
    `query_group`). For instance:

    ```python
    df = plpipes.database.query(sql, backend="spark")
    ```

2. In the database connection configuration. For instance:

    ```yaml
    db:
      instance:
        work:
          backend: polars
    ```

3. Every database driver can set its own default. For instance,
   currently, the `spatialite` driver sets `geopandas` as its default
   backend.

Read operations transparently call `load_backend` as needed. The
default backend is also loaded automatically when the database is
initialized.

### Backend specifics

#### `pandas` backend

This is the default backend.

#### `geopandas` backend

The `geopandas` backend can handle both `geopandas` and regular
`pandas` dataframes.

In read operations, the argument `geom_col` must be used to indicate
which column contains the geometric data.

If the argument is ommited, the backend returns a regular `pandas` dataframe.

Example:

```python
df = db.query("select * from countries", geom_col="geometry")
```

In order to read geometric data from the database the backend may
mangle the query in order to transform the geometric column values
into the right format for `geopandas.read_postgis`
method. Specifically, in the case of Spatialite, it wraps the
geometric column in the query as `Hex(ST_AsBinary(geom_col))`.

Alternatively, and in order to avoid such processing, the
`wkb_geom_col` argument can be used instead. In that case, it is the
programmer responsability to write a query returning the values in
such colum in a format supported by geopandas (`wkb` stands for [Well
Known
Binary](https://en.wikipedia.org/wiki/Well-known_text_representation_of_geometry)).

