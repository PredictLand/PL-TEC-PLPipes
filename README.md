---
title: PLPipes - PredictLand Data Science Framework
---

# Introduction

PredictLand is a consultancy firm focused on Data Science and related
fields, Data Analytics, AI and ML, Big Data, Data Engineering, etc. We
work for customers that range from small companies with just a few
employees to big corporations and that requires us to be very flexible
in the way we work as the platforms, IT systems and tools we can use
in every project vary greatly.

More specifically it is not uncommon for us to work on projects where
all the infrastructure we have available to carry on our tasks are
just our laptops! Yes, that's it, no fancy environments like
Databricks or Snowflake, no cloud instances with hundreds of RAM GBs,
no data automation services as Azure Data Factory or DBT, sometimes
not just a simple Database server... It is just our laptops, a git
repository and maybe some Excel files with the data.

So that's one way to think about PLPipes, a Data Science framework
on the cheap, a poor-man Databricks replacement!

But even if that is true, we prefer to think of PLPipes as a **lean
and very scalable framework**. Something that you can use to train
some models from a few CSVs in your laptop, to process terabytes of
information on a cloud cluster, to embed in a lambda function, to run
some model inside a Docker container, etc.

So, what is exactly PLPipes?

Several things:

1. It is a thin layer **integrating** several technologies so that
   they can be used easily and efficiently to solve common data
   science problems.

2. It is an **automation** framework for creating data processing
   pipelines.

3. It is a **programming** framework for reducing boilerplate,
   enforcing some best-practices and providing support for common
   tasks.

4. It is also a **mindset** and a way to **standardize** Data Science
   project development.

5. It is a very **customizable** framework with **sane defaults**, so
   that you can start working on your projects right there without
   having to perform a complex setup up front.

6. It is a **work in process** yet! Even if the ideas behind PLPipes
   are not new and we have used/implemented them in different forms
   and in different projects in the past (or in some cases, just
   copied them from other 3rd party projects), the framework is still
   very new and most of it should be considered experimental!

# Nomenclature

We use `PLPipes` to refer to the framework as a whole (projects, code,
conventions, mindset, etc.) and `plpipes` to refer specifically to the
Python package.

# Overview

So, how is the typical PLPipes project?

PLPipes projects are organized around [**actions**](#Actions) which
can be considered as atomic units of work. Examples of actions are
downloading a file, transforming some data or training a model.

Actions are grouped in sequences to create data processing
pipelines. Several pipelines can be defined inside one project, and it
is even possible to change which actions form a pipeline dynamically
depending on the deployment environment, the configuration, command
line arguments, etc.

Another key concept of PLPipes is that a relational
[**database**](#Database) is used to pass information between actions
(alternatively the file system can be used, but the database is
preferred).

The pipelines are launched by the [**runner**](#Runner), which is
nothing else than a Python script that calls into `plpipes` and is
able to handle command line arguments, configuration files and
environment variables in a unified way.

In summary, when using PLPipes, instead of a bunch of scripts, every
one doing something different, we have a set of pipelines built on top
of actions that use a relational database to store intermediate data
and we use a standardized python script to get everything running.

Finally other of the key features available from PLPipes is a powerful
[configuration](#Configuration) system.

# Project Setup

This chapter describes how to set up a PLPipes project from scratch.

PLPipes is quite configurable and most of its workings can be changed
and redefined, but that doesn't preclude it from offering some sane
defaults that we advise you to follow.

Specifically, by default, it expects some directory structure and a
main script which is used to organize the project operations as
described in the following sections:

## Directory structure

A PLPipes project is structured in the following directories which
should be created by hand (development of a utility to do it
automatically is planed).

* `lib` (optional): This is where reusable Python modules specific to
  the project are stored.

* `bin`: This is the place where to place scripts for the
  project. Though, usually if just contains [the main
  script](#The-main-script) `run.py`.

  Other scripts can be placed here, but it should be noted that the
  [Actions](#Actions) mechanism available through `run.py` is the
  preferred way to organize the project operations.

* `actions`: Action definitions. See [Actions](#Actions) below.

* `notebooks` (optional): Jupyter notebooks go here

* `config`: Configuration files are stored here. See
  [Configuration](#Configuration).

* `defaults` (optional): Default configuration files go here (the
  contents of this directory should be committed to git).

  The semantic distinction between `defaults` and `config` is
  something we are still considering and that may change.

* `input` (optional): Project input files.

* `work`: Working directory, intermediate files go here.

  Also, the default working database is stored here as
  `work/work.duckdb`.

* `output` (optional): Final output files generated can go here.

* `venv` (optional): Even if `plpipes` does not depend on it, we
  recommend to use a virtual environment for the project whit that
  name.

## The main script

 `bin/run.py` is the main entry point for PLPipes and should be
created by hand with the following content:

```python
#!/usr/bin/env python3
from plpipes.runner import main
main()
```

## Installing `plpipes`

The Python module `plpipes` can be installed in two ways.

### Installing a packed version

This is the way to install the module we recommend when you don't want
to contribute to the development of the framework and just want to use
it.

*Note that In practice, as `plpipes` is still in a very early
development stage, that may not be a realistic assumption and you may
be required to switch to the development version available from git
quite soon!*

Somehow (!) obtain the module wheel and install it using pip:

```bash
pip install /path/to/.../plpipes-0.1-py2.py3-none-any.whl
```

Hopefully, `plpipes` would be directly available from
[PyPI](https://pypi.org/) soon!

### Installing from git

1. Clone the repository outside of your project directory and switch
   to the `develop` branch:

   ```bash
   git clone git@github.com:PredictLand/PL-TEC-PLPipes.git
   cd PL-TEC-PLPipes
   git checkout develop
   ```

2. Add the `src` subdirectory to Python search path.

   ```bash
   # Linux and/or bash:
   export PYTHONPATH=path/to/.../PL-TEC-PLPipes/src

   # Windows
   set PYTHONPATH=C:\path\to\...\PL-TEC-PLPipes\src
   ```

3. Check that it works:

   ```bash
   python -m plpipes -c "print('ok')"
   ```

Alternatively you can modify your project main script to append
the`src` directory to the module search path so that you don't need to
set `PYTHONPATH` by hand every time you start a new session.

For instance:

```python
from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent.parent.parent / "PL-TEC-PLPipes/src"))

from plpipes.runner import main
main()
```

Or you could also set `PYTHONPATH` from your shell startup script
(`~/.profile`) or in the Windows registry.

# Using PLPipes

PLPipes is comprised of several modules which can be used together or
independently.

## Configuration

The configuration module is one of the core components of PLPipes
pervasively used by `plpipes` itself, so even if you don't want to use
it directly in your project it would be used internally by the
framework.

Configuration data is structured in a global tree-like object which is
initialized from data read from several files in sequence and from the
command line.

Both YAML and JSON files are supported (though, we recommended YAML
usage as it is usually easier to read by humans).

When the same setting appears in several configuration files, the last
one read is the one that prevails.

### File structure

The list of files from with the configuration is read is dynamically
calculated based on two settings:

* The script "stem": It is the name of the script run without the
  extension (for instance, the stem for `run.py` is `run`).

  When `plpipes` is used from a Jupyter notebook, the stem can be
  passed on the `%plpipes` line magic:

  ```python
  %plpipes foobalizer
  ```

- The deployment environment (`dev`, `pre`, `pro`, etc.): this can be
  set from the command line or using the environment variable
  `PLPIPES_ENV` (see [Environment variables](#Environment-variables)
  below). It defaults to `dev`.

Also, there are two main directories where configuration files are
stored:

- `default`: This directory should contain configuration files that
  are considered defaults and that are not going to be changed by the
  project users. We think of it as the place where to place setting
  that otherwise would be hard-coded.

- `config`: This directory contains configuration files which are
  editable by the project users or where developers can put temporary
  settings they don't want to push into git.

*We are currently considering whether this division makes sense or if
we should otherwise replace it by something better*

When PLPipes configuration module is initialized it looks in those two
directories for files whose names follow the following rules:

1. Base name: the base name is taken as `common` or the stem so that,
   for instance, when loading the configuration from `run.py`, both
   `common.yaml` and `run.yaml` files would be taken into account.

2. Secrets: files with a `-secrets` post-fix are also loaded (for
   instance, `common-secrets.yaml` and `run-secrets.yaml`).

2. Environment: files with the deployment environment attached as a
   post-fix are also loaded (`run-dev.yaml` or `run-secrets-dev.yaml`).

Additionally two user-specific configuration files are
considered. Those are expected to contain global configuration
settings which are not project specific as API keys, common database
definitions, etc.

```
~/.config/plpipes/plpipes.yaml
~/.config/plpipes/plpipes-secrets.yaml
```

Finally, when using the default runner (See [Runner](#Runner) below),
the user can request additional configuration files to be loaded.

In summary, the full set of files which are consider for instance,
when the `run.py` script is invoked in the `dev` environment is as
follows (and in this particular order):

```
~/.config/plpipes/plpipes.json
~/.config/plpipes/plpipes.yaml
~/.config/plpipes/plpipes-secrets.json
~/.config/plpipes/plpipes-secrets.yaml
default/common.json
default/common.yaml
default/common-dev.json
default/common-dev.yaml
default/common-secrets.json
default/common-secrets.yaml
default/common-secrets-dev.json
default/common-secrets-dev.yaml
default/run.json
default/run.yaml
default/run-dev.json
default/run-dev.yaml
default/run-secrets.json
default/run-secrets.yaml
default/run-secrets-dev.json
default/run-secrets-dev.yaml
config/common.json
config/common.yaml
config/common-dev.json
config/common-dev.yaml
config/common-secrets.json
config/common-secrets.yaml
config/common-secrets-dev.json
config/common-secrets-dev.yaml
config/run.json
config/run.yaml
config/run-dev.json
config/run-dev.yaml
config/run-secrets.json
config/run-secrets.yaml
config/run-secrets-dev.json
config/run-secrets-dev.yaml
```

### Automatic configuration

There are some special settings that are automatically set by the
framework when the configuration is initialized:

- `fs`: The file system sub-tree, contains entries for the main project
  subdirectories (`root` which points to the project root directory,
  `bin`, `lib`, `config`, `default`, `input`, `work`, `output` and
  `actions`).

- `env`: The deployment environment

- `logging.level`: The logging level.

All those entries can be overridden in the configuration files.

### Wildcards

In order to simplify the declaration of similar configuration
subtrees, a wildcard mechanism is provided.

Entries named `*` (an asterisk) are copied automatically into sibling
configurations.

For instance, in the following configuration most of the database
connection parameters for `input` and `work` instances are obtained
from the `*` entry.

```yaml
db:
  instance:
    '*':
      driver: azure_sql
      server: example.databse.windows.net
      user: jtravolta
      password: grease78

    input:
      database: data_source

    work:
      database: tempdb
```

### Python usage

The configuration is exposed through the `plpipes.cfg` object.

It works as a dictionary which accepts dotted entries as keys. For
instance:

```python
from plpipes import cfg
print(f"Project root dir: {cfg['fs.root']}")
```

A sub-tree view can be created using the `cd` method:

```python
cfs = cfg.cd('fs')
print(f"Project root dir: {cfs['root']}")
```

Most dictionary methods work as expected. For instance it is possible
to mutate the configuration or to set defaults:

```python
cfg["my.conf.key"] = 7
cfg.setdefault("my.other.conf.key", 8)
```

Though note that configuration changes are not backed to disk.

### Config Initialization

The method `init` of the module `plpipes.init` is the one in charge of
populating the `cfg` object and should be called explicitly in scripts
that want to use the configuration module without relying in other
parts of the framework.

`plpipes.init.init` is where the set of files to be loaded based on
the stem and on the deployment environment is calculated and where
they are loaded into the configuration object.

[Automatic configuration](#Automatic-configuration) is also performed by
this method.

Note that `plpipes.init` is a low level package that is not expected
to be used directly from user code. Instead you should use the methods
provided in `plpipes.runner` which take care of initializing the
environment and also the configuration subsystem.

## Database

`plpipes` provides a simple way to declare and use multiple database
connections and a set of shortcuts for simplifying some procedures
common in a Data Science context (i.e. running a query and getting
back a DataFrame or creating a new table from a DataFrame).

### Default database

One of the key points of the framework is that a locally stored
[DuckDB](https://duckdb.org/) database is always available for usage
with zero setup work.

Also, as most things in PLPipes, that default database (AKA as `work`
database) is also configurable, so for instance, it can be changed to
be a PostgreSQL one running in AWS for the production environment or
to use a SQLite one because of its GIS support or whatever.

### Database configuration

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
[Database-backends](#Database_backends).

The remaining configuration entries are driver specific and as follow:

#### DuckDB configuration

- `driver`: `duckdb`
- `file`: name of the database file. Defaults to
  `{instance_name}.duckdb`.

If the instance is named `input` or `output`, the database file is
placed inside the matching directory (for instance,
`input/input.duckdb`).

Otherwise it is placed in the `work` directory (example:
`work/other.duckdb`).

#### SQLite configuration

- `driver`: `sqlite`
- `file`: database file name.

Works in exactly the same way as DuckDB but using `sqlite` as the
database file extension.

#### Spatialite configuraion

Spatialite is an extension of SQLite designed to facilitate the
manipulation of geographic data.

- `driver`: `spatialite`
- `file`: databse file name.

The extension must be installed. If you are using Conda it is
available from the `conda-forge` repository and can be installed as
follows:

```bash
conda install libspatialite -c conda-forge
```

Note that Spatialite database files also use the `sqlite` extension.


#### SQLServer configuration

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

#### AzureSQL configuration

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

#### Other databases configuration

*Not implemented yet, but just ask for them!!!*

### Database usage

`plpipes.database` provides a set of functions for accessing the
databases declared in the configuration.

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

##### `query`

```python
query(sql, parameters=None, db='work')
```

Submits the query to the database and returns a pandas dataframe as
the result.

#### `read_table`

```python
read_table(table_name, db="work", columns=None)
```

Reads the contents of the table as a dataframe.

The columns to be loaded can be specified with the `columns` optional
argument.

#### `execute`

```python
execute(sql, parameters=None, db='work')
```
Runs a SQL sentence that does not generate a result set.

#### `execute_script`

```python
execute_script(sql_script, db='work')
```

Runs a sequence of SQL sentences.

*This method is an unstable state, waiting for a proper implementation to happen :-)*

#### `create_table`

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

#### `copy_table`

```python
copy_table(source_table_name, dest_table_name=source_table_name,
           source_db="work", dest_db="work", db="work",
           if_exists="replace", **kws)
```
Copies table `source_table_name` from database `source_db` into
`dest_table_name` at database `dest_db`.

#### `update_table`

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

#### `begin`

```python
with begin(db='work') as conn:
    df = conn.query(sql1)
    df = conn.execute(sql2)
    ...
```

This method returns a database connection with an open transaction.

The transaction is automatically commited when the with block is done
unless an exception is raised. In that case, a rollback is performed.


### Connection class

The connection class is returned by calling `begin`.


```python
connection(db='work')
```

Returns a SQLAlchemy connection (created by `begin`).

Also useful for integrating `plpipes` with other third party modules
or for using other `SQLAlchemy` methods not directly wrapped by
`plpipes`.

### Database backends

Besides pandas, which is the de-facto standard in the Python
Data-Science context for representing tabular data, there are other
libraries than for certain problems may be more suitable (for
instance, [geopandas](https://geopandas.org/en/stable/) for the
manipulation of geo-referenced data).

PLPipes has a set of plugable backends controlling how data from the
database is serialized/deserialized into the different DataFrame
implementations.

So far, backends for `pandas` and `geopandas` are provided. Others for
[polars](https://www.pola.rs/), [spark](https://spark.apache.org/),
[vaex](https://vaex.io/) or [dassk](https://www.dask.org/) will be
added as the need arises.

In any case, note that changing the backend, usually also requires
changing the code that uses the dataframes as every library provides
its own similar but incompatible API.

Every backend may also accept custom keyword arguments. See [Backend
specifics](#Backend-specifics) bellow.

#### Picking the backend

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

#### Backend specifics

##### `pandas` backend

This is the default backend.

##### `geopandas` backend

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

## Actions

Actions are the atomic units of work that when combined allow one to
perform the tasks required by the project.

They are defined inside the `actions` directory in a hierarchical way.

There are several types of actions predefined and also new ones can be
added.

Actions are declared with a configuration file with the name of the
action, for instance `preprocessor.yaml`.

Inside this configuration file the action type must be declared using
the `type` setting. For instance:

```yaml
type: python_script
```

Alternatively, `plpipes` can autodetect an action type when it finds a
file with the action name and some recognized extension (for example,
`model_training.py`). In that case the configuration file is not
required.

The list of currently supported action types follows:

### `python_script`

Extension: `.py`

The python code in the file is executed.

The following objects are directly available in the script:

- `plpipes`: the main `plpipes` package.

- `cfg`: the configuration object.

- `action_cfg`: the action configuration (read from the action yaml
  file or from the global configuration).

- `db`: a shortcut for the `plpipes.database` package.

### `sql_script`

Extension `.sql`

Runs the SQL sentences in the action file against the `work` database.

The SQL code is preprocessed using
[Jinja](https://jinja.palletsprojects.com/en/3.1.x/). That feature can
be used to for instance, set values from the configuration:

```sql
CREATE TABLE foo AS
SELECT * FROM bar
WHERE data >= "{{ cfg["data.limits.date.low.cutoff"] }}"
```

*Currently this action type is only supported when `work` is backed by
a SQLite database.*

### `sql_table_creator`

Extension `.table.sql`

Runs the SQL query in the file and stores the output data frame in a
new table with the name of the action.

Jinja is also used to preprocess the SQL statement.

### `qrql_script`

Extension: `.prql`

[PRQL](https://prql-lang.org/) (Pipelined Relational Query Language)
is an alternative query language for relational databases.

This action runs the PRQL sentences in the file against the `work`
database.

Jinja is used to preprocess the PRQL statement.

*Currently this action type is only supported when `work` is backed up
by a SQLite database.*

### `qrql_table_creator`

Runs the PRQL query in the file and stores the output data frame in a
new table with the name of the action.

Jinja is also used to preprocess the PRQL statement.

### `quarto`

Extension: `.qmd`

Processes the file using [quarto](https://quarto.org/).

The following configuration options can be used:

- `dest`:
  - `key`: any of `work`, `input` or `output`

  - `dir`: destination dir to store the generated files.

  - `file`: destination file name. Defaults to the action name with
    the extension associated to the output format.

  - `format`: output format.

The action configuration can also be included directly in the `qmd`
yaml header, under the `plpipes` branch.

### `sequence`

Runs a set of actions in sequence.

The list of actions to be run are declared as an array under the
`sequence` setting.

Relative action names (starting by a dot) are also accepted.

Example `yaml` configuration:

```yaml
type: sequence
sequence:
    - .bar
    - miau.gloglo
```

### `loop`

The `loop` action is a construct for creating action loops.

It runs a set of subactions in sequence repeatedly according to
specified iterators, enabling one to perform repetitive operations
following several strategies and with varying parameters.

The configuration specifies the subactions to be executed in the loop
and the iterators that control the iterations. These are the accepted
keys:

- `sequence`: Specifies the names of the subactions to be executed in
  the loop. The subactions will be executed in the order specified.

- `iterator`: Specifies the iterators to be used for the loop.

  Each iterator is defined by a key and its corresponding
  configuration, which includes the type of the iterator and any
  required parameters.

  The supported iterator types are:

  - `values`: Iterates over a list of specific values.

  - `configkeys`: Iterates over the keys of a specific path in the
    configuration.

- `ignore_errors` (optional): If set to `true`, any errors that occur
  during an iteration will be logged but will not stop the loop. If
  not specified or set to `false`, an error during iteration will
  raise an exception and halt the loop.

Sample configuration:

```yaml
loop:
  sequence:
    - subaction1
    - subaction2
    - subaction3

  iterator:
    one:
      type: values
      values:
        - value1
        - value2
        - value3
    two:
      type: configkeys
      path: my_config.path

  ignore_errors: true
```

## Runner

The purpose of the runner is to offer a unified entry point for the
project actions and pipelines.

It extracts information from a set of environment variables and also
parses command line arguments in a standard way.

### Command line arguments

The accepted command line arguments are as follow:

- `-d`, `--debug`: Sets the logging level to debug.

- `-c file`, `--config file`: Reads configuration settings from the
  given file.

- `-s key=value`, `--set key=value`: sets the given configuration. For
  instance: `-s fs.output=/var/storage/ai-output`.

- `-S key=value, --set-json key=value`: parses the given value as JSON
  an sets the related configuration entry.

- `-e env`, `--env env`: defined the deployment environment.

- `action1 action2 ...`: set of actions to execute.

### Environment variables

The following environment variables can be used to configure the framework:

* `PLPIPES_ROOT_DIR`: The project root directory.

* `PLPIPES_ENV`: The deployment environment (usually `DEV`, `PRE` or
  `PRO`).

* `PLPIPES_LOGLEVEL`: The default log level (`debug`, `info`,
  `warning` or `error`).


### Under the hood

The runner has two parts. The little `run.py` script that is just a
wrapper for `plpipes.runner.main`. and the later which is the real
thing!

`run.py` is required because `plpipes` uses that program path to
locate the project root directory and the rest of the files.

### Custom scripts

Sometimes you may need to create some custom script out of the actions
structure. In that case, you can write it as a custom runner in the
following manner:

```python
import plpipes.runner

# Get an argument parser preinitilized.
arg_parser = plpipes.runner.arg_parser()

# Add new options to the argument parser if needed
arg_parser.add_argument(...)

# Parse argument and initialize plpipes
opts = plpipes.runner.parse_args_and_init(arg_parser, sys.argv)

# Your code goes here!!!
```

For simple cases, where no extra arguments are going to be needed, the
framework also provides a `simple_init` function:

```python
import plpipes.runner
plpipes.runner.simple_init()

# Your code goes here!!!
```

Note also that PLPipes uses the script name (actually the stem) as a
key when loading configuration files, so it is possible to have
different configuration for those scripts which are loaded
automatically. See configuration [File Structure](#File-structure)
section above.


## Logging

Python standard logging framework is instantiated by the framework and
can be used directly from actions code.

If you need some particular configuration not yet supported, just ask
for it!

Also, take into account that some python frameworks (for instance,
Tensorflow or OpenVINO) unconditionally change or overload python
logging on its own.

### Automatic file logging

After initialization `plpipes` automatically creates a new file logger
which saves a copy of the log in the `logs` directory. Also, on
operating systems supporting symbolic links, it also creates a link
named `logs\last_log.txt`.

## Cloud services

PLPipes provides shortcuts for accessing common cloud services.

### Azure

#### Authentication

Package `plpipes.cloud.azure.auth` provides an easy way to manage
Azure credentials.

##### API

Credential objects of type `azure.identity.MsalCredential` can be
retrieved using function `credentials` as follows:

```python
import plpipes.cloud.azure.auth
cred = plpipes.cloud.azure.auth.credentials("predictland")
```

##### Configuration

Authentication accounts are declared in the configuration files and
instantiated by the module on demand (which for some kind of
authentication methods may require user interaction).

For instance, the following configuration snippet defines the
authorization account `predictland`.

```yaml
cloud:
  azure:
    auth:
      predictland:
        type: interactive_browser
        tenant_id: 01234567-89ab-cdef-0123-456789abcdef
        client_id: 01234567-89ab-cdef-0123-456789abcdef
        client_secret: super-super-super-super-secret
        authentication_callback_port: 8283
        username: elvis@predictland.com
        scopes:
          - "https://graph.microsoft.com/.default"
```

The meaning of every key is as follows:

- `type`: indicates the type of authentication to be used. It
defaults to `InteractiveBrowserCredential`.

- `scopes`: the list of scopes (groups of permissions) to be
requested. This entry is optional, as most Azure services would re-ask
for the credentials with the scopes they need.

Every driver may also accept and/or require additional configuration
entries:

###### `interactive_browser`:

Launches a browser and lets the use authenticate using her
account. Credentials are cached when possible.

- `client_id` and `client_secret`: are the application credentials
which must be registered in Azure Active Directory (AAD). See
[Register
Application](https://learn.microsoft.com/en-us/azure/healthcare-apis/register-application)
at MS Learn website.

- `tenant_id`: the tenant where the application has been registered.

- `username`: expected user, optional. Note that when Azure shows the
login page to the user, it allows her to login with any account
registered in the tenant AD. When this option is used, the framework
ensures that the user logs with the expected one. Otherwise it throws
an error.

- `authentication_callback_port`: The framework starts an HTTP server
at the given port in order to receive the data from the user browser
(afterwards it is stopped). The port must be the same used to register
the application in AAD.


###### `az_cli`:

Uses Azure command line client (`az`) for authentication.

Accepted entries are as follows:

- `private` (defaults to true): whether to use a private `az`
  configuration for this login or the global one for the user.

  In the later case, the global configuration must be initialized by
  the user calling `az login`.

### Google Cloud

*Note: This is a work in progress*

#### Authentication

##### API

Credential objects of type `google.auth.credentials.Credentials` can
be retrieved using function `credentials` as follows:

```python
import plpipes.cloud.cloud.auth
cred = plpipes.cloud.cloud.auth.credentials("predictland")
```

##### Configuration

Authentication accounts are declared in the configuration files and
instantiated by the module on demand (which for some kind of
authentication methods may require user interaction).

For instance, the following configuration snippet defines the
authorization account `predictland`.

```yaml
cloud:
  azure:
    auth:
      predictland:
        type: oauth2
        scopes:
          - "https://www.googleapis.com/auth/cloud-platform"
        ...
```

The meaning of every key is as follows:

- `type`: name of the authentication backend.
- `scopes`: list of scope for which access is being requested.

Every backend requires a different set of additional options:

###### `oauth2`

- `installed`: the additional entries required by
  `google_auth_oauthlib.flow.InstalledAppFlow.from_client_config`
  (`client_id`, `project_id`, `auth_uri`, `token_uri`,
  `auth_provider_x509_cert_url`, `client_secret` and `redirect_uris').

  Those options can be retrieved from the JSON file generated by
  GoogleCloud when a new OAuth2 installed applications is registered
  ([GoogleCloud Console](https://console.cloud.google.com/welcome) →
  APIs & Services → Credentials → Create Credentials → OAuth Client ID →
  Desktop App → Download JSON).

Example:

```yaml
google:
  auth:
    predictland:
      type: oauth2
      installed:
        client_id: "..."
        project_id: "predictland"
        auth_uri: "https://accounts.google.com/o/oauth2/auth"
        token_uri: "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
        client_secret: "..."
        redirect_uris: ["http://localhost"]
      scopes:
        - https://www.googleapis.com/auth/cloud-platform
```

### Microsoft Graph

Interaction with MS Graph API, which provides access to OneDrive,
SharePoint and Teams resources, is available through the package
`plpipes.cloud.azure.graph`.

#### API

- `graph(account_name)`: returns an object of class
msgraph.code.GraphClient. Note that the Python Azure SDK is still in
beta, in a state of flush and so, this method may return objects of a
different class in the future.

`fs(account_name)`: returns an object that allows to access MS Graph
resources as a file system.

##### File-system view

The file system facade class exposes MS Graph resources as a file
system.

Resources are exposed under different routes as follows:

- `me`: Business user OneDrive drive.

- `groups`: Teams group drives.


The file system objects returned by `fs` support the following
methods:

- `go(path)`: You can think of this method as a change dir (`cd`)
  operation with the particularity that it also allows one to descend
  into file-like resources.

  The returned value is a new file system object with the root at
  `path`.

- `ls(path)`: Return a dictionary of file-name and entry pairs
  representing the entries under the directory `path`.

- `names(path)`: Similar to `ls` but returns only the names of the
  entries.

- `is_file()` and `is_dir()`: Determines where the current file system
  object is pointing to a file or a directory respectively.

- `get(path="", dest=None, dir=None, name=None)`: downloads the remote
  object pointed by the current file system object.

  When `dest` is given it is used as the local destination path.

  Alternatively, when `dest` is not given, `dir` and `name` values (or
  their defaults) are used to construct the local destination
  path. `name` defaults to the remote file name. `dir` defaults to the
  working directory (i.e. `cfg['fs.work']`).

- `rget(path="", dest=None, dir=None, name=None)`: recursively downloads
  the remote object (typically a directory) to the current file
  system.

Example usage:

```python
import plpipes.cloud.azure.graph

fs = plpipes.cloud.azure.graph.fs("predictland")
group_drive = fs.go("groups/HAL/General")
group_drive.rget("input-data")
```

#### Configuration

Currently, the only supported configuration parameter is `credentials`
with must be the name of an Azure authentication account defined under
`cloud.azure.auth`. When not given, it defaults to the one of the same
name.

```yaml
cloud:
  azure:
    graph:
      predictland:
        credentials: predictland
```

### OpenAI (ChatGPT)

PLPipes provides a very thin wrapper for the `openai` package.

Currently, it just automates the authentication side, reading the API
key from the configuration and setting it on the client package.

```python
import plpipes.cloud.openai as openai
completion = openai.Completion.create(...)
```

If used outside actions, if should be taken into account that
PLPipes config subsystem must be initialized before importing
`plpipes.cloud.openai`.

#### Configuration

```yaml
cloud:
  openai:
    auth:
      api_key: YOUR-SECRET-KEY-GOES-HERE
```

## Network

### Clients

PLPipes provides a set of easy-to-use network clients for downloading
data from several institutions.

This is still a work in progress.

#### Eurostat

#### U.S. Energy Information Administration (EIA)

API key must be dowloaded from
https://www.eia.gov/opendata/register.php and added into the
configuration as `net.client.eia.api_key`.

#### European Central Bank (ECB)

#### U.S. Bureau of Labor Statistics (BLS)

API key must be downloaded from
https://data.bls.gov/registrationEngine/ and added into the
configuration as `net.client.us_bls.api_key`.


# Jupyter integration

PLPipes includes an IPython extension which exposes the framework
functionality in Jupyter notebooks.

## Initialization

The extension is loaded adding the following lines at the beginning of
your notebook:

```python
%load_ext plpipes.jupyter
%plpipes {stem}
```

Where `{stem}` is the name used as the main key when looking for
configuration files (defaults to `jupyter`).

In order to find the project configuration, the extension looks into
the environment variable `PLPIPES_ROOT_DIR`. If that variable is not
defined then it looks for a `config` directory in the current working
directory of the IPython kernel (usually the directory from where
`jupyter-lab` was launched) and walks up the file system until such
directory is found.

Once the extension is loaded and initialized, the features described
in the following sections can be used.

## Variable, packages and method shortcuts

The following variables and methods are made available in the session:

* `cfg`: The configuration object

* `input_dir`, `work_dir` and `output_dir`: `libpath` objects pointing
  to the input, work and output directories.

  For instance:
  ```python
  df = pandas.read_csv(input_dir / "data001.csv")
  ```

* `db`: a shortcut for `plpipes.database`

* `create_table` and `query`: shortcuts for the functions of the same
  name in `plpipes.database`.


## SQL integration

The IPython SQL extension (see https://pypi.org/project/ipython-sql/)
is automatically loaded and the configured PLPipes `work` database set
as the default one.

Other databases configured in PLPipes can be selected using a double
at sign (`@@`) followed by the database name.  For instance:

```sql
%%sql @@input
select * from customers
limit 100
```


# Packing `plpipes`

Currently, `plpipes` is packed with
[flit](https://flit.pypa.io/en/stable/) (which can be installed with
the usual `pip` command: `pip install flit`).

A python wheel file for `plpipes` is generated running the following
command from inside `plpipes` root directory:

```bash
flit build
```

The generated wheel file is placed inside `dist`. That file is a
standard (pure) Python package that can be installed in anywhere. For
instance:

```bash
pip install ../PL-TEC-PLPipes/dist/plpipes-0.1-py2.py3-none-any.whl
```


# FAQ

## Design

* *Why is the database used to pass data between actions? Isn't that
  inefficient?*

  Usually it is not.

  Both SQLite and DuckDB are pretty fast reading and writing data so
  that the database trip is very rarely the bottleneck.

  Actually, if you are able to delegate the data transformation tasks
  to the database (writing SQL code or using some front-end as ibis),
  they would perform way faster than the equivalent pandas code.

  Coming back to the *why*. Using a database has several additional
  benefits:

  - It is quite easy to inspect intermediate data, just point your
    favorite SQL GUI (for instance, [DBeaver](https://dbeaver.io/))
    to the database and look at the tables you want to see.

  - It allows the programmer to easily add pre and post-condition
    checking scripts which unintrusively validate the data before and
    after every action is run (planed).

  - It allows one to switch between functional-equivalent actions
    easily. For instance, in order to add support for some new
    algorithm into a project, all that is required is to develop the
    new model-training action and to plug it into some pipeline.

  - It becomes easier for new people to get to work into the project,
    as they only need to understand the data in the tables where they
    are going to work.

  - It is easy to establish guidelines about documenting the
    intermediate information structure (something that never happens
    for in-process pipelines).

* *How should I break my program into actions?*

  Well, the truth is we are still learning about what are the best ways
  to structure data science projects around actions!

  Typically, there are three clear parts in a Data Science project:

  1. Data preprocessing
  2. Model training and validation
  3. Predicting

  Though, sometimes, it doesn't make sense to split the training and
  the prediction stages. For instance, when the model needs to be
  retrained every time as it happens with time series data.

  Then every one of the actions above may be broken in several
  sub-actions. For instance, as part of the preprocessing we would have
  a data-retrieving action (maybe composed of several sub-actions as
  well). And then two more actions for converting from bronze-quality
  data first to silver and then to gold (see the [Medallion
  architecture](https://www.databricks.com/glossary/medallion-architecture)).

  Then, inside the model training, we could have still some data
  manipulation actions in order to adapt the generic gold format to
  the format required by the specific model, then an action that
  trains and saves the model to disk and finally some action that
  calculates some KPIs.

  Otherwise, maybe for that particular algorithm it is easier to do
  the data preparation, training and evaluation in just one
  action.

  Note also, that `actions` are not the only available abstraction to
  be used with PLPipes. Code can be organized as regular Python
  modules inside the `lib` directory and called from multiple actions.

  In summary, Common sense should be applied. Actions should not be a
  straitjacket, but just another element in your tool-set!
