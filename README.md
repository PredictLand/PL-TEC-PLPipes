---
title: PLPipes - PredictLand Data Science Framework
---

# Introduction

PredicLand is a consultancy firm focused on Data Science and related
fields, Data Analytics, AI and ML, Big Data, Data Engineering, etc. We
work for custommers that range from small companies with just a few
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
on the cheap, a poor-man Databricks replacemnent!

But even if that is true, we prefer to think of PLPipes as a lean and
very scalable framework. Something that you can use to train some
models from a few CSVs in your laptop or to process terabytes of
information on a cloud cluster or embedded in a lambda function, or a
Docker to run some model.

So, what is exactly PLPipes?

Several things:

1. It is a thin layer integrating several technologies so that they
   can be used easily and efficiently to solve common data science
   problems.

2. It is an automation framework for creating data processing
   pipelines.

3. It is a programming framework for reducing boilerplate, enforcing
   some best-practices and providing support for common tasks.

4. It is also a mindset and a way to standardize Data Science
   project development.

5. It is a very customizable framework with sane defaults, so that you
   can start working on your projects right there without having to
   perform a complex setup up front.

6. It is a work in process yet! Even if the ideas behind PLPipes are
   not new and we have used/implemented them in different forms and in
   different projects in the past (or in some cases, just copied them
   from other 3rd party projects), the framework is still very new and
   most of it should be considered experimental!

# Nomenclature

We use `PLPipes` to refer to the framework as a whole (projects, code,
conventions, mindset, etc.) and `plpipes` to refer specifically to the
Python package.

# Project Setup

This is how to setup a PLPipes project from scratch.

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
  preferred way to organise the project operations.

* `actions`: Action definitions. See [Actions](#Actions) below.

* `notebooks` (optional): Jupyter notebooks go here

* `config`: Configuration files are stored here. See
  [Configuration](#Configuration).

* `defaults` (optional): Default configuration files go here (the
  contents of this directory should be commited to git).

  The semantic distinction between `defaults` and `config` is
  something we are still considering and that may change.

* `input` (optional): Project input files.

* `work`: Working directory, intermediate files go here.

  Also, the default working database is stored here as
  `work/work.duckdb`.

* `output` (optional): Final output files generated can go here.

* `venv` (optional): Even if `plpipes` does not depend on it, we
  recomend to use a virtual environment for the project whit that
  name.

## The main script

 `bin/run.py` is the main entry point for PLPipes and should be
created by hand with the following content:

```
#!/usr/bin/env python3
from plpipes.runner import main
main()
```

## Installing `plpipes`

The Python module `plpipes` can be installed in two ways.

### Installing a packed version

This is the way to install the module we recomend when you don't want
to contribute to the development of the framework and just want to use
it.

*Note that In practice, as `plpipes` is still in a very early
development stage, that may not be a realistic asumption and you may
be required to switch to the development version available from git
quite soon!*

Somehow (!) obtain the module wheel and install it using pip:

```
pip install /path/to/.../plpipes-0.1-py2.py3-none-any.whl
```

Hopefully, `plpipes` would be directly available from
[PyPI](https://pypi.org/) soon!

### Installing from git

1. Clone the repository outside of your project directory and switch
   to the `development` branch:

   ```
   git clone git@github.com:PredictLand/PL-TEC-PLPipes.git
   cd PL-TEC-PLPipes
   git checkout development
   ```

2. Add the `src` subdirectory to Python search path.

   ```
   # Linux and/or bash:
   export PYTHONPATH=path/to/.../PL-TEC-PLPipes/src

   # Windows
   set PYTHONPATH=C:\path\to\...\PL-TEC-PLPipes\src
   ```

3. Check that it works:

   ```
   python -m plpipes -c "print('ok')"
   ```

Alternatively you can modify your project main script to append
the`src` directory to the module search path so that you don't need to
set `PYTHONPATH` by hand everytime you start a new session.

For instance:

```
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

Both YAML and JSON files are supported (though, we recomended YAML
usage as it is usually easier to read).

### File structure

The list of files from with the configuration is read is dynamically
calculated based on two settings:

* The script "stem": It is the name of the script run without the
  extension (i.e. the stem for `run.py` is `run`).

  When plpipes is used from a Jupyter notebook, the stem can be passed
  on the %plpipes line magic:

  ```
  %plpipes foobalizer
  ```

- The deployment environment (`dev`, `pre`, `pro`, etc.): this can be
  set from the command line or using the environment variable
  `PLPIPES_ENV` (see [Environment variables](#Environmen-variables)
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

2. Secrets: files with a `-secrets` postfix are also loaded (for
   instance, `common-secrets.yaml` and `run-secrets.yaml`).

2. Environment: files with the deployment environment attached as a
   postfix are also loaded (`run-dev.yaml` or `run-secrets-dev.yaml`).




### Python usage

### Initialization

## Database

## Actions

## Runner

### Environment variables

The following environment variables can be used to configure the framework:

* `PLPIPES_ROOT_DIR`: The project root directory.

* `PLPIPES_ENV`: The deployment environment (usually `DEV`, `PRE` or
  `PRO`).

* `PLPIPES_LOGLEVEL`: The default log level (`debug`, `info`,
  `warning` or `error`).

## Logging



# Jupyter integration

PLPipes includes an IPython extension which exposes the framework
functionality in Jupyter notebooks.

## Initialization

The extension is loaded adding the following lines at the beginning of
your notebook:

```
%load plpipes.jupyter
%plpipes $stem
```

Where `$stem` is the name used as the main key when looking for
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
  ```
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

```
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

```
flit build
```

The generated wheel file is placed inside `dist`. That file is a
standard (pure) Python package that can be installed in anywhere. For
instance:

```
pip install ../PL-TEC-PLPipes/dist/plpipes-0.1-py2.py3-none-any.whl
```
