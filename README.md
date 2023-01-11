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

1. It is a thin layer integrating several technologies so they can be
   used easily and efficiently to solve common data science problems.

2. It is an automation framework for creating data processing
   pipelines.

3. It is a programming framework for reducing boilerplate, enforcing
   some best-practices and providing support for common tasks.

4. It is also a way to standardize Data Science projects and a
   mindset.

5. It is a very customizable framework with sane defaults, so that you
   can start working on your projects right there without having to
   perform a complex setup up front.

6. It is a work in process yet! Even if the ideas behind PLPipes are
   not new and we have used/implemented them in different forms and in
   different projects in the past (or in some cases, just copied them
   from other 3rd party projects), the framework is very new and most
   of it should still be considered experimental!


# Project Setup

This is how to setup a PLPipes project from scratch.

PLPipes is quite configurable and most of its workings can be changed
and redefined, but that doesn't preclude it from offering some sane
defaults that we advise you to follow.

Specifically, by default it expects some directory structure.

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

* `actions`: Action definitions. See [Actions](#Actions) bellow.

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


## The main script

 `bin/run.py` is the main entry point for PLPipes and should be
created by hand with the following content:

```
#!/usr/bin/env python3
from plpipes.runner import main
main()
```

## Installing PLPipes

### Installing a packed version

### Installing from git




## Environment variables

The following environment variables can be used to configure the framework:

* `PLPIPES_ROOT_DIR`: The project root directory.

* `PLPIPES_ENV`: The environment (usually `DEV`, `PRE` or `PRO`).

* `PLPIPES_LOGLEVEL`: The default log level (`debug`, `info`,
  `warning` or `error`).


# Features

## Configuration

## Database

## Actions

## Runner




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
is automatically loaded and the configured PLPipes `work` database
configured as the default one.

Other databases configured in PLPipes can be selected using a double
at sign (`@@`) followed by the database name.  For instance:

```
%%sql @@input
select * from customers
limit 100
```


# Packing PLPipes

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
