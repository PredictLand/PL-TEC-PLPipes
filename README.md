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

## Environment variables

The following environment variables can be used to configure the framework:

* `PLPIPES_ROOT_DIR`: The project root directory.

* `PLPIPES_ENV`: The environment (usually `DEV`, `PRE` or `PRO`).

* `PLPIPES_LOGLEVEL`: The default log level (`debug`, `info`,
  `warning` or `error`).


# Modules

## Configuration

## Database

## Actions

## Runner




# Jupyter integration

PLPipes includes an IPython extension which exposed the framework
functionality in Jupyter notebooks.

## Initialization

It can be loaded adding the following lines at the beginning of your
notebook:

```
%load plpipes.jupyter
%plpipes $stem
```

Where `$stem` is the name used as the main key when looking for
configuration files. Defaults to `jupyter`.

In order to find the project configuration, the extension looks into
the environment variable `PLPIPES_ROOT_DIR`. If it is not defined it
looks for a `config` directory in the current working directory of the
IPython kernel (usually the directory from where `jupyter-lab` was
launched) and walks up the file system until such directory is found.

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
