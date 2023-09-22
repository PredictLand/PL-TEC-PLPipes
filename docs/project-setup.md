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
should be **created by hand** (development of a utility to do it
automatically is planed).

* `lib` (optional): This is where reusable Python modules specific to
    the project are stored.

* `bin`: This is the place where to place scripts for the
    project. Though, usually if just contains [the main
    script](#the-main-script) `run.py`.

    Other scripts can be placed here, but it should be noted that the
    [Actions](#actions.md) mechanism available through `run.py` is the
    preferred way to organize the project operations.

* `actions`: Action definitions. See [Actions](actions.md).

* `notebooks` (optional): Jupyter notebooks go here.

* `config`: Configuration files are stored here. See
    [Configuration](configuration.md).

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
