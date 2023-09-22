# Runner

The purpose of the runner is to provide a unified entry point for project actions and pipelines.

It extracts information from a set of environment variables and also parses command line arguments in a standardized manner.

## Command line arguments

The accepted command line arguments are as follows:

- `-d`, `--debug`: Sets the logging level to debug.

- `-c file`, `--config file`: Reads configuration settings from the specified file.

- `-s key=value`, `--set key=value`: Sets the given configuration. For example: `-s fs.output=/var/storage/ai-output`.

- `-S key=value`, `--set-json key=value`: Parses the provided value as JSON and sets the corresponding configuration entry.

- `-e env`, `--env env`: Defines the deployment environment.

- `action1 action2 ...`: A list of actions to execute.

## Environment variables

The following environment variables can be used to configure the framework:

* `PLPIPES_ROOT_DIR`: The project root directory.

* `PLPIPES_ENV`: The deployment environment (typically `DEV`, `PRE`, or `PRO`).

* `PLPIPES_LOGLEVEL`: The default log level (`debug`, `info`, `warning`, or `error`).

## Under the hood

The runner consists of two parts: a small `run.py` script that serves as a thin wrapper for the 
[`main`](reference/plpipes/runner.md#plpipes.runner.main) function provided by [`plpipes.runner`](reference/plpipes/runner.md).

`run.py` is necessary as `plpipes` uses the script's path to locate the project root directory and other related files.

## Custom scripts

In some cases, you may need to create a custom script outside the actions structure. To do this, you can write a custom runner as follows:

```python
import plpipes.runner

# Get a pre-initialized argument parser
arg_parser = plpipes.runner.arg_parser()

# Add new options to the argument parser if needed
arg_parser.add_argument(...)

# Parse arguments and initialize plpipes
opts = plpipes.runner.parse_args_and_init(arg_parser, sys.argv)

# Your code goes here!!!
```

For simpler cases where no additional arguments are required, the framework also provides a [`simple_init`](reference/plpipes/runner.md#plpipes.runner.simple_init) function:

```python
import plpipes.runner
plpipes.runner.simple_init()

# Your code goes here!!!
```

It's worth noting that PLPipes uses the script name (or its stem) as a key when loading configuration files, enabling the use of different configurations for scripts that are loaded automatically. Refer to the configuration [File Structure](#File-structure) section above.

