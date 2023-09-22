"""
Module: plpipes.runner

The purpose of the runner is to offer a unified entry point for the
project actions and pipelines. It extracts information from a set of
environment variables and parses command line arguments in a standard way.

For more information, refer to [Runner](../../runner.md).
"""

import plpipes.init
import plpipes.action
import plpipes.config
import sys
import argparse
import os
import pathlib
import json
import logging

class _PairAction(argparse.Action):
    """
    Custom argparse action for handling key-value pairs input.

    This action allows key-value pairs to be passed in as arguments,
    with optional unpacking for JSON values.
    """
    def __init__(self, option_strings, dest, nargs=None, unpack=None, default=None, **kwargs):
        if nargs is not None:
            raise ValueError("nargs not allowed")
        if default is None:
            default = {}
        super().__init__(option_strings, dest, nargs=1, default=default, **kwargs)
        self.unpack = unpack

    def __call__(self, parser, namespace, values, option_string=None):
        """
        Processes the input key-value pair(s) and assigns them to the namespace.

        Parameters:
            parser: The argument parser.
            namespace: The namespace where the values should be stored.
            values: The input values to process.
            option_string: The option string used for this action.
        """
        if self.unpack:
            if self.unpack == "json":
                def unpacker(x):
                    return json.loads(x)
            else:
                raise Exception(f"Bad unpacker {self.unpack}")
        else:
            unpacker = None
        for pair in values:
            try:
                (k, v) = pair.split("=", 2)
            except:
                raise argparse.ArgumentError(self, f"Can not parse config pair '{pair}'")
            if unpacker:
                try:
                    v = unpacker(v)
                except:
                    raise argparse.ArgumentError(self, f"Can not unpack value part of '{pair}' as {self.unpack}.")
            try:
                getattr(namespace, self.dest).append({k: v})
            except Exception as ex:
                raise argparse.ArgumentError(self, f"Conflicting config pair '{pair}': {ex}") from ex


_default_arg_parser_args = { 'description': 'PLPipes runner' }

def arg_parser(**kwargs):
    """
    Initializes the argument parser for the runner.

    Parameters:
        kwargs: Additional keyword arguments for customizing the parser.

    Returns:
        An instance of argparse.ArgumentParser configured for the runner.
    """
    parser = argparse.ArgumentParser(**{**_default_arg_parser_args, **kwargs})
    parser.add_argument('-d', '--debug',
                        help="Turns on debugging",
                        action='store_true')
    parser.add_argument('-c', '--config',
                        action="append",
                        metavar="CFG_FN",
                        help="Additional configuration file",
                        default=[])
    parser.add_argument('-s', '--set',
                        action=_PairAction,
                        metavar="CFG_KEY=VAL",
                        help="Set configuration entry",
                        default=[])
    parser.add_argument('-S', '--set-json',
                        action=_PairAction,
                        metavar="CFG_KEY=JSON_VAL",
                        unpack="json",
                        dest="set",
                        help="Set configuration entry (value is parsed as JSON)")
    parser.add_argument('-e', '--env',
                        metavar="ENVIRONMENT",
                        help="Select environment (dev, pre, pro, etc.)")
    return parser

def parse_args_and_init(arg_parser, args=None):
    """
    Parses command line arguments and initializes the PLPipes framework.

    Parameters:
        arg_parser: The argument parser instance to use for parsing.
        args: The command line arguments to parse (defaults to sys.argv).

    Returns:
        The parsed options as an object with attributes corresponding to the arguments.

    Raises:
        Exception: If the program name is missing from the argument list.
    """
    if args is None:
        args = sys.argv

    if len(args) < 1:
        raise Exception("Unable to infer config stem. Program name missing from argument list")

    prog_path = pathlib.Path(args[0])
    if "PLPIPES_ROOT_DIR" in os.environ:
        root_dir = pathlib.Path(os.environ["PLPIPES_ROOT_DIR"]).resolve(strict=True)
    else:
        root_dir = prog_path.parent.parent
    root_dir = root_dir.absolute()

    opts = arg_parser.parse_args(args[1:])

    config_extra = [{'fs': {'stem': str(prog_path.stem),
                            'root': str(root_dir),
                            'project': str(root_dir.stem)}}]

    config_extra += opts.set

    if opts.env is not None:
        config_extra["env"] = opts.env
    if opts.debug:
        config_extra.append({"logging.level": "debug"})
        config_extra.append({"logging.level_file": "debug"})

    plpipes.init.init(*config_extra, config_files=opts.config)

    sys.path.append(plpipes.config.cfg["fs.lib"])

    os.environ.setdefault("PLPIPES_ROOT_DIR", plpipes.config.cfg["fs.root"])

    return opts

def simple_init(args=None):
    """
    A simplified initialization function that uses a default argument parser.

    Parameters:
        args: The command line arguments to parse (defaults to None).

    Returns:
        The parsed options as an object with attributes corresponding to the arguments.
    """
    return parse_args_and_init(arg_parser(), args)

def main(args=None):
    """
    Main entry point for the runner. Parses arguments and executes specified actions.

    Parameters:
        args: The command line arguments to parse (defaults to None).
    """
    parser = arg_parser()
    parser.add_argument('actions', nargs="*",
                        metavar="ACTION", default=["default"])
    opts = parse_args_and_init(parser, args)

    for action in opts.actions:
        logging.info(f"Executing action {action}")
        plpipes.action.run(action)
