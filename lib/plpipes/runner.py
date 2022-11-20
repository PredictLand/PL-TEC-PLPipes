import plpipes
import sys
import argparse
import os
import pathlib
import json

def _cfg_set(root, dotted_key, value):
    path = dotted_key.split(".")
    try:
        for p in path[:-1]:
            root = root.setdefault(p, {})
        p[path[-1]] = value
    except:
        raise KeyError(dotted_key)

def main(args=None):
    if args is None:
        args = sys.argv

    if len(args) < 1:
        raise Exception("Unable to infer config stem. Program name missing from argument list")

    stem = pathlib.Path(args[0]).stem

    parser = argparse.ArgumentParser(prog='runner',
                                     description="PLPipes runner")

    parser.add_argument('-d', '--debug',
                        help="Turns on debugging",
                        action='store_true')
    parser.add_argument('-c', '--config',
                        action="append",
                        metavar="CFG_FN",
                        help="Additional configuration file",
                        default=[])
    parser.add_argument('-s', '--set',
                        action="append",
                        metavar="CFG_KEY=VAL",
                        help="Set configuration entry",
                        default=[])
    parser.add_argument('-S', '--set-json',
                        action="append",
                        metavar="CFG_KEY=JSON_VAL",
                        help="Set configuration entry (value is parsed as JSON)",
                        default=[])
    parser.add_argument('-e', '--env',
                        metavar="ENVIRONMENT",
                        help="Select environment (DEV, PRE, PRO, etc.)")

    parser.add_argument('action', nargs="*",
                        metavar="ACTION", default=["default"])

    opts = parser.parse_args(args)

    config_extra = {}
    for s in opts.set:
        k, v = s.split("=", 1)
        _cfg_set(config_extra, k, v)
    for s in opts.set_json:
        k, v = s.split("=", 1)
        _cfg_set(config_extra, k, json.loads(v))

    plpipes.init(stem,
                 config_files=opts.config,
                 config_extra=config_extra,
                 env=opts.env)

