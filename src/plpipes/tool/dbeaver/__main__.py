# Module for autoconfiguring DBeaver

import sys
from plpipes.tool.dbeaver import run_dbeaver
from plpipes.runner import arg_parser, parse_args_and_init

def run(argv):
    ap = arg_parser(prog="plpipes.tool.dbeaver",
                    description='PLPipes DBeaver autoconfiguration tool',
                    epilog="""This tool is used to automate
                              the configuration of DBeaver with database
                              connections defined in your project.  The
                              database connection details are read from
                              the project-specific configuration files
                              managed by the plpipes framework.""")

    ap.add_argument('-p', '--permanent', '--save', action='store_true',
                    help='Make the connections persistent in DBeaver')
    ap.add_argument('-o', '--connect', '--open', action='store_true',
                    help='Connect immediately to the databases after setting them up')
    ap.add_argument('-i', '--instance', type=str, action='append',
                    help='Initialize only the connection(s) with the given name. Can be used multiple times.')

    opts = parse_args_and_init(ap, argv)

    # Launch DBeaver with the specified options
    run_dbeaver(permanent=opts.permanent, connect=opts.connect, instances=opts.instance)

if __name__ == "__main__":
    run(["plpipes-tool-dbeaver", *sys.argv[1:]])
