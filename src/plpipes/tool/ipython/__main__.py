import sys
from plpipes.tool.dbeaver import run_dbeaver
from plpipes.runner import simple_init

import plpipes.config
import plpipes.database
import plpipes.filesystem

from IPython import embed

def run(argv):
    simple_init(['ipython'] + argv[1:])

    using={'cfg': plpipes.config.cfg,
           'db': plpipes.database,
           'fs': plpipes.filesystem}

    embed()

run(sys.argv)
