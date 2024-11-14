# Module for autoconfiguring DBeaver

import sys
from plpipes.tool.dbeaver import run

if __name__ == "__main__":
    run(["plpipes-tool-dbeaver", *sys.argv[1:]])

