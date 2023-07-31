import sys
import os
from pathlib import Path

sys.path.append(str(Path(os.getcwd()).joinpath("src")))

import plpipes.config


import unittest