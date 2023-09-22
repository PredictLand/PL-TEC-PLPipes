"""
This module handles the registration and execution of various action drivers
for the plpipes framework.

Action drivers are responsible for executing specific types of actions defined
within the project. The supported action drivers include simple actions, SQL
actions, downloading actions, quarto processing, file downloading, archive
unpacking, and loop actions.

"""

from .driver import simple
from .driver import sql
from .driver import downloader
from .driver import quarto
from .driver import file_downloader
from .driver import archive_unpacker
from .driver import loop

# import the runner
from .runner import run
