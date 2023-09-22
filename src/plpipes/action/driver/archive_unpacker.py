"""This module contains the _ArchiveUnpacker class, which is responsible for handling actions with type `archive_unpacker`
 within the plpipes framework.

The `archive_unpacker` action reads the configuration to identify the
archive file and its target extraction location.  It utilizes the
patoolib library to perform the extraction of the specified archive.
"""

import logging
from pathlib import Path

from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg

class _ArchiveUnpacker(Action):
    """
    Action for unpacking archive files.

    This class extracts the contents of an archive file specified in the configuration
    and places the resulting files in a target directory.
    """

    def do_it(self):
        """
        Executes the unpacking of the specified archive.

        Retrieves the archive file and target directory from the configuration,
        and uses patoolib to extract the contents of the archive.
        If specific subtrees are defined in the configuration, a warning is logged
        as limiting the unpack to specific subtrees is not supported.
        """
        work = Path(cfg["fs.work"])
        archive = work / self._cfg["archive"]

        target = self._cfg.get("target")
        if target is None:
            target = archive.stem
        target = work / target
        options=[]
        subtrees = self._cfg.get("subtrees")
        if subtrees is not None:
            # options += [-r, *subtrees]
            logging.warn("Limiting the unpack to specific subtrees is not yet supported!")

        import patoolib
        patoolib.extract_archive(str(archive), outdir=str(target))

register_class("archive_unpacker", _ArchiveUnpacker)
