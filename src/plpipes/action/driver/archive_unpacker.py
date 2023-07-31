import logging
from pathlib import Path

from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg

class _ArchiveUnpacker(Action):
    def do_it(self):
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
