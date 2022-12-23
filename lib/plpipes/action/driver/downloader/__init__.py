import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

class _Downloader(Action):
    def do_it(self):
        service_name = self._cfg["service"]
        path = self._cfg.get("path", "")
        params = self._cfg.get("params", {})
        db = self._cfg.get("db", "work")

        impl = __import__(f"action.downloader.service.{service_name.lower()}")
        service = impl.download(path=path, keys=keys, db=db, acfg=self._acfg)
