import logging
import pathlib
import importlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

class _Downloader(Action):
    def do_it(self):
        print(f"Action config: {self._cfg.to_tree()}")
        service_name = self._cfg["service"]
        path = self._cfg.get("path", "")
        params = self._cfg.to_tree("params")
        print(f"params: {params}")
        db = self._cfg.get("db", "work")

        impl = importlib.import_module(f"plpipes.action.driver.downloader.service.{service_name.lower()}")
        service = impl.download(path=path, params=params, db=db, acfg=self._cfg)

register_class("downloader", _Downloader)
