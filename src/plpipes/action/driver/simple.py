import logging

from plpipes.config import cfg
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.action.runner import lookup

import plpipes.database
import plpipes

def _action_namespace_setup(action_cfg=None):
    return {"cfg": cfg, "action_cfg": action_cfg, "db": plpipes.database, "plpipes": plpipes}

class _PythonRunner(Action):
    def _do_it(self, indent):
        if not hasattr(self, "_code"):
            self._path = self._cfg["files.py"]
            try:
                with open(self._path, "r", encoding="utf8") as f:
                    py_code = f.read()
                self._code = compile(py_code, self._path, 'exec')
            except Exception as ex:
                logging.error(f"Action of type python_script failed while compiling {self._path}")
                raise ex
        try:
            logging.debug(f"Running python code at {self._path}")
            exec(self._code, _action_namespace_setup(action_cfg=self._cfg))
            del self._code
        except Exception as ex:
            logging.error(f"Action of type python_script failed while executing {self._path}")
            raise ex

class _Sequencer(Action):
    def do_it(self):
        name = self._name

        for child_name in self._cfg["sequence"]:
            lookup(child_name, parent=name).run()

register_class("python_script", _PythonRunner, "py")
register_class("sequence", _Sequencer, "dir")
