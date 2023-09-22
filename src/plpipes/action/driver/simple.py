"""
This module implements the basic drivers for running Python code and sequences of actions
in the plpipes framework.

This module contains action drivers for executing Python scripts and for sequencing
multiple actions. The action drivers are responsible for managing the execution of
specific actions defined in the project.

"""

import logging

from plpipes.config import cfg
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.action.runner import lookup

import plpipes.database
import plpipes

def _action_namespace_setup(action_cfg=None):
    """
    Set up the action namespace for executing Python code.

    Args:
        action_cfg: Optional configuration specific to the action.

    Returns:
        A dictionary containing the configuration, action configuration, database, and plpipes module.
    """
    return {"cfg": cfg, "action_cfg": action_cfg, "db": plpipes.database, "plpipes": plpipes}

class _PythonRunner(Action):
    """
    Class to run Python scripts as actions in the plpipes framework.

    Inherits from the Action base class and overrides the method to execute the script
    code defined in the action configuration.
    """

    def _do_it(self, indent):
        """
        Execute the Python script defined in the action configuration.

        Args:
            indent: An indentation level for logging purposes.

        Raises:
            Exception: Raises an exception if there is an error during compilation or execution of the script.
        """
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
    """
    Class to execute a sequence of actions defined in the action configuration.

    Inherits from the Action base class and provides functionality to run
    multiple child actions in the order specified in the configuration.
    """

    def do_it(self):
        """
        Execute the sequence of actions as defined in the action configuration.

        This method retrieves the names of the child actions and runs them in order.

        Raises:
            Exception: Raises an exception if any of the child actions fail to execute.
        """
        name = self._name

        for child_name in self._cfg["sequence"]:
            lookup(child_name, parent=name).run()

register_class("python_script", _PythonRunner, "py")
register_class("sequence", _Sequencer, "dir")
