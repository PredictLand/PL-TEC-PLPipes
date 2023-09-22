import logging
import time
import re

class Action:
    """
    Represents an action to be performed within the plpipes framework.

    Attributes:
        _name (str): The name of the action.
        _cfg (dict): Configuration parameters for the action.
    """

    def __init__(self, name, action_cfg):
        """
        Initializes the Action instance.

        Args:
            name (str): The name of the action.
            action_cfg (dict): Configuration parameters for the action.
        """
        self._name = name
        self._cfg = action_cfg

    def name(self):
        """
        Returns the name of the action.

        Returns:
            str: The name of the action.
        """
        return self._name

    def short_name(self):
        """
        Returns the short name of the action, derived from the full name.

        Returns:
            str: The short name of the action.
        """
        return re.split(r'[./\\]', self._name)[-1]

    def _do_it(self, indent):
        """
        Executes the action.

        Args:
            indent (int): The indentation level for logging.
        """
        self.do_it()

    def do_it(self):
        """
        The main logic of the action must be implemented in subclasses.
        This should contain the code that performs the action.
        """
        ...

    def run(self, indent=0):
        """
        Executes the action and logs its execution time.

        Args:
            indent (int): The indentation level for logging.
        """
        name = self.name()
        logging.info(f"{' '*indent}Action {name} started")
        start = time.time()
        self._do_it(indent=indent)
        lapse = int(10 * (time.time() - start) + 0.5) / 10.0
        logging.info(f"{' '*indent}Action {name} done ({lapse}s)")

    def __str__(self):
        """
        Returns a string representation of the Action instance.

        Returns:
            str: A string representation of the Action.
        """
        return f"<Action {self._name}>"
