import logging

from plpipes.config import cfg
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.action.runner import lookup
from plpipes.init import init_run_as_of_date
import plpipes

class _Iterator:
    """
    Base class for iterators used in the plpipes framework.

    Attributes:
        _key (str): The key associated with the iterator.
        _cfg (dict): Configuration dictionary for the iterator.
    """

    def __init__(self, key, icfg):
        """
        Initializes the iterator with a key and configuration.

        Args:
            key (str): The key for the iterator.
            icfg (dict): Configuration settings for the iterator.
        """
        self._key = key
        self._cfg = icfg
        self.reset()

    def reset(self):
        """Resets the iterator to its initial state."""
        pass

    def next(self):
        """
        Advances the iterator to the next value.

        Returns:
            bool: True if there is a next value; False otherwise.
        """
        return False

    def where(self):
        """
        Provides a string representation of the current state of the iterator.

        Returns:
            str: A string indicating the current key and value.
        """
        return self._key

class _ListIterator(_Iterator):
    """
    Iterator for a list of values.

    Attributes:
        _values (list): The list of values to iterate over.
        _target (str): The target configuration key to set on each iteration.
    """

    def __init__(self, key, icfg, values):
        """
        Initializes the list iterator.

        Args:
            key (str): The key for the iterator.
            icfg (dict): Configuration settings for the iterator.
            values (list): The list of values to iterate over.
        """
        self._values = values
        self._target = icfg["target"]
        super().__init__(key, icfg)

    def reset(self):
        """Resets the iterator to the start of the values."""
        self._ix = -1
        cfg[self._target] = None

    def next(self):
        """
        Advances the iterator to the next value.

        Returns:
            bool: True if there is a next value; False otherwise.
        """
        self._ix += 1
        try:
            v = self._values[self._ix]
        except IndexError:
            return False
        logging.debug(f"Setting {self._target} to {v}")
        cfg[self._target] = v
        return True

    def where(self):
        """
        Provides a string representation of the current state of the iterator.

        Returns:
            str: A string indicating the current key and value from _values.
        """
        return f"{self._key}={self._values[self._ix]}"

class _ValuesIterator(_ListIterator):
    """
    Iterator for a list of values obtained from the configuration.

    Args:
        key (str): The key for the iterator.
        icfg (dict): Configuration settings for the iterator.
    """

    def __init__(self, key, icfg):
        """
        Initializes the values iterator.

        Args:
            key (str): The key for the iterator.
            icfg (dict): Configuration settings for the iterator.
        """
        super().__init__(key, icfg, list(icfg["values"]))

class _ConfigKeysIterator(_ListIterator):
    """
    Iterator for configuration keys.

    Args:
        key (str): The key for the iterator.
        icfg (dict): Configuration settings for the iterator.
    """

    def __init__(self, key, icfg):
        """
        Initializes the config keys iterator.

        Args:
            key (str): The key for the iterator.
            icfg (dict): Configuration settings for the iterator.
        """
        values = list(cfg.cd(icfg["path"]).keys())
        super().__init__(key, icfg, values)

class RunAsOfDateIterator(_ListIterator):
    """
    Iterator for run-as-of-date values based on specified parameters.

    Args:
        key (str): The key for the iterator.
        icfg (dict): Configuration settings for the iterator.
    """

    def __init__(self, key, icfg):
        """
        Initializes the run-as-of-date iterator.

        Args:
            key (str): The key for the iterator.
            icfg (dict): Configuration settings for the iterator.
        """
        values = icfg.get("values")
        icfg.setdefault("target", "run.as_of_date")

        start = icfg.get("start")
        if start is not None:
            end = icfg.get("end", "today")
            periodicity = icfg.get("periodicity", "daily")
            values = self._date_range(start, end, periodicity, values)
        elif values is None:
            raise ValueError("No values or start date provided for RunAsOfDateIterator")

        super().__init__(key, icfg, list(values))

    def _date_range(self, start, end, periodicity, more_values):
        """
        Generates a range of dates based on the specified parameters.

        Args:
            start (str): Start date.
            end (str): End date.
            periodicity (str): The frequency of the dates (daily, weekly, etc.).
            more_values (list): Additional values to include in the range.

        Returns:
            list: A sorted list of dates within the specified range.
        """
        from friendlydateparser import parse_date
        from dateutil.relativedelta import relativedelta
        start = parse_date(start)
        end = parse_date(end)
        if periodicity == "daily":
            step = relativedelta(days=1)
        elif periodicity == "weekly":
            step = relativedelta(days=7)
        elif periodicity == "monthly":
            step = relativedelta(months=1)
        elif periodicity == "yearly":
            step = relativedelta(years=1)
        else:
            raise ValueError(f"Unsupported periodicity {periodicity}")

        if more_values is None:
            more_values = []
        values = set(fdp.parse_date(v) for v in more_values)
        value = start
        while value <= end:
            values.add(value)
            value += step
        values = sorted(values)
        logging.debug(f"Date range from {start} to {end} with periodicity {periodicity} yields {values}")
        return values

    def reset(self):
        """Resets the current date to 'now' and initializes the run date."""
        super().reset()
        cfg['run.as_of_date'] = 'now'
        init_run_as_of_date()

    def next(self):
        """
        Advances the iterator to the next date value.

        Returns:
            bool: True if there is a next date; False otherwise.
        """
        if super().next():
            init_run_as_of_date()
            return True
        return False

def _init_iterator(key, icfg):
    """
    Initializes an iterator based on the specified type in configuration.

    Args:
        key (str): The key for the iterator.
        icfg (dict): Configuration settings for the iterator.

    Returns:
        _Iterator: An instance of a specific iterator class.

    Raises:
        NotImplementedError: If the iterator type is unsupported.
    """
    type = icfg.get("type", "value")
    if type == "values":
        return _ValuesIterator(key, icfg)
    elif type == "configkeys":
        return _ConfigKeysIterator(key, icfg)
    elif type == "runasofdate":
        return RunAsOfDateIterator(key, icfg)
    else:
        raise NotImplementedError(f"Unsupported iterator type {type} found in loop")

def _iterate(iterators):
    """
    Iterates through a list of iterators and yields the state of each.

    Args:
        iterators (list): List of iterator instances to be iterated.

    Yields:
        str: A string representation of the current state of all iterators.
    """
    level = 0
    while level >= 0:
        if iterators[level].next():
            if level < len(iterators) - 1:
                level += 1
            else:
                wheres = [i.where() for i in iterators]
                yield "/".join(wheres)
        else:
            iterators[level].reset()
            level -= 1

class _Loop(Action):
    """
    Action class that represents a loop of operations, utilizing specified iterators.

    Methods:
        do_it: Executes the loop action based on the configured sequence of operations.
    """

    def do_it(self):
        """
        Executes the loop action, iterating through the configured children actions.
        Logs iteration states and handles any exceptions based on configuration.
        """
        name = self._name

        children = [lookup(name, parent=self._name)
                    for name in self._cfg["sequence"]]

        iterators = []
        iicfg = self._cfg.cd("iterator")
        for key in iicfg.keys():
            icfg = iicfg.cd(key)
            iterators.append(_init_iterator(key, icfg))

        for where in _iterate(iterators):
            logging.info(f"Iterating at {where}")
            try:
                for child in children:
                    child.run()
            except Exception as ex:
                if self._cfg.get("ignore_errors", False):
                    logging.exception(f"Iteration {where} failed")
                else:
                    raise

register_class("loop", _Loop)
