import logging

from plpipes.config import cfg
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.action.runner import lookup
from plpipes.init import init_run_as_of_date
import plpipes

class _Iterator:
    def __init__(self, key, icfg):
        self._key = key
        self._cfg = icfg
        self.reset()

    def reset(self):
        pass

    def next(self):
        return False

    def where(self):
        return self._key

class _ListIterator(_Iterator):
    def __init__(self, key, icfg, values):
        self._values = values
        self._target = icfg["target"]
        super().__init__(key, icfg)

    def reset(self):
        self._ix = -1
        cfg[self._target] = None

    def next(self):
        self._ix += 1
        try:
            v = self._values[self._ix]
        except IndexError:
            return False
        logging.debug(f"Setting {self._target} to {v}")
        cfg[self._target] = v
        return True

    def where(self):
        return f"{self._key}={self._values[self._ix]}"

class _ValuesIterator(_ListIterator):
    def __init__(self, key, icfg):
        super().__init__(key, icfg, list(icfg["values"]))

class _ConfigKeysIterator(_ListIterator):
    def __init__(self, key, icfg):
        values = list(cfg.cd(icfg["path"]).keys())
        super().__init__(key, icfg, values)

class RunAsOfDateIterator(_ListIterator):
    def __init__(self, key, icfg):
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
        super().reset()
        cfg['run.as_of_date'] = 'now'
        init_run_as_of_date()

    def next(self):
        if super().next():
            init_run_as_of_date()
            return True
        return False

def _init_iterator(key, icfg):
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

    def do_it(self):
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
