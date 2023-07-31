import logging

from plpipes.config import cfg
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.action.runner import lookup

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


def _init_iterator(key, icfg):
    type = icfg.get("type", "value")
    if type == "values":
        return _ValuesIterator(key, icfg)
    elif type == "configkeys":
        return _ConfigKeysIterator(key, icfg)
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
