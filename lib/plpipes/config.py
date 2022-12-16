
import yaml
import logging
import sys
import re
import copy
import collections.abc

from pathlib import Path

class ConfigStack:
    def __init__(self, top=None):
        if not top:
            top = _Level()
        self._top = top

    def _cd(self, path):
        return _Ptr(self, path)

    def root(self):
        return self._cd("")

    def reset_cache(self):
        self._top._reset_cache()

class _Ptr(collections.abc.MutableMapping):
    def __init__(self, stack, path):
        self._path = path
        self._stack = stack

    def cd(self, key):
        return self._stack._cd(self._mkkey(key)) if key else self

    def _mkkey(self, key):
        if key:
            if self._path:
                return f"{self._path}.{key}"
            return key
        return self._path

    def __getitem__(self, key):
        return self._stack._top._get(self._mkkey(key))

    def __contains__(self, item):
        return self._stack._top._contains(self._mkkey(key))

    def __setitem__(self, key, value):
        return self._stack._top._set(self._mkkey(key), value)

    def __delitem__(self, key):
        self._stack._level._del(key)

    def __len__(self):
        self._stack._level._len()

    def to_tree(self, key=""):
        return self._stack._top._to_tree(self._mkkey(key))

    def merge(self, tree, key="", frame=0):
        return self._stack._top._merge(self._mkkey(key), tree, frame)

    def merge_file(self, fn, path="", frame=0):
        logging.debug(f"Reading configuration file {fn}")
        with open(fn, "r+", encoding="utf8") as f:
            if re.search(r'\.ya?ml', fn, re.IGNORECASE):
                import yaml
                tree = yaml.safe_load(f)
            elif re.search(r'\.json', fn, re.IGNORECASE):
                import json
                tree = json.load(f)
        self.merge(self._mkkey(key), tree, path, frame)

    def fs(self, key, set_default=None):
        if default:
            return Path(self[f"fs.{key}"])

    def squash_frames(self):
        self._stack._top._squash_frames()

    def __iter__(self):
        ...

    def __str__(self):
        return str(self._stack._top._to_tree(""))

def _merge_any(tree, new):
    if isinstance(new, dict):
        if isinstance(tree, dict):
            for k, v in new.items():
                tree[k] = _merge_any(tree[k], v) if k in tree else copy.deepcopy(v)
            return tree
    return copy.deepcopy(new)

class _Level:
    def __init__(self, root={}, parent=None):
        self._root = {}
        self._parent = parent
        self._merge("", root)

    def _reset_cache(self):
        self._cache={}
        if self._parent:
            self._parent._reset_cache(self)

    def _merge(self, key, value, frame=0):
        if frame < 0:
            if not self._parent:
                self._parent = _Level()
            self._parent._merge(key, value, frame+1)
        else:
            tree = self._root
            if key:
                parts = key.split(".")
                last = parts.pop()
                for p in parts:
                    if (p not in tree) or (not isinstance(tree[p], dict)):
                        tree[p] = {}
                    tree = tree[p]
                tree[last] = _merge_any(tree.get(last, None), value)
            else:
                if not isinstance(value, dict):
                    raise ValueError("Top configuration must be a dictionary")
                self._root = _merge_any(tree, value)
        self._cache = {}

    def _merge_default(self, key, value):
        ...

    def _set(self, key, value):
        if isinstance(value, dict) or isinstance(value, list):
            raise ValueError("It is not possible to set a configuration entry to a dictionary or list, use merge instead")
        elif not isinstance(value, int):
            value = str(value)
        self._merge(key, value)

    def _get_nocache(self, key):
        parts = key.split(".")
        v = self._root
        for p in parts:
            try:
                v = v[p]
            except TypeError:
                raise KeyError(f"Config key '{key}' traversing blocked by a non dictionary object")
            except KeyError as ex:
                if self._parent:
                    return self._parent._get(key)
                raise ex

        if isinstance(v, dict) or isinstance(v, list):
            raise KeyError(f"config key '{key}' does not point to a terminal node")
        return v

    def _get(self, key):
        if key not in self._cache:
            self._cache[key] = self._get_nocache(key)
        return self._cache[key]

    def _contains(self, key):
        if key in self._cache:
            return True
        parts = key.split(".")
        v = self._root
        for p in parts:
            try:
                v = v[p]
            except TypeError:
                return False
            except KeyError as ex:
                return self._parent._contains(self, key) if self._parent else False
        return True

    def _to_tree(self, key):
        parent_ex = None
        tree = {}
        if self._parent:
            try:
                tree = self._parent._to_tree(key)
            except Exception as ex:
                parent_ex = ex

        v = self._root
        if key:
            parts = key.split(".")
            for p in parts:
                try:
                    v = v[p]
                except TypeError:
                    raise KeyError(f"Config key '{key}' traversing blocked by a non dictionary object")
                except:
                    if parent_ex:
                        raise parent_ex
                    return tree
        return _merge_any(tree, v)

    def _steal_root(self):
        self._squash_frames()
        root = self._root
        del self._root
        return root

    def _squash_frames(self):
        if self._parent:
            self._tree = _merge_any(self._parent._steal_root(), self._root)
            self._parent = None
