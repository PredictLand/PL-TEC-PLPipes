
import yaml
import logging
import sys
import collections.abc
import re

from pathlib import Path

class ConfigStack:

    def __init__(self):
        self._stack = []

    def root(self):
        return _ConfigMapping(self, [])

    def _path_to_string(self, path, ix=None):
        if ix is not None:
            path = path[:ix]
        return ".".join(path)

    def _wrap(self, path, val):
        if isinstance(val, list):
            return _ConfigCollection(self, path)
        if isinstance(val, dict):
            return _ConfigMapping(self, path)
        return val

    def _get(self, path, default=None, _has=False):
        anchored = False # Once we go into an array or a non-dict
                         # object we stop the stack lookup-merge
                         # process. This flag indicates that
                         # condition.
        for level in self._stack:
            try:
                p = level
                for ix, key in enumerate(path):
                    if isinstance(p, list):
                        anchored = True
                        p = p[key]
                    elif isinstance(p, dict):
                        p = p[key]
                    else:
                        raise KeyError(self._path_to_string(path, ix))
                return True if _has else self._wrap(path, p)
            except (IndexError, KeyError):
                pass
            if anchored:
                break
        if default is None:
            raise KeyError(self._path_to_string(path))
        return default

    def _has(self, path):
        return self._get(path, _has=True)

    def _dict_iterator(self, path):
        anchored = False
        keys = set()
        for level in self._stack:
            try:
                p = level
                for key in path:
                    p = p[key]
                    if not isinstance(p, dict):
                        anchored = True

                if isinstance(p, dict):
                    for k in p:
                        if k not in keys:
                            yield k
                            keys.add(k)

            except (IndexError, KeyError):
                pass
            if anchored:
                break
        return

    def _list_iterator(self, path):
        anchored = False
        for level in self._stack:
            try:
                p = level
                for key in path:
                    p = p[key]
                    if not isinstance(p, dict):
                        anchored = True

                assert isinstance(p, list)
                for ix, v in enumerate(p):
                    yield self._wrap(path + [ix], v)
                return
            except (IndexError, KeyError):
                pass
            if anchored:
                break
        return

    def squash(self):
        pass

    def push(self, level):
        assert isinstance(level, dict)
        self._stack.insert(0, level)

    def push_file(self, fn):
        logging.debug(f"Reading configuration file {fn}")
        with open(fn, "r+", encoding="utf8") as f:
            if re.search(r'\.ya?ml', fn, re.IGNORECASE):
                import yaml
                level = yaml.safe_load(f)
            elif re.search(r'\.json', fn, re.IGNORECASE):
                import json
                level = json.load(f)
        self.push(level)

    def pop(self, level):
        self._stack.pop()

class _ConfigPtr:
    def __init__(self, stack, path):
        super().__init__()
        self._stack=stack
        self._path=path

    def __getattr__(self, key):
        return self._stack._get(self._path + [key])

    def __getitem__(self, key):
        return self._stack._get(self._path + [key])

    def __contains__(self, key):
        return self._stack._contains(self._path + [key])

    def __len__(self):
        len = 0
        for _ in self:
            len += 1
        return len

    def _child_as_tree(self, child):
        if isinstance(child, _ConfigPtr):
            return child._to_tree()
        return child

    def __repr__(self):
        return repr(self._to_tree())

    def __str__(self):
        return str(self._to_tree())

class _ConfigMapping(_ConfigPtr, collections.abc.Mapping):

    def _to_tree(self):
        return { k: self._child_as_tree(self[k]) for k in self }

    def __iter__(self):
        return self._stack._dict_iterator(self._path)

class _ConfigCollection(_ConfigPtr, collections.abc.Collection):

    def _to_tree(self):
        return [ self._child_as_tree(v) for v in self ]

    def __iter__(self):
        return self._stack._list_iterator(self._path)

