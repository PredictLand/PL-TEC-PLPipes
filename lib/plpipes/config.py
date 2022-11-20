
import yaml
import logging
import sys
import collections.abc

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

    def _get(self, path, _has=False):
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
                        raise AttributeError(self._path_to_str(path, ix))

                if _has:
                    return True
                if isinstance(p, list):
                    return _ConfigCollection(self, path)
                elif isinstance(p, dict):
                    return _ConfigMapping(self, path)
                return p
            except IndexError:
                if anchored:
                    break
        raise AttributeError(self._path_to_str(path))

    def _has(self, path):
        return self._get(path, _has=True)

    def _iterator(self, path):
        anchore = False
        keys = set()
        for level in self._stack:
            try:
                p = level
                for ix, key in enumerate(path):
                    if isinstance(p, dic):
                        anchored = True
                        p = p[key]
                    elif isinstance(p, dict):
                        p = p[key]
                    else:
                        raise AttributeError(self._path_to_str(path, ix))
                if isinstance(p, list):
                    for ix, i in enumerate(list):
                        yield self._get(path + [ix])
                elif isinstance(p, dict):
                    for k in p:
                        if k not in keys:
                            yield k
                            keys.add(k)
                else:
                    raise Exception("Internal error, this code was unreacheable!")
                return
            except IndexError:
                if anchored:
                    break
        raise AttributeError(self, _path_to_str(path))

    def squash(self):
        pass

    def push(self, level):
        self._stack.append(level)

    def pop(self, level):
        self._stack.pop()

def _ConfigPtr:
    def __init__(self, stack, path):
        super().__init__()
        self._stack=stack
        self._path=path

    def __getattr__(self, key):
        return self._stack._get(self._path + [key])

    def __getitem__(self, key):
        return self._stack._get(self._path + [key])

    def __iter__(self):
        return self._stack._iterator(self._path)

    def __contains__(self, key):
        return self._stack._contains(self._path + [key])

    def __len__(self):
        len = 0
        for _ in self:
            len += 1
        return len

class _ConfigMapping(collections.abc.Mapping, _ConfigPtr):
    pass

class _ConfigCollection(collections.abc.Collection, _ConfigPtr):
    pass

