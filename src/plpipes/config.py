import json
import logging
import re
import copy
import collections.abc

def _merge_any(tree, new):
    if isinstance(new, dict):
        if isinstance(tree, dict):
            for k, v in new.items():
                tree[k] = _merge_any(tree[k], v) if k in tree else copy.deepcopy(v)
            return tree
        else:
            return {k: _merge_any(None, v) for k, v in new.items()}
    if isinstance(new, list):
        return [_merge_any(None, e) for e in new]
    if isinstance(new, (int, float, bool)) or new is None:
        return new
    return str(new)

def _flatten_tree(tree):
    flat = {}
    def rec(subtree, path):
        for k, v in subtree.items():
            child_path = path + [k]
            if isinstance(v, dict):
                rec(v, child_path)
            else:
                flat[".".join(child_path)] = v
    rec(tree, [])
    return flat


class ConfigStack:
    def __init__(self):
        self._frames = []
        self._cache = {}

    def _cd(self, path):
        return _Ptr(self, path)

    def root(self):
        return self._cd("")

    def reset_cache(self):
        self._cache = {}

    def _get(self, key, frame=0):
        if frame == 0:
            if key not in self._cache:
                self._cache[key] = self._get_nocache(key, 0)
            return self._cache[key]
        else:
            return self._get_nocache(key, frame)

    def _get_nocache(self, key, frame):
        # As we have to take into account wildcards at different
        # levels, we use a search algorithm that explores the problem
        # space taking into account the specificity of the entries (no
        # wildcards, or wildcards nearer to the right side) and the
        # frame position. The rules are as follows:
        #
        # 1. More specific entries always win over less specific ones.
        #
        # 2. For entries with the same specificity, the one from the
        # lowest frame (last loaded) wins.
        #
        # This is implemented using a mix of A*/depth-first search
        # algorithm.

        (key_part, *right) = key.split(".")
        # queue structure:
        #   specificity, frame_ix, tree, left_path, frozen_key, rigth_path
        queue = [(('!', '*')[k], ix, f, (key_part, '*')[k], (key_part, '*')[k], right)
                 for ix, f in enumerate(self._frames[frame:])
                 for k in (0, 1)]
        while queue:
            queue.sort(reverse=True)
            (specifity, frame_ix, tree, left, key_part, right) = queue.pop()
            while True:
                try:
                    if isinstance(tree, dict):
                        tree = tree[key_part]
                    elif isinstance(tree, list):
                        queue = []
                        if key_part.isnumeric():
                            tree = tree[int(key_part)]
                        else:
                            raise IndexError("Expecting numeric key")
                    else:
                        raise ValueError(f"Config key '{key}' traversing blocked by a non dictionary object at {left}")
                except (IndexError, KeyError):
                    break

                if right:
                    (key_part, *right) = right
                    queue.append((specifity + "*", frame_ix, tree, left + ".*", '*', right))
                    left = left + "." + key_part
                    specifity += "!"
                else:
                    if isinstance(tree, dict):
                        raise ValueError(f"config key '{key}' does not point to a terminal node")
                    return tree
        raise KeyError(f"config key '{key}' not found")

    def _contains(self, key):
        try:
            self._get(key, 0)
            return True
        except KeyError:
            return False

    def _merge(self, key, newtree, frame=0):
        # Auto-allocate frames
        if len(self._frames) <= frame:
            self._frames += [{} for _ in range(frame - len(self._frames) + 1)]
        tree = self._frames[frame]

        if key != "":
            parts = key.split(".")
            last = parts.pop()
            for p in parts:
                if (p not in tree) or (not isinstance(tree[p], dict)):
                    tree[p] = {}
                tree = tree[p]
            tree[last] = _merge_any(tree.get(last, None), newtree)
        else:
            if not isinstance(newtree, dict):
                raise ValueError("Top configuration must be a dictionary")
            self._frames[frame] = _merge_any(tree, newtree)
        self._cache = {}

    def _set(self, key, value):
        if not isinstance(value, (str, int, float, bool, list)) and value is not None:
            if isinstance(value, dict):
                raise ValueError("It is not possible to set a configuration entry to a dictionary, use merge instead")
            value = str(value)
        self._merge(key, value)

    def _multicd(self, key):
        # queue structure:
        #   specificity, frame_ix, tree
        queue = [("", ix, f) for ix, f in enumerate(self._frames)]
        if key != "":
            right = key.split(".")
            while right:
                queue.sort()
                key = right.pop(0)
                new_queue = []
                for specifity, ix, tree in queue:
                    if isinstance(tree, dict):
                        for s in ('!', '*'):
                            k = s if s == '*' else key
                            if k in tree:
                                new_queue.append((specifity + s, ix, tree[k]))
                    elif not new_queue:
                        raise ValueError(f"Config key '{key}' blocked by a non dictionary object")
                    else:
                        break
                queue = new_queue
        return [t for _, _, t in sorted(queue, reverse=True)]

    def _to_tree(self, key, defaults=None):
        m = self._multicd(key)
        tree = {}
        if defaults is not None:
            tree = _merge_any(tree, defaults)
        for other in m:
            tree = _merge_any(tree, other)
        return tree

    def _keys(self, key):
        m = self._multicd(key)
        seen = set()
        inner_is_dict = True
        for other in m:
            if isinstance(other, dict):
                if not inner_is_dict:
                    inner_is_dict = True
                    seen = set()
                for k in other.keys():
                    if k != '*':
                        seen.add(k)
            else:
                inner_is_dict = False
        if inner_is_dict:
            return sorted(seen)
        raise ValueError(f"Config key '{key}' blocked by a non dictionary object")

    def _squash_frames(self):
        tree = self._frames.pop()
        while self._frames:
            tree = _merge_any(tree, self._frames.pop())
        self._frames.append(tree)

class _Ptr(collections.abc.MutableMapping):
    def __init__(self, stack, path):
        self._path = path
        self._stack = stack

    def cd(self, key):
        return self._stack._cd(self._mkkey(key)) if key else self

    def _mkkey(self, key):
        if key == "":
            return self._path
        if self._path == "":
            return key
        return f"{self._path}.{key}"

    def __getitem__(self, key):
        return self._stack._get(self._mkkey(key))

    def __contains__(self, key):
        return self._stack._contains(self._mkkey(key))

    def __setitem__(self, key, value):
        return self._stack._set(self._mkkey(key), value)

    def __delitem__(self, key):
        self._stack._del(key)

    def __len__(self):
        return len(self.__keys__())

    def to_tree(self, key="", defaults=None):
        return self._stack._to_tree(self._mkkey(key), defaults)

    def to_flat_dict(self, key="", defaults=None):
        t = self._stack._to_tree(self._mkkey(key), defaults)
        return _flatten_tree(t)

    def to_json(self, key="", defaults=None):
        return json.dumps(self.to_tree(key, defaults))

    def merge(self, tree, key="", frame=0):
        return self._stack._merge(self._mkkey(key), tree, frame)

    def merge_file(self, fn, key="", frame=0):
        logging.debug(f"Reading configuration file {fn}")
        with open(fn, "r+", encoding="utf8") as f:
            if re.search(r'\.ya?ml', str(fn), re.IGNORECASE):
                import yaml
                tree = yaml.safe_load(f)
            elif re.search(r'\.json', str(fn), re.IGNORECASE):
                import json
                tree = json.load(f)
            else:
                raise ValueError(f"Can't determine file type for {str(fn)}")
        self.merge(tree, key, frame=frame)

    def squash_frames(self):
        self._stack._squash_frames()

    def __iter__(self):
        for k in self._stack._keys(self._mkkey("")):
            yield k

    def __keys__(self):
        return self._stack._keys(self._mkkey(""))

    def __str__(self):
        return str(self._stack._to_tree(""))

    def copydefaults(self, src, *keys, **keys_with_default):
        for key in keys:
            if key not in self:
                if key in src:
                    self[key] = src[key]
                else:
                    logging.debug(f"key {key} not found!")
        for key, default in keys_with_default.items():
            if key not in self:
                self[key] = src.get(key, default)

    def setdefault_lazy(self, key, cb):
        if key not in self:
            self[key] = cb()
        return self[key]

cfg_stack = ConfigStack()
cfg = cfg_stack.root()
