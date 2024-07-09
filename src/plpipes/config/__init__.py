import json
import logging
import re
import copy
import collections.abc

from .magic import AsteriskMagic, LinkMagic, InterpolateMagic

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
                # print(f"_get_nocache({key}, 0) --> {self._cache[key]}") 
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
        # 1. Entries from higher frames always win over entries from
        # lower ones.
        #
        # 2. For entries from the same frame, the most specific one
        # wins.
        #
        # The implementatio keeps a queue of entries to explore, each
        # entry is a tuple with the following fields:
        #
        # - tree: the subtree to explore.
        # - left: the path traversed so far, used for error messages.
        # - right: the remaining path to traverse, the next keys to explore.
        # - vlinks: a set of visited links, to avoid infinite loops.
        #
        # The algorithm also takes into account magic entries
        # implementing special behaviour:
        #
        # - AsteriskMagic: a wildcard that matches any key.
        # - LinkMagic: a link to another key.
        # - InterpolateMagic: a string that is interpolated.

        right = [x
                 for x in key.split(".")
                 if x != ""]

        queue = [(tree, [], right, set()) for tree in self._frames[frame:]][::-1]

        while queue:
            (tree, left, right, vlinks) = queue.pop()
            if isinstance(tree, LinkMagic):
                # Links cut the search, so we fully replace the queue.
                src = ".".join(left)
                if src in vlinks:
                    raise ValueError(f"Config key '{key}' has a loop in the links at {src}")
                link = [x
                        for x in tree.link.split(".")
                        if x != ""]

                queue = [(tree, [], link + right, vlinks.union({src})) for tree in self._frames[frame:]][::-1]
            else:
                if right:
                    (key, *right) = right
                    if isinstance(tree, dict):
                        # AsteriskMagic path is inserted first so that it is looked the last.
                        if AsteriskMagic in tree:
                            queue.append((tree[AsteriskMagic], left + ["*"], right, vlinks))
                        if key in tree:
                            queue.append((tree[key], left + [key], right, vlinks))
                    elif isinstance(tree, list):
                        if key.isnumeric():
                            queue.append((tree[int(key)], left + [key], right, vlinks))
                        else:
                            raise IndexError(f"Config key '{key}' traversing blocked by a list expecting "
                                             f"a numeric subkey at {'.'.join(left + [key])}")
                    else:
                        raise ValueError(f"Config key '{key}' traversing blocked by a non dictionary object at {left}")

                else:
                    if isinstance(tree, dict):
                        raise ValueError(f"Config key '{key}' does not point to a terminal node")
                    if isinstance(tree, InterpolateMagic):
                        return tree.interpolate(self, key, frame)
                    return tree

        raise KeyError(f"Config key '{key}' not found")

    def _contains(self, key):
        try:
            self._get(key, 0)
            return True
        except KeyError:
            return False

    def _to_tree(self, key, defaults=None):
        queue = self.__init_traverse(defaults)
        queue = self.__traverse_key(queue, key)
        return self.__traverse_tree(queue)

    def __init_traverse(self, defaults=None):
        queue = [tree for tree in self._frames]
        if defaults is not None:
            queue.append(defaults)
        return queue

    def __traverse_key(self, queue, key):
        for part in key.split("."):
            if part is not None and part != "":
                queue = self.__traverse_part(queue, part)
        return queue

    def __init_traverse_key(self, key):
        queue = self.__init_traverse()
        return self.__traverse_key(queue, key)

    def __traverse_part(self, queue, part):
        queue = queue[::-1] # besides inverting the queue, we also do a copy so we can destructively manipulate it!
        new_queue = []
        first = True
        while queue:
            print(f"queue: {queue}")
            t = queue.pop()
            if isinstance(t, LinkMagic):
                queue = self.__init_traverse_key(t.link)[::-1]
                continue
            if isinstance(t, dict):
                if part in t:
                    new_queue.append(t[part])
                if AsteriskMagic in t:
                    new_queue.append(t[AsteriskMagic])
            elif first:
                if isinstance(t, list):
                    if int(part) in t:
                        new_queue = [t[part]]
                    break
                else:
                    raise ValueError(f"Config key '{part}' traversing blocked by a non dictionary object")
            else:
                break
            first = False
        return new_queue

    def __traverse_tree(self, queue):
        queue1 = queue[::-1]
        if queue1:
            first = queue1.pop()
            if isinstance(first, LinkMagic):
                return self._to_tree(first.link)
            if isinstance(first, dict):
                keys = set(first.keys())
                while queue1:
                    t = queue1.pop()
                    if isinstance(t, LinkMagic):
                        queue1 = self.__init_traverse_key(t.link)[::-1]
                        continue
                    if isinstance(t, dict):
                        keys.update(t.keys())
                    else:
                        break
                return {k: self.__traverse_subtree(queue, k) for k in keys}
            if isinstance(first, list):
                return [self.__traverse_subtree([first], i) for i in range(len(first))]
            else:
                return first
        else:
            return None

    def __traverse_subtree(self, queue, part):
        q = self.__traverse_part(queue, part)
        return self.__traverse_tree(q)

    def _merge_final(self, tree, new):
        if isinstance(new, LinkMagic):
            trees = self._multicd(new.link)
            for t in trees:
                tree = self._merge_final(tree, t)
            return tree

        if isinstance(new, dict):
            if isinstance(tree, dict):
                for k, v in new.items():
                    if k == AsteriskMagic:
                        pass  # TODO: handle wildcards correctly!
                    tree[k] = self._merge_final(tree.get(k, None), v)
            else:
                return {k: self._merge_final(None, v) for k, v in new.items()}
            if isinstance(new, list):
                return [self._merge_final(None, e) for e in new]
            if isinstance(new, InterpolateMagic):
                return new.interpolate(self, "", 0)
            if isinstance(new, (int, float, bool)) or new is None:
                return new
            return str(new)

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
        queue = [(ix, "", f) for ix, f in enumerate(self._frames)]
        if key != "":
            right = key.split(".")
            while right:
                queue.sort()
                key = right.pop(0)
                new_queue = []
                for ix, specifity, tree in queue:
                    if isinstance(tree, dict):
                        for s in ('!', '*'):
                            k = s if s == '*' else key
                            if k in tree:
                                new_queue.append((ix, specifity + s, tree[k]))
                    elif not new_queue:
                        raise ValueError(f"Config key '{key}' blocked by a non dictionary object")
                    else:
                        break
                queue = new_queue
        return [t for _, _, t in sorted(queue, reverse=True)]

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

    def merge_fh(self, fh, format="yaml", key="", frame=0):
        if format == "yaml":
            from . import yaml
            tree = yaml.load(fh)
        elif format == "json":
            tree = json.load(fh)
        else:
            raise ValueError(f"Unsupported file format {format}")
        self.merge(tree, key, frame)

    def merge_file(self, fn, key="", frame=0, format=None):
        fn = str(fn)
        logging.debug(f"Reading configuration file {fn}")
        if format is None:
            if re.search(r'\.ya?ml', fn, re.IGNORECASE):
                format = "yaml"
            elif re.search(r'\.json', fn, re.IGNORECASE):
                format = "json"
            else:
                raise ValueError(f"Can't determine file type for {fn}")
        with open(fn, "r+", encoding="utf8") as f:
            self.merge_fh(f, format, key, frame)

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

