
import logging

_class_registry = {}

_suffix_registry = [] # vector of pairs (suffix, action type)

def register_class(action_type, action_class, *suffixes):
    _class_registry[action_type] = action_class

    for suffix in suffixes:
        _suffix_registry.append((suffix, action_type))

    _suffix_registry.sort(key=lambda x: len(x[0]))

def _action_type_lookup(files):

    logging.debug(f"suffix registry: {_suffix_registry}")

    for suffix, action_type in _suffix_registry:
        if suffix in files:
            return action_type
    return None

def _action_class_lookup(type):
    if type in _class_registry:
        return _class_registry[type]
    raise ValueError(f"Unsupported action type {type}")

