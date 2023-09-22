import logging

_class_registry = {}

_suffix_registry = []  # vector of pairs (suffix, action type)

def register_class(action_type, action_class, *suffixes):
    """
    Register a new action class with its associated action type and suffixes.

    Parameters:
    action_type (str): The type of action being registered.
    action_class (type): The class that implements the action.
    suffixes (str): One or more file suffixes associated with the action type.
    """
    _class_registry[action_type] = action_class

    for suffix in suffixes:
        _suffix_registry.append((suffix, action_type))

    _suffix_registry.sort(key=lambda x: len(x[0]), reverse=True)

def _action_type_lookup(files):
    """
    Lookup the action type based on the provided file suffixes.

    Parameters:
    files (list): A list of file names to check against registered suffixes.

    Returns:
    str: The action type associated with the matched suffix, or None if no match is found.
    """
    logging.debug(f"suffix registry: {_suffix_registry}")

    for suffix, action_type in _suffix_registry:
        if suffix in files:
            return action_type
    return None

def _action_class_lookup(type):
    """
    Retrieve the action class associated with the given action type.

    Parameters:
    type (str): The action type to look up.

    Returns:
    type: The class associated with the action type.

    Raises:
    ValueError: If the action type is not supported.
    """
    if type in _class_registry:
        return _class_registry[type]
    raise ValueError(f"Unsupported action type {type}")
