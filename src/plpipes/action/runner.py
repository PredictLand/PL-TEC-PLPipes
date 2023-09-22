import pathlib
import logging
import re

from plpipes.config import cfg
from plpipes.action.registry import _action_class_lookup, _action_type_lookup

_action_cache = {}

def _find_action_files(action_root, name):
    """
    Find action files in the specified directory for a given action name.

    Args:
        action_root (pathlib.Path): The root directory to search for action files.
        name (str): The name of the action.

    Returns:
        dict: A dictionary containing the found action files categorized by their suffix.
    """
    root = action_root.absolute()
    name1 = name.replace(".", "/")
    dn = root / name1
    logging.debug(f"dn: {dn}")
    files = {}
    if dn.is_dir():
        files["dir"] = str(dn)
    glob_pattern = f"{name1}.*"
    logging.debug(f"calling root: {root}, pattern: {glob_pattern}")
    for fn in root.glob(glob_pattern):
        logging.debug(f"checking {fn}")
        if fn.is_file():
            suffix = "".join(fn.suffixes)[1:].replace(".", "_")
            if re.match("[a-z0-9_\.]+$", suffix, re.IGNORECASE):
                files.setdefault(suffix, str(fn))

    logging.debug(f"files found for action {name}: {files}")

    return files

def resolve_action_name(name, parent):
    """
    Resolve the full action name from a relative action name and its parent.

    Args:
        name (str): The action name to resolve.
        parent (str): The parent action name.

    Returns:
        str: The resolved full action name.

    Raises:
        ValueError: If the name is a relative name but no parent is provided.
    """
    if name.startswith("."):
        if parent:
            return f"{parent}{name}"
        else:
            raise ValueError("Can't resolve relative action name without a parent")
    return name

def lookup(name, parent=""):
    """
    Lookup and retrieve an action by its name.

    Args:
        name (str): The name of the action to look up.
        parent (str): An optional parent action name.

    Returns:
        Action: The action instance corresponding to the provided name.

    Raises:
        ValueError: If the action type is not declared or the action file is not found.
    """
    name = resolve_action_name(name, parent)
    if name not in _action_cache:
        actions_dir = pathlib.Path(cfg["fs.actions"])
        files = _find_action_files(actions_dir, name)

        cfg_path = "actions." + ".children.".join(name.split("."))
        acfg = cfg.cd(cfg_path)

        for ext in ("yaml", "json"):
            if ext in files:
                acfg.merge_file(files[ext], frame=-1)

        for k, v in files.items():
            acfg.setdefault(f"files.{k}", v)

        action_type = acfg.setdefault("type", _action_type_lookup(files))
        if action_type is None:
            raise ValueError(f"Action {name} has no type declared or action file not found")

        logging.debug(f"action_type: {action_type}")
        _action_cache[name] = _action_class_lookup(action_type)(name, acfg)

    return _action_cache[name]

def run(name):
    """
    Execute the action corresponding to the provided name.

    Args:
        name (str): The name of the action to run.
    """
    lookup(name).run()
