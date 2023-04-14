from plpipes.config import cfg
from pathlib import Path

def path(relpath=None, section=None):
    if section is None:
        section = "work"
    start = Path(cfg["fs." + section])
    if relpath is None:
        return start
    return start / relpath

def openfile(relpath, mode="r", section=None):
    return open(path(relpath, section), mode)

def read_csv(relpath, section=None, **kwargs):
    import pandas as pd
    return pd.read_csv(path(relpath, section), **kwargs)

def write_csv(relpath, df, section=None, mkdir=True, **kwargs):
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, **kwargs)

def write_text(relpath, text, section=None, mkdir=True):
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w") as f:
        return f.write(text)

def read_yaml(relpath, section=None):
    import yaml
    with openfile(relpath, section=section) as f:
        return yaml.safe_load(f)

def read_json(relpath, section=None):
    import json
    with openfile(relpath, section=section) as f:
        return json.load(f)
