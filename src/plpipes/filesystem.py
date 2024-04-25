from plpipes.config import cfg
from pathlib import Path

def path(*args, mkparentdir=False, mkdir=False, **kwargs):
    p = _path(*args, **kwargs)
    if mkdir:
        p.mkdir(exist_ok=True, parents=True)
    elif mkparentdir:
        p.parent.mkdir(exist_ok=True, parents=True)
    return p

def _path(relpath=None, section=None):
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

def read_text(relpath, section=None, encoding="utf-8"):
    target = path(relpath, section)
    with open(target, "r", encoding=encoding) as f:
        return f.read()

def write_yaml(relpath, data, section=None, mkdir=True, **kwargs):
    import yaml
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w") as f:
        yaml.dump(data, f, **kwargs)

def read_yaml(relpath, section=None):
    import yaml
    with openfile(relpath, section=section) as f:
        return yaml.safe_load(f)

def read_json(relpath, section=None):
    import json
    with openfile(relpath, section=section) as f:
        return json.load(f)

def tempdir(parent=None):
    if parent is None:
        parent = fs.path("tmp")
    import tempfile

    return tempfile.TemporaryDirectory(dir=parent)

def read_excel(relpath, section=None, **kwargs):
    import pandas as pd
    return pd.read_excel(path(relpath, section), **kwargs)

def write_excel(relpath, df, section=None, mkparentdir=True, autofilter=False, **kwargs):
    target = path(relpath, section=section, mkparentdir=mkparentdir)
    df.to_excel(target, index=False, **kwargs)

    if autofilter:
        import openpyxl
        wb = openpyxl.load_workbook(target)
        ws = wb.active
        ws.auto_filter.ref = ws.dimensions
        wb.save(target)


    return target
