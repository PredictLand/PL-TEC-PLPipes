from plpipes.config import cfg
from pathlib import Path

"""
Module: plpipes.filesystem
This module provides functionalities for handling file paths, reading from and writing to various file formats,
and managing directories using the configuration settings defined in the application's config.
"""

def path(*args, mkparentdir=False, mkdir=False, **kwargs):
    """
    Generate a Path object based on the provided arguments.

    Args:
        *args: Positional arguments for the _path function.
        mkparentdir (bool): If True, create the parent directory of the path.
        mkdir (bool): If True, create the path directory.
        **kwargs: Additional keyword arguments for the _path function.

    Returns:
        Path: A Path object representing the constructed path.
    """
    p = _path(*args, **kwargs)
    if mkdir:
        p.mkdir(exist_ok=True, parents=True)
    elif mkparentdir:
        p.parent.mkdir(exist_ok=True, parents=True)
    return p

def _path(relpath=None, section=None):
    """
    Construct a Path object from a relative path and section.

    Args:
        relpath (str or None): Relative path to append to the base path.
        section (str or None): Configuration section to use.

    Returns:
        Path: A Path object representing the base path or a path with the relative path appended.
    """
    if section is None:
        section = "work"
    start = Path(cfg["fs." + section])
    if relpath is None:
        return start
    return start / relpath

def assign_section(target_section, relpath=None, section=None, **kwargs):
    """
    Assign a target section in the configuration and return a constructed path.

    Args:
        target_section (str): The section to assign.
        relpath (str or None): Relative path to use if section does not exist.
        section (str or None): Alternative section to use.
        **kwargs: Additional keyword arguments for the path function.

    Returns:
        Path: A Path object for the assigned section.
    """
    if target_section not in cfg.cd("fs"):
        if relpath is None:
            relpath = target_section
        cfg["fs." + target_section] = str(_path(relpath, section))
    return path(section=target_section, **kwargs)

def openfile(relpath, mode="r", section=None):
    """
    Open a file and return the file object.

    Args:
        relpath (str): Relative path to the file.
        mode (str): Mode in which to open the file (default is 'r').
        section (str or None): Configuration section to use.

    Returns:
        file object: The opened file object.
    """
    return open(path(relpath, section), mode)

def read_csv(relpath, section=None, **kwargs):
    """
    Read a CSV file and return a DataFrame.

    Args:
        relpath (str): Relative path to the CSV file.
        section (str or None): Configuration section to use.
        **kwargs: Additional keyword arguments for pandas read_csv.

    Returns:
        DataFrame: DataFrame containing the CSV data.
    """
    import pandas as pd
    return pd.read_csv(path(relpath, section), **kwargs)

def write_csv(relpath, df, section=None, mkdir=True, **kwargs):
    """
    Write a DataFrame to a CSV file.

    Args:
        relpath (str): Relative path for the CSV file.
        df (DataFrame): DataFrame to write.
        section (str or None): Configuration section to use.
        mkdir (bool): If True, create the directory if it does not exist.
        **kwargs: Additional keyword arguments for pandas to_csv.

    Returns:
        None
    """
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, **kwargs)

def write_text(relpath, text, section=None, mkdir=True):
    """
    Write text to a file.

    Args:
        relpath (str): Relative path for the text file.
        text (str): Text to write to the file.
        section (str or None): Configuration section to use.
        mkdir (bool): If True, create the directory if it does not exist.

    Returns:
        int: Number of characters written to the file.
    """
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w") as f:
        return f.write(text)

def read_text(relpath, section=None, encoding="utf-8"):
    """
    Read text from a file.

    Args:
        relpath (str): Relative path to the text file.
        section (str or None): Configuration section to use.
        encoding (str): Encoding to use for reading the file (default is 'utf-8').

    Returns:
        str: The contents of the file as a string.
    """
    target = path(relpath, section)
    with open(target, "r", encoding=encoding) as f:
        return f.read()

def write_yaml(relpath, data, section=None, mkdir=True, **kwargs):
    """
    Write data to a YAML file.

    Args:
        relpath (str): Relative path for the YAML file.
        data (any): Data to serialize to YAML.
        section (str or None): Configuration section to use.
        mkdir (bool): If True, create the directory if it does not exist.
        **kwargs: Additional keyword arguments for yaml.dump.

    Returns:
        None
    """
    import yaml
    target = path(relpath, section)
    if mkdir:
        target.parent.mkdir(parents=True, exist_ok=True)
    with open(target, "w") as f:
        yaml.dump(data, f, **kwargs)

def read_yaml(relpath, section=None):
    """
    Read data from a YAML file.

    Args:
        relpath (str): Relative path to the YAML file.
        section (str or None): Configuration section to use.

    Returns:
        any: The deserialized data from the YAML file.
    """
    import yaml
    with openfile(relpath, section=section) as f:
        return yaml.safe_load(f)

def read_json(relpath, section=None):
    """
    Read data from a JSON file.

    Args:
        relpath (str): Relative path to the JSON file.
        section (str or None): Configuration section to use.

    Returns:
        any: The deserialized data from the JSON file.
    """
    import json
    with openfile(relpath, section=section) as f:
        return json.load(f)

def tempdir(parent=None):
    """
    Create a temporary directory for file operations.

    Args:
        parent (str or None): Parent directory for the temporary directory.

    Returns:
        TemporaryDirectory: Temporary directory context manager.
    """
    if parent is None:
        parent = fs.path("tmp")
    import tempfile

    return tempfile.TemporaryDirectory(dir=parent)

def read_excel(relpath, section=None, **kwargs):
    """
    Read data from an Excel file and return a DataFrame.

    Args:
        relpath (str): Relative path to the Excel file.
        section (str or None): Configuration section to use.
        **kwargs: Additional keyword arguments for pandas read_excel.

    Returns:
        DataFrame: DataFrame containing the Excel data.
    """
    import pandas as pd
    return pd.read_excel(path(relpath, section), **kwargs)

def write_excel(relpath, df, section=None, mkparentdir=True, autofilter=False, **kwargs):
    """
    Write a DataFrame to an Excel file.

    Args:
        relpath (str): Relative path for the Excel file.
        df (DataFrame): DataFrame to write.
        section (str or None): Configuration section to use.
        mkparentdir (bool): If True, create the parent directory if it does not exist.
        autofilter (bool): If True, apply autofilter to the written Excel file.
        **kwargs: Additional keyword arguments for pandas to_excel.

    Returns:
        Path: The path of the written Excel file.
    """
    target = path(relpath, section=section, mkparentdir=mkparentdir)
    df.to_excel(target, index=False, **kwargs)

    if autofilter:
        import openpyxl
        wb = openpyxl.load_workbook(target)
        ws = wb.active
        ws.auto_filter.ref = ws.dimensions
        wb.save(target)

    return target
