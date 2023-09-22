"""
This module contains the logic for generating SQL code using the Jinja2 template system.
It provides several convenient helpers for easing SQL generation.
"""

import jinja2
import re
import logging
import importlib

from plpipes.config import cfg
from plpipes.util import pluralsingular

_SQL_RESERVED_WORDS = {'select', 'from', 'where', 'join', 'order', 'group', 'having'}

def _quote(val):
    """Quote a value by adding single quotes and handling internal single quotes.

    Args:
        val (str): The value to be quoted.

    Returns:
        str: The quoted value, wrapped in single quotes if necessary.
    """
    val = val.replace("'", "''")
    return f"'{val}'"

def _escape(arg, pre=None, post=None, esc_char='"'):
    """Escape an identifier name if necessary by adding double quotes and handling internal double quotes.

    Args:
        arg (str): The identifier name to be escaped.
        pre (str, optional): Prefix to add to the identifier name.
        post (str, optional): Suffix to add to the identifier name.
        esc_char (str, optional): The character used for escaping, defaults to double quote.

    Returns:
        str: The escaped identifier name, wrapped in double quotes if necessary.
    """
    if arg is None:
        raise ValueError("Cannot escape None value")
    if pre is not None:
        arg = pre + arg
    if post is not None:
        arg = arg + post
    if not re.match(r'^\w+$', arg) or arg.lower() in _SQL_RESERVED_WORDS:
        arg = arg.replace(esc_char, esc_char * 2)
        return esc_char + {arg} + esc_char
    return arg

def _join_columns(columns, table_name=None, pre=None, post=None, sep=", "):
    """Join a list of columns into a single SQL-compatible string with optional table prefixes and proper escaping.

    Args:
        columns (list of str): List of column names to be joined.
        table_name (str, optional): Table name to prefix each column with, e.g., "t". If None, no prefix is used.
        pre (str, optional): Prefix to add to each column name.
        post (str, optional): Suffix to add to each column name.
        sep (str, optional): Separator to use between the joined columns, defaults to ", ".

    Returns:
        str: A single string containing the joined columns, separated by ", ".

    Usage:
        >>> join_columns(['id', 'name', 'email'])
        'id, name, email'

        >>> join_columns(['id', 'name', 'email'], table_name='t')
        't.id, t.name, t.email'

        >>> join_columns(['select', 'name with space', 'email'], table_name='t')
        't."select", t."name with space", t.email'
    """
    if isinstance(columns, str):
        columns = [columns]
    if pre is not None:
        columns = [pre + col for col in columns]
    if post is not None:
        columns = [col + post for col in columns]
    columns = [_escape(col) for col in columns]
    if table_name:
        table_name = _escape(table_name)
        columns = [f'{table_name}.{col}' for col in columns]
    return sep.join(columns)

def _pluralize(word, marks=False, **kwargs):
    """Pluralize a word using the pluralsingular utility.

    Args:
        word (str): The word to be pluralized.
        marks (bool, optional): Whether to include marks in the pluralized word.

    Returns:
        str: The pluralized form of the word.
    """
    return pluralsingular.pluralize(word, marks=marks, **kwargs)

def _singularize(word, marks=False, **kwargs):
    """Singularize a word using the pluralsingular utility.

    Args:
        word (str): The word to be singularized.
        marks (bool, optional): Whether to include marks in the singularized word.

    Returns:
        str: The singular form of the word.
    """
    return pluralsingular.singularize(word, marks=marks, **kwargs)

def _unidecode(word):
    """Remove accents from a word using the unidecode library.

    Args:
        word (str): The word to be processed.

    Returns:
        str: The processed word without accents.
    """
    import unidecode
    return unidecode.unidecode(word)

def _debug(obj, msg=None):
    """Log a debug message with the object's string representation.

    Args:
        obj: The object to be logged.
        msg (str, optional): An optional message to accompany the log.

    Returns:
        The original object.
    """
    if msg is None:
        logging.debug(str(obj))
    else:
        logging.debug(f"{msg}: {obj}")
    return obj

def _cfg_tree(key):
    """Convert a configuration key to a tree structure.

    Args:
        key: The configuration key to be converted.

    Returns:
        The configuration in a tree structure.
    """
    return cfg.to_tree(key)

def _cfg_list(key):
    """Convert a configuration key to a list structure.

    Args:
        key: The configuration key to be converted.

    Returns:
        list: The configuration as a list.

    Raises:
        AssertionError: If the configuration is not a list.
    """
    tree = cfg.to_tree(key)
    assert isinstance(tree, list)
    return tree

def render_template(src, global_vars):
    """Render a Jinja2 template with the given source and global variables.

    Args:
        src (str): The source template string.
        global_vars (dict): A dictionary of global variables to be passed to the template.

    Returns:
        str: The rendered template output.
    """
    env = jinja2.Environment()
    env.filters['cols'] = _join_columns
    env.filters['esc'] = _escape
    env.filters['quote'] = _quote
    env.filters['debug'] = _debug
    env.filters['pluralize'] = _pluralize
    env.filters['singularize'] = _singularize
    env.globals['cfg_tree'] = _cfg_tree
    env.globals['cfg_list'] = _cfg_list
    env.globals['logging'] = logging
    return env.from_string(src).render(**global_vars)
