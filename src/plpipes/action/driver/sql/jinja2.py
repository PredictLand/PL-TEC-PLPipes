import jinja2
import re
import logging
import importlib

from plpipes.config import cfg

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

def _escape(arg,  pre=None, post=None, esc_char='"'):
    """Escape a identifier name if necessary by adding double quotes and handling internal double quotes.

    Args:
        arg (str): The identifier name to be escaped.

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

def _pluralize(word, lang='en', marks=False):
    if isinstance(word, list):
        return [_pluralize(w, lang) for w in word]
    import pluralsingular
    p = pluralsingular.pluralize(word, lang=lang)
    if not marks:
        p = _unidecode(p)
    return p

def _singularize(word, lang='en', marks=False):
    if isinstance(word, list):
        return [_singularize(w, lang) for w in word]
    import pluralsingular
    s = pluralsingular.singularize(word, lang=lang)
    if not marks:
        s = _unidecode(s)
    return s

def _unidecode(word):
    import unidecode
    return unidecode.unidecode(word)

def _debug(obj, msg=None):
    if msg is None:
        logging.debug(str(obj))
    else:
        logging.debug(f"{msg}: {obj}")
    return obj

def _cfg_tree(key):
    return cfg.to_tree(key)

def _cfg_list(key):
    tree = cfg.to_tree(key)
    assert isinstance(tree, list)
    return tree

def render_template(src, global_vars):
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
