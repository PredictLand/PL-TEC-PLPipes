from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable

import logging

class _CreateSomethingAs(Executable, ClauseElement):
    def __init__(self, table_or_view, table_name, select, if_not_exists=False):
        self._table_name = table_name
        self._select = select
        self._if_not_exists = if_not_exists
        self._table_or_view = table_or_view

class CreateTableAs(_CreateSomethingAs):
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        super().__init__("TABLE", *args, **kwargs)

class CreateViewAs(_CreateSomethingAs):
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        super().__init__("VIEW", *args, **kwargs)

@compiles(_CreateSomethingAs)
def _create_something_as(element, compiler, **kw):
    select = compiler.process(element._select)
    if element._if_not_exists:
        sql = f"CREATE {element._table_or_view} IF NOT EXISTS {element._table_name} AS {select}"
    else:
        sql = f"CREATE {element._table_or_view} {element._table_name} AS {select}"
    logging.debug(f"SQL code: {sql}")
    return sql

class _DropSomething(Executable, ClauseElement):
    def __init__(self, table_or_view, table_name, if_exists=False):
        self._table_name = table_name
        self._if_exists = if_exists
        self._table_or_view = table_or_view

class DropTable(_DropSomething):
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        super().__init__("TABLE", *args, **kwargs)

class DropView(_DropSomething):
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        super().__init__("VIEW", *args, **kwargs)

@compiles(_DropSomething)
def _drop_something(element, compiler, **kwargs):
    if element._if_exists:
        sql = f"DROP {element._table_or_view} IF EXISTS {element._table_name}"
    else:
        sql = f"DROP {element._table_or_view} {element._table_name}"
    logging.debug(f"SQL code: {sql}")
    return sql
