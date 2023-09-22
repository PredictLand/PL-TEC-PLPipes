"""
This module provides SQLAlchemy extensions for performing tasks not natively supported by the SQLAlchemy ORM.
It includes constructs for creating and dropping tables and views, inserting data from queries,
and handling subqueries.
"""

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import ClauseElement, Executable, FromClause
import sqlalchemy.sql
import logging

class _CreateSomethingAs(Executable, ClauseElement):
    """
    Base class for creating a table or view from a select statement.
    """
    def __init__(self, table_or_view, table_name, select, if_not_exists=False):
        """
        Initializes the creation of a table or view.

        :param table_or_view: Type of object being created ('TABLE' or 'VIEW').
        :param table_name: Name of the table or view to be created.
        :param select: Select statement to create the object from.
        :param if_not_exists: Whether to create the object only if it does not exist.
        """
        self._table_name = table_name
        self._select = select
        self._if_not_exists = if_not_exists
        self._table_or_view = table_or_view

class CreateTableAs(_CreateSomethingAs):
    """
    Class for creating a table from a select statement.
    """
    inherit_cache = True

    def __init__(self, *args, **kwargs):
        """
        Initializes the creation of a table.
        """
        super().__init__("TABLE", *args, **kwargs)

class CreateViewAs(_CreateSomethingAs):
    """
    Class for creating a view from a select statement.
    """
    inherit_cache = True

    def __init__(self, *args, **kwargs):
        """
        Initializes the creation of a view.
        """
        super().__init__("VIEW", *args, **kwargs)

@compiles(_CreateSomethingAs)
def _create_something_as(element, compiler, **kw):
    """
    Compiles the creation of a table or view into a SQL statement.

    :param element: The element being compiled.
    :param compiler: The SQL compiler.
    :param kw: Additional options for compilation.
    :return: A SQL statement for creating the specified object.
    """
    table_name = compiler.preparer.quote_identifier(element._table_name)
    select = compiler.process(element._select, **kw)
    if element._if_not_exists:
        sql = f"CREATE {element._table_or_view} IF NOT EXISTS {table_name} AS {select}"
    else:
        sql = f"CREATE {element._table_or_view} {table_name} AS {select}"
    logging.debug(f"SQL code: {sql}")
    return sql

class InsertIntoTableFromQuery(Executable, ClauseElement):
    """
    Class for inserting data into a table from a select statement.
    """
    inherit_cache = True

    def __init__(self, table_name, select):
        """
        Initializes the insert operation.

        :param table_name: Name of the table to insert into.
        :param select: Select statement providing data to insert.
        """
        self._table_name = table_name
        self._select = select

@compiles(InsertIntoTableFromQuery)
def _insert_into_table_from_query(element, compiler, **kw):
    """
    Compiles the insert operation into a SQL statement.

    :param element: The element being compiled.
    :param compiler: The SQL compiler.
    :param kw: Additional options for compilation.
    :return: A SQL statement for inserting data into the specified table.
    """
    table_name = compiler.preparer.quote_identifier(element._table_name)
    select = compiler.process(element._select, **kw)
    sql = f"INSERT INTO {table_name} {select}"
    logging.debug(f"SQL code: {sql}")
    return sql

class _DropSomething(Executable, ClauseElement):
    """
    Base class for dropping a table or view.
    """
    def __init__(self, table_or_view, table_name, if_exists=False):
        """
        Initializes the drop operation.

        :param table_or_view: Type of object being dropped ('TABLE' or 'VIEW').
        :param table_name: Name of the table or view to be dropped.
        :param if_exists: Whether to drop the object only if it exists.
        """
        self._table_name = table_name
        self._if_exists = if_exists
        self._table_or_view = table_or_view

class DropTable(_DropSomething):
    """
    Class for dropping a table.
    """
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        """
        Initializes the drop operation for a table.
        """
        super().__init__("TABLE", *args, **kwargs)

class DropView(_DropSomething):
    """
    Class for dropping a view.
    """
    inherit_cache = False

    def __init__(self, *args, **kwargs):
        """
        Initializes the drop operation for a view.
        """
        super().__init__("VIEW", *args, **kwargs)

@compiles(_DropSomething)
def _drop_something(element, compiler, **kwargs):
    """
    Compiles the drop operation into a SQL statement.

    :param element: The element being compiled.
    :param compiler: The SQL compiler.
    :param kwargs: Additional options for compilation.
    :return: A SQL statement for dropping the specified object.
    """
    if element._if_exists:
        sql = f"DROP {element._table_or_view} IF EXISTS {element._table_name}"
    else:
        sql = f"DROP {element._table_or_view} {element._table_name}"
    logging.debug(f"SQL code: {sql}")
    return sql

class AsSubquery(FromClause):
    """
    Class for handling subqueries.
    """
    inherit_cache = False

    def __init__(self, txt):
        """
        Initializes a subquery.

        :param txt: The text of the subquery.
        """
        self._txt = txt

@compiles(AsSubquery)
def _as_subquery(element, compiler, **kwargs):
    """
    Compiles the subquery into a SQL statement.

    :param element: The element being compiled.
    :param compiler: The SQL compiler.
    :param kwargs: Additional options for compilation.
    :return: A SQL representation of the subquery.
    """
    txt = compiler.process(element._txt)
    return f"({txt})"

def Wrap(str_or_something):
    """
    Wraps a string or other expression into a SQLAlchemy text object.

    :param str_or_something: The string or expression to wrap.
    :return: A SQLAlchemy text object or the original expression.
    """
    if isinstance(str_or_something, str):
        return sqlalchemy.sql.text(str_or_something)
    return str_or_something
