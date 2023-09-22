"""
Module for handling database drivers in the plpipes framework.

This module defines the Driver class that acts as an abstraction layer for
database interactions, allowing for the use of various backends for
database operations. It provides an interface for executing SQL commands,
managing transactions, and interacting with tables and views within a
database.

Classes:
    Driver: The base class for database drivers providing methods for
            executing SQL commands, handling transactions, and managing
            database backend interactions.
"""

import logging
from contextlib import contextmanager
from plpipes.util.typedict import dispatcher
from plpipes.util.method_decorators import optional_abstract
import plpipes.plugin
import types
import plpipes.database.driver.transaction

_backend_class_registry = plpipes.plugin.Registry("db_backend", "plpipes.database.backend.plugin")

class Driver(plpipes.plugin.Plugin):
    """
    Driver class for handling database interactions.

    Attributes:
        _default_backend_name (str): The default backend name to use.
        _backend_subkeys (list): List of backend subkeys associated with this driver.

    Methods:
        config(): Returns the configuration for the driver.
        driver_name(): Returns the name of the driver.
        begin(): Context manager for starting a transaction.
        _execute(txn, sql, parameters=None): Executes an SQL command in the transaction.
        _execute_script(txn, sql): Executes a SQL script in the transaction.
        _list_tables(txn): Lists tables in the database.
        _read_table(txn, table_name, backend, kws): Reads a table from the database.
        _drop_table(txn, table_name, only_if_exists): Drops a table from the database.
        _create_table(txn, table_name, sql_or_df, parameters, if_exists, kws): Creates a new table.
        _create_view(txn, view_name, sql, parameters, if_exists, kws): Creates a new view in the database.
        _copy_table(txn, from_table_name, to_table_name, if_exists, kws): Copies data between tables.
        _query_chunked(txn, sql, parameters, backend, kws): Executes a chunked query.
        _query_group(txn, sql, parameters, by, backend, kws): Executes a grouped query.
        load_backend(name): Loads a specific backend into the driver.
    """

    _default_backend_name = "pandas"
    _backend_subkeys = []

    @classmethod
    def _init_plugin(klass, key):
        """
        Initializes the plugin with backend registry and configuration.

        Args:
            klass: The class reference.
            key: The key of the plugin instance.
        """
        super()._init_plugin(key)
        klass._backend_registry = {}
        klass._backend_subkeys = [key, *klass._backend_subkeys]
        klass._create_table = klass._create_table.copy()

    @classmethod
    def _backend_lookup(klass, name):
        """
        Looks up and returns the specified backend by name.

        Args:
            klass: The class reference.
            name: The name of the backend to look up.

        Returns:
            backend: The backend instance associated with the specified name.
        """
        try:
            return klass._backend_registry[name]
        except KeyError:
            backend_class = _backend_class_registry.lookup(name, subkeys=klass._backend_subkeys)
            backend = backend_class()
            klass._backend_registry[name] = backend
            backend.register_handlers({'create_table': klass._create_table.td})
            logging.debug(f"backend {backend._plugin_name} for {klass._plugin_name} loaded")
            return backend

    def __init__(self, name, drv_cfg):
        """
        Initializes the Driver instance with a name and configuration.

        Args:
            name: The name of the database driver.
            drv_cfg: The configuration settings for the driver instance.
        """
        self._name = name
        self._cfg = drv_cfg
        self._last_key = 0
        self._default_backend = self._backend_lookup(self._cfg.get("backend", self._default_backend_name))
        for backend_name in self._cfg.get('extra_backends', []):
            self._backend_lookup(backend_name)

    def config(self):
        """
        Returns the configuration settings for the driver.

        Returns:
            Tree: A tree representation of the configuration.
        """
        return self._cfg.to_tree()

    def _backend(self, name):
        """
        Retrieves the requested backend or the default backend if none is specified.

        Args:
            name: The name of the backend to retrieve.

        Returns:
            backend: The specified backend instance or default backend if name is None.
        """
        if name is None:
            return self._default_backend
        logging.debug(f"looking up backend {name}")
        return self._backend_lookup(name)

    def driver_name(self):
        """
        Returns the name of the database driver.

        Returns:
            str: The name of the driver.
        """
        return self._plugin_name

    @optional_abstract
    @contextmanager
    def begin(self):
        """
        Context manager for beginning a database transaction.

        Yields:
            Transaction: A transaction object for conducting operations within a context.
        """
        ...

    @optional_abstract
    def _execute(self, txn, sql, parameters=None):
        """
        Executes an SQL command within a transaction.

        Args:
            txn: The transaction instance to execute the command within.
            sql: The SQL command to execute.
            parameters: Optional parameters for the SQL command.
        """
        ...

    @optional_abstract
    def _execute_script(self, txn, sql):
        """
        Executes a SQL script within a transaction.

        Args:
            txn: The transaction instance to execute the script within.
            sql: The SQL script to execute.
        """
        ...

    @optional_abstract
    def _list_tables(self, txn):
        """
        Lists all tables within the database for the given transaction.

        Args:
            txn: The transaction instance for querying the database.
        """
        ...

    def _next_key(self):
        """
        Generates the next unique key for database operations.

        Returns:
            int: A unique key for the current operation.
        """
        self._last_key += 1
        return self._last_key

    def _query(self, txn, sql, parameters, backend, kws):
        """
        Executes a database query and returns the result.

        Args:
            txn: The transaction instance to use for the query.
            sql: The SQL query string to execute.
            parameters: Optional parameters for the SQL query.
            backend: The backend to use for executing the query.
            kws: Additional keyword arguments for the backend.

        Returns:
            Result: The result of the query execution.
        """
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query(txn, sql, parameters, kws)

    def _query_first(self, txn, sql, parameters, backend, kws):
        """
        Executes a query and returns the first result.

        Args:
            txn: The transaction instance.
            sql: The SQL query string to execute.
            parameters: Optional parameters for the SQL query.
            backend: The backend to use for executing the query.
            kws: Additional keyword arguments.

        Returns:
            Result: The first result of the query execution.
        """
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query_first(txn, sql, parameters, kws)

    def _query_first_value(self, txn, sql, parameters, backend, kws):
        """
        Executes a query and returns the first value from the result.

        Args:
            txn: The transaction instance.
            sql: The SQL query string to execute.
            parameters: Optional parameters for the SQL query.
            backend: The backend to use for executing the query.
            kws: Additional keyword arguments.

        Returns:
            Any: The first value from the result of the query execution.
        """
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query_first_value(txn, sql, parameters, kws)

    @optional_abstract
    def _read_table(self, txn, table_name, backend, kws):
        """
        Reads a table from the database.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to read.
            backend: The backend to use for reading the table.
            kws: Additional keyword arguments.

        Returns:
            DataFrame: The data read from the table.
        """
        ...

    @optional_abstract
    def _drop_table(self, txn, table_name, only_if_exists):
        """
        Drops a specified table from the database.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to be dropped.
            only_if_exists: Boolean to specify if the table should only be dropped if it exists.
        """
        ...

    @dispatcher({str: '_create_table_from_str',
                 list: '_create_table_from_records',
                 types.GeneratorType: '_create_table_from_iterator',},
                ix=2)
    def _create_table(self, txn, table_name, sql_or_df, parameters, if_exists, kws):
        """
        Creates a table in the database from various input types.

        Args:
            txn: The transaction instance to create the table within.
            table_name: The name of the table to create.
            sql_or_df: The SQL command or DataFrame to define the table.
            parameters: Optional parameters for creating the table.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        ...

    @optional_abstract
    def _create_table_from_str(self, txn, table_name, sql, parameters, if_exists, kws):
        """
        Creates a table from a SQL string command.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to create.
            sql: SQL string defining the table schema.
            parameters: Optional parameters for the SQL command.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        ...

    @optional_abstract
    def _create_table_from_clause(self, txn, table_name, clause, parameters, if_exists, kws):
        """
        Creates a table from a SQL clause.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to create.
            clause: SQL clause for creating the table.
            parameters: Optional parameters for the SQL command.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        ...

    def _create_table_from_records(self, txn, table_name, records, parameters, if_exists, kws):
        """
        Creates a table from a list of records.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to create.
            records: Iterable containing records for the table.
            parameters: Optional parameters for creating the table.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        backend = self._backend(kws.pop("backend", None))
        backend.create_table_from_records(txn, table_name, records, parameters, if_exists, kws)

    def _create_table_from_iterator(self, txn, table_name, iterator, parameters, if_exists, kws):
        """
        Creates a table from an iterator yielding records.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to create.
            iterator: Iterator yielding records for the table.
            parameters: Optional parameters for creating the table.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        for chunk in iterator:
            self._create_table(txn, table_name, chunk, parameters, if_exists, kws)
            if_exists = 'append'

    @optional_abstract
    def _create_view(self, txn, view_name, sql, parameters, if_exists, kws):
        """
        Creates a view in the database.

        Args:
            txn: The transaction instance.
            view_name: The name of the view to create.
            sql: SQL string defining the view.
            parameters: Optional parameters for creating the view.
            if_exists: Specifies how to handle the view if it already exists.
            kws: Additional keyword arguments.
        """
        ...

    @optional_abstract
    def _copy_table(self, txn, from_table_name, to_table_name, if_exists, kws):
        """
        Copies the contents of one table to another.

        Args:
            txn: The transaction instance.
            from_table_name: The name of the table to copy from.
            to_table_name: The name of the table to copy to.
            if_exists: Specifies how to handle the table if it already exists.
            kws: Additional keyword arguments.
        """
        ...

    @optional_abstract
    def _read_table_chunked(self, txn, table_name, backend, kws):
        """
        Reads a table in chunks.

        Args:
            txn: The transaction instance.
            table_name: The name of the table to read.
            backend: The backend to use for reading the table.
            kws: Additional keyword arguments.
        """
        ...

    def _query_chunked(self, txn, sql, parameters, backend, kws):
        """
        Executes a chunked query and returns results.

        Args:
            txn: The transaction instance.
            sql: The SQL query string to execute.
            parameters: Optional parameters for the SQL query.
            backend: The backend to use for executing the query.
            kws: Additional keyword arguments.

        Returns:
            Chunked results of the query execution.
        """
        return self._backend(backend).query_chunked(txn, sql, parameters, kws)

    def _query_group(self, txn, sql, parameters, by, backend, kws):
        """
        Executes a grouped query and returns results.

        Args:
            txn: The transaction instance.
            sql: The SQL query string to execute.
            parameters: Optional parameters for the SQL query.
            by: Column(s) to group the results by.
            backend: The backend to use for executing the query.
            kws: Additional keyword arguments.

        Returns:
            Grouped results of the query execution.
        """
        return self._backend(backend).query_group(txn, sql, parameters, by, kws)

    def load_backend(self, name):
        """
        Loads a specific backend into the driver.

        Args:
            name: The name of the backend to load.
        """
        self._backend_lookup(name)
