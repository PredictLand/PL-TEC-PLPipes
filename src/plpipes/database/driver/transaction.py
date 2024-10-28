

class Transaction:
    """
    The Transaction class represents a database transaction.

    :param driver: The database driver object.
    :type driver: object
    :param conn: The database connection object.
    :type conn: object
    """

    def __init__(self, driver, conn):
        """
        Creates a new transaction object.

        :param driver: The database driver object.
        :param conn: The database connection object.

        Note that Transaction objects should not be created calling the class
        constructor directly but through Driver `begin` method.

        """
        self._driver = driver
        self._conn = conn

    def driver(self):
        """
        Returns the database driver object associated with this transaction.
        """
        return self._driver

    def db_name(self):
        return self._driver._name
    
    def connection(self):
        """
        Returns the database connection object associated with this transaction.
        """
        return self._conn

    def execute(self, sql, parameters=None):
        """
        Executes an SQL statement with optional parameters.

        :param sql: The SQL statement to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        """
        self._driver._execute(self, sql, parameters)

    def execute_script(self, sql_script):
        """
        Executes a script containing multiple SQL statements.

        :param sql_script: The SQL script to execute.
        """
        return self._driver._execute_script(self, sql_script)

    def create_table(self, table_name, sql_or_df, parameters=None, if_exists="replace", **kws):
        """
        Creates a new table in the database.

        :param table_name: The name of the table to create.
        :param sql_or_df: The SQL statement or DataFrame defining the table schema.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param if_exists: How to handle the table if it already exists. Valid options are "fail", "replace", and "append".
        :param **kws: Additional keyword arguments to pass to the driver.
        """
        return self._driver._create_table(self, table_name, sql_or_df,
                                          parameters, if_exists, kws)

    def create_view(self, view_name, sql, parameters=None, if_exists="replace", **kws):
        """
        Creates a new view in the database.

        :param view_name: The name of the view to create.
        :param sql: The SQL statement defining the view.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param if_exists: How to handle the view if it already exists. Valid options are "fail", "replace", and "append".
        :param **kws: Additional keyword arguments to pass to the driver.
        """
        return self._driver._create_view(self, view_name, sql, parameters, if_exists, kws)

    def read_table(self, table_name, backend=None, **kws):
        """
        Reads a table from the database into a DataFrame.

        :param table_name: The name of the table to read.
        :param backend: The backend to use for reading the table. If None, the default backend for the driver is used.
        :param **kws: Additional keyword arguments to pass to the backend.

        :returns: A DataFrame containing the table data.
        """
        return self._driver._read_table(self, table_name, backend, kws)

    def read_table_chunked(self, table_name, backend=None, **kws):
        """
        Creates a new view in the database.

        :param view_name: The name of the view to create.
        :type view_name: str
        :param sql: The SQL statement defining the view.
        :type sql: str
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :type parameters: dict, optional
        :param if_exists: How to handle the view if it already exists. Valid options are "fail", "replace", and "append".
        :type if_exists: str, optional
        :param **kws: Additional keyword arguments to pass to the driver.
        """
        return self._driver._read_table_chunked(self, table_name, backend, kws)

    def query(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as a DataFrame.

        :param sql: The SQL query to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param backend: The backend to use for executing the query. If None, the default backend is used.
        :param **kws: Additional keyword arguments to pass to the driver.
        :return: A DataFrame containing the query result.
        """
        return self._driver._query(self, sql, parameters, backend, kws)

    def query_first(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as a DataFrame.

        :param sql: The SQL query to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param backend: The backend to use for executing the query. If None, the default backend is used.
        :param **kws: Additional keyword arguments to pass to the driver.
        :return: A dataframe/dictionary containing the result first row.
        """
        return self._driver._query_first(self, sql, parameters, backend, kws)

    def query_first_value(self, sql, parameters=None, backend="tuple", **kws):
        """
        Executes an SQL query and returns the result as a DataFrame.

        :param sql: The SQL query to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param backend: The backend to use for executing the query. If None, the default backend is used. Defaults to `tuple`.
        :param **kws: Additional keyword arguments to pass to the driver.
        :return: The first value from the result (first row, first column).
        """
        return self._driver._query_first_value(self, sql, parameters, backend, kws)


    def query_chunked(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as an iterator over chunks of rows.

        :param sql: The SQL query to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param backend: The backend to use for executing the query. If None, the default backend is used.
        :param **kws: Additional keyword arguments to pass to the driver.
        :return: An iterator over chunks of rows.
        """
        return self._driver._query_chunked(self, sql, parameters, backend, kws)

    def query_group(self, sql, parameters=None, by=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as a DataFrame grouped by one or more columns.

        :param sql: The SQL query to execute.
        :param parameters: A dictionary containing values to fill in SQL statement placeholders.
        :param by: The column(s) to group by.
        :param backend: The backend to use for executing the query. If None, the default backend is used.
        :param **kws: Additional keyword arguments to pass to the driver.
        :return: A DataFrame containing the grouped query result.
        """
        return self._driver._query_group(self, sql, parameters, by, backend, kws)

    def drop_table(self, table_name, only_if_exists=True):
        """
        Drops a table from the database.

        :param table_name: The name of the table to drop.
        :param only_if_exists: If True, the table is only dropped if it exists. Otherwise, an error is raised if the table does not exist.
        """
        return self._driver._drop_table(self, table_name, only_if_exists)

    def list_tables(self):
        """
        Lists the tables in the database.

        :return: Dataframe with the list of tables.
        """
        return self._driver._list_tables(self)

    def list_views(self):
        """
        Lists the views in the database.

        :return: Dataframe with the list of views.
        """
        return self._driver._list_views(self)

    def table_exists_p(self, table_name):
        """
        Checks whether a table exists in the database.

        :param table_name: The name of the table to check.
        :return: True if the table exists, False otherwise.
        """
        return self._driver._table_exists_p(self, table_name)

    def copy_table(self, from_table_name, to_table_name, if_exists="replace", **kws):
        """
        Copies the contents of one table to another.

        :param from_table_name: The name of the table to copy from.
        :param to_table_name: The name of the table to copy to.
        :param if_exists: How to handle the destination table if it already exists. Valid options are "fail", "replace", and "append".
        :param **kws: Additional keyword arguments to pass to the driver.

        :raises ValueError: If the source and destination table names are the same.

        :returns: The number of rows copied.
        """
        if from_table_name == to_table_name:
            raise ValueError("source and destination tables must be different")
        return self._driver._copy_table(self, from_table_name, to_table_name, if_exists, kws)
