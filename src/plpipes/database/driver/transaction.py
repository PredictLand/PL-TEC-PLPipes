class Transaction:
    """
    The Transaction class represents a database transaction.

    Attributes:
        driver (object): The database driver object.
        conn (object): The database connection object.
    """

    def __init__(self, driver, conn):
        """
        Creates a new transaction object.

        Args:
            driver (object): The database driver object.
            conn (object): The database connection object.

        Note:
            Transaction objects should not be created calling the class
            constructor directly but through Driver `begin` method.
        """
        self._driver = driver
        self._conn = conn

    def driver(self):
        """
        Returns the database driver object associated with this transaction.

        Returns:
            object: The database driver object.
        """
        return self._driver

    def db_name(self):
        """Returns the name of the database."""
        return self._driver._name

    def connection(self):
        """
        Returns the database connection object associated with this transaction.

        Returns:
            object: The database connection object.
        """
        return self._conn

    def execute(self, sql, parameters=None):
        """
        Executes an SQL statement with optional parameters.

        Args:
            sql (str): The SQL statement to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
        """
        self._driver._execute(self, sql, parameters)

    def execute_script(self, sql_script):
        """
        Executes a script containing multiple SQL statements.

        Args:
            sql_script (str): The SQL script to execute.
        """
        return self._driver._execute_script(self, sql_script)

    def create_table(self, table_name, sql_or_df, parameters=None, if_exists="replace", **kws):
        """
        Creates a new table in the database.

        Args:
            table_name (str): The name of the table to create.
            sql_or_df (str or DataFrame): The SQL statement or DataFrame defining the table schema.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            if_exists (str, optional): How to handle the table if it already exists. Valid options are "fail", "replace", and "append".
            **kws: Additional keyword arguments to pass to the driver.
        """
        return self._driver._create_table(self, table_name, sql_or_df, parameters, if_exists, kws)

    def create_view(self, view_name, sql, parameters=None, if_exists="replace", **kws):
        """
        Creates a new view in the database.

        Args:
            view_name (str): The name of the view to create.
            sql (str): The SQL statement defining the view.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            if_exists (str, optional): How to handle the view if it already exists. Valid options are "fail", "replace", and "append".
            **kws: Additional keyword arguments to pass to the driver.
        """
        return self._driver._create_view(self, view_name, sql, parameters, if_exists, kws)

    def read_table(self, table_name, backend=None, **kws):
        """
        Reads a table from the database into a DataFrame.

        Args:
            table_name (str): The name of the table to read.
            backend (optional): The backend to use for reading the table. If None, the default backend for the driver is used.
            **kws: Additional keyword arguments to pass to the backend.

        Returns:
            DataFrame: A DataFrame containing the table data.
        """
        return self._driver._read_table(self, table_name, backend, kws)

    def read_table_chunked(self, table_name, backend=None, **kws):
        """
        Reads a table from the database in chunks.

        Args:
            table_name (str): The name of the table to read.
            backend (optional): The backend to use for reading the table. If None, the default backend for the driver is used.
            **kws: Additional keyword arguments to pass to the backend.
        """
        return self._driver._read_table_chunked(self, table_name, backend, kws)

    def query(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as a DataFrame.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            backend (optional): The backend to use for executing the query. If None, the default backend is used.
            **kws: Additional keyword arguments to pass to the driver.

        Returns:
            DataFrame: A DataFrame containing the query result.
        """
        return self._driver._query(self, sql, parameters, backend, kws)

    def query_first(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the first row of the result.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            backend (optional): The backend to use for executing the query. If None, the default backend is used.
            **kws: Additional keyword arguments to pass to the driver.

        Returns:
            DataFrame or dict: A dataframe/dictionary containing the result first row.
        """
        return self._driver._query_first(self, sql, parameters, backend, kws)

    def query_first_value(self, sql, parameters=None, backend="tuple", **kws):
        """
        Executes an SQL query and returns the first value from the result.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            backend (str, optional): The backend to use for executing the query. If None, the default backend is used. Defaults to `tuple`.
            **kws: Additional keyword arguments to pass to the driver.

        Returns:
            Any: The first value from the result (first row, first column).
        """
        return self._driver._query_first_value(self, sql, parameters, backend, kws)

    def query_chunked(self, sql, parameters=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as an iterator over chunks of rows.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            backend (optional): The backend to use for executing the query. If None, the default backend is used.
            **kws: Additional keyword arguments to pass to the driver.

        Returns:
            iterator: An iterator over chunks of rows.
        """
        return self._driver._query_chunked(self, sql, parameters, backend, kws)

    def query_group(self, sql, parameters=None, by=None, backend=None, **kws):
        """
        Executes an SQL query and returns the result as a DataFrame grouped by one or more columns.

        Args:
            sql (str): The SQL query to execute.
            parameters (dict, optional): A dictionary containing values to fill in SQL statement placeholders.
            by (optional): The column(s) to group by.
            backend (optional): The backend to use for executing the query. If None, the default backend is used.
            **kws: Additional keyword arguments to pass to the driver.

        Returns:
            DataFrame: A DataFrame containing the grouped query result.
        """
        return self._driver._query_group(self, sql, parameters, by, backend, kws)

    def drop_table(self, table_name, only_if_exists=True):
        """
        Drops a table from the database.

        Args:
            table_name (str): The name of the table to drop.
            only_if_exists (bool, optional): If True, the table is only dropped if it exists. Otherwise, an error is raised if the table does not exist.
        """
        return self._driver._drop_table(self, table_name, only_if_exists)

    def list_tables(self):
        """
        Lists the tables in the database.

        Returns:
            DataFrame: DataFrame with the list of tables.
        """
        return self._driver._list_tables(self)

    def list_views(self):
        """
        Lists the views in the database.

        Returns:
            DataFrame: DataFrame with the list of views.
        """
        return self._driver._list_views(self)

    def table_exists_p(self, table_name):
        """
        Checks whether a table exists in the database.

        Args:
            table_name (str): The name of the table to check.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        return self._driver._table_exists_p(self, table_name)

    def copy_table(self, from_table_name, to_table_name, if_exists="replace", **kws):
        """
        Copies the contents of one table to another.

        Args:
            from_table_name (str): The name of the table to copy from.
            to_table_name (str): The name of the table to copy to.
            if_exists (str, optional): How to handle the destination table if it already exists. Valid options are "fail", "replace", and "append".
            **kws: Additional keyword arguments to pass to the driver.

        Raises:
            ValueError: If the source and destination table names are the same.

        Returns:
            int: The number of rows copied.
        """
        if from_table_name == to_table_name:
            raise ValueError("source and destination tables must be different")
        return self._driver._copy_table(self, from_table_name, to_table_name, if_exists, kws)
