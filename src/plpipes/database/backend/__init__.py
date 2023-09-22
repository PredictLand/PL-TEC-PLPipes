"""
Module for defining the database backend interface in the plpipes framework.

This module provides the Backend class, which serves as an abstraction layer
for different database backends. It allows interaction with various data
representations such as pandas, geopandas, Spark DataFrames, polars, and more.

Classes:
    Backend: The base class for database backends providing methods for
             executing queries, handling data chunking, grouping results,
             and retrieving first results.
"""

from plpipes.plugin import Plugin

class Backend(Plugin):
    """
    Abstract base class for database backends.

    This class defines the interface for various database backends that can
    perform queries and manage data representations.

    Methods:
        query(engine, sql, parameters, kws): Executes a query and returns the result.
        query_chunked(engine, sql, parameter, kws): Executes a chunked query and returns results.
        query_group(engine, sql, parameters, by, kws): Executes a grouped query and returns results.
        query_first(engine, sql, parameters, kws): Executes a query and returns the first result.
        query_first_value(engine, sql, parameters, kws): Executes a query and returns the first value.
    """

    def query(self, engine, sql, parameters, kws):
        """
        Executes a query against the database.

        Args:
            engine (object): The database engine to use for executing the query.
            sql (str): The SQL query to execute.
            parameters (dict): Optional parameters for the SQL query.
            kws (dict): Additional keyword arguments for the execution.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("This function is not yet implemented.")

    def query_chunked(self, engine, sql, parameters, kws):
        """
        Executes a chunked query against the database.

        Args:
            engine (object): The database engine to use for executing the query.
            sql (str): The SQL query to execute.
            parameters (dict): Optional parameters for the SQL query.
            kws (dict): Additional keyword arguments for the execution.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("This function is not yet implemented.")

    def query_group(self, engine, sql, parameters, by, kws):
        """
        Executes a grouped query against the database.

        Args:
            engine (object): The database engine to use for executing the query.
            sql (str): The SQL query to execute.
            parameters (dict): Optional parameters for the SQL query.
            by (str): The column(s) to group the results by.
            kws (dict): Additional keyword arguments for the execution.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("This function is not yet implemented.")

    def query_first(self, engine, sql, parameters, kws):
        """
        Executes a query and retrieves the first result.

        Args:
            engine (object): The database engine to use for executing the query.
            sql (str): The SQL query to execute.
            parameters (dict): Optional parameters for the SQL query.
            kws (dict): Additional keyword arguments for the execution.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("This function is not yet implemented.")

    def query_first_value(self, engine, sql, parameters, kws):
        """
        Executes a query and retrieves the first value from the result.

        Args:
            engine (object): The database engine to use for executing the query.
            sql (str): The SQL query to execute.
            parameters (dict): Optional parameters for the SQL query.
            kws (dict): Additional keyword arguments for the execution.

        Raises:
            NotImplementedError: This method must be implemented in subclasses.
        """
        raise NotImplementedError("This function is not yet implemented.")
