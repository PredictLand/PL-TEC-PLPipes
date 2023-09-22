from plpipes.config import cfg
import logging
import plpipes.plugin
import plpipes.database.driver
import plpipes.database.driver.transaction

_driver_registry = plpipes.plugin.Registry("db_driver", "plpipes.database.driver.plugin")
_db_registry = {}

def lookup(db=None):
    """
    Lookup the database driver instance for the specified database.

    Args:
        db (str, optional): The name of the database instance to look up. Defaults to "work".

    Returns:
        driver: The database driver instance.
    """
    if db is None:
        db = "work"
    if db not in _db_registry:
        _db_registry[db] = _init_driver(db)
    return _db_registry[db]

def _init_driver(name):
    """
    Initialize the database driver for the specified instance name.

    Args:
        name (str): The name of the database instance to initialize.

    Returns:
        driver: The initialized database driver.
    """
    drv_cfg = cfg.cd(f"db.instance.{name}")
    driver_name = drv_cfg.setdefault("driver", "sqlite")
    logging.debug(f"Initializing database instance {name} using driver {driver_name}")
    driver = _driver_registry.lookup(driver_name)
    return driver(name, drv_cfg)

def begin(db=None):
    """
    Begin a transaction on the specified database instance.

    Args:
        db (str, optional): The name of the database instance to begin a transaction. Defaults to "work".

    Returns:
        Transaction: A transaction object for the database.
    """
    return lookup(db).begin()

class _TxnWrapper:
    """
    A wrapper class to handle transaction context management.
    """
    def __init__(self, txn):
        self._txn = txn

    def __enter__(self):
        return self._txn

    def __exit__(self, a, b, c):
        return False

def _begin_or_pass_through(db_or_txn):
    """
    Begin a transaction or pass through to the existing transaction.

    Args:
        db_or_txn: Either a database name or an existing transaction.

    Returns:
        TransactionWrapper: A context manager for the transaction.
    """
    if isinstance(db_or_txn, plpipes.database.driver.transaction.Transaction):
        return _TxnWrapper(db_or_txn)
    else:
        return lookup(db_or_txn).begin()

def query_first(sql, parameters=None, db=None, backend=None, **kws):
    """
    Execute a SQL query and return the first result.

    Args:
        sql (str): The SQL query to execute.
        parameters (dict, optional): The parameters for the SQL query.
        db (str, optional): The database instance to use.
        backend (str, optional): The backend to use.

    Returns:
        object: The first result of the query.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.query_first(sql, parameters, backend, **kws)

def query_first_value(sql, parameters=None, db=None, backend="tuple", **kws):
    """
    Execute a SQL query and return the first value from the result.

    Args:
        sql (str): The SQL query to execute.
        parameters (dict, optional): The parameters for the SQL query.
        db (str, optional): The database instance to use.
        backend (str, optional): The backend to use. Defaults to "tuple".

    Returns:
        object: The first value from the query result.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.query_first_value(sql, parameters, backend, **kws)

def query(sql, parameters=None, db=None, backend=None, **kws):
    """
    Execute a SQL query and return the results.

    Args:
        sql (str): The SQL query to execute.
        parameters (dict, optional): The parameters for the SQL query.
        db (str, optional): The database instance to use.
        backend (str, optional): The backend to use.

    Returns:
        DataFrame: The results of the query as a DataFrame.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.query(sql, parameters, backend, **kws)

def execute(sql, parameters=None, db=None):
    """
    Execute a SQL command that does not return a result set.

    Args:
        sql (str): The SQL command to execute.
        parameters (dict, optional): The parameters for the SQL command.
        db (str, optional): The database instance to use.

    Returns:
        None
    """
    with _begin_or_pass_through(db) as txn:
        return txn.execute(sql, parameters)

def create_table(table_name, sql_or_df, parameters=None, db=None, if_exists="replace", **kws):
    """
    Create a new table in the database from a DataFrame, SQL command, or SQLAlchemy select object.

    Args:
        table_name (str): The name of the table to create.
        sql_or_df (DataFrame, str, or SQLAlchemy select object): The DataFrame, SQL command, or SQLAlchemy select object for creating the table.
        parameters (dict, optional): The parameters for creating the table.
        db (str, optional): The database instance to use.
        if_exists (str, optional): What to do if the table already exists. Defaults to "replace".
        **kws: Additional keyword arguments.

    Returns:
        None
    """
    logging.debug(f"create table {table_name}")
    with _begin_or_pass_through(db) as txn:
        return txn.create_table(table_name, sql_or_df, parameters, if_exists, **kws)

def create_view(view_name, sql, parameters=None, db=None, if_exists="replace", **kws):
    """
    Create a new view in the database.

    Args:
        view_name (str): The name of the view to create.
        sql (str): The SQL command for creating the view.
        parameters (dict, optional): The parameters for creating the view.
        db (str, optional): The database instance to use.
        if_exists (str, optional): What to do if the view already exists. Defaults to "replace".
        **kws: Additional keyword arguments.

    Returns:
        None
    """
    with _begin_or_pass_through(db) as txn:
        return txn.create_view(view_name, sql, parameters, if_exists, **kws)

def read_table(table_name, db=None, backend=None, **kws):
    """
    Read the contents of a table and return it as a data frame.

    Args:
        table_name (str): The name of the table to read.
        db (str, optional): The database instance to use.
        backend (str, optional): The backend to use.
        **kws: Additional keyword arguments.

    Returns:
        DataFrame: The contents of the table as a DataFrame.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.read_table(table_name, backend, **kws)

def execute_script(sql_script, db=None):
    """
    Execute a sequence of SQL commands.

    Args:
        sql_script (str): The SQL script to execute.
        db (str, optional): The database instance to use.

    Returns:
        None
    """
    with _begin_or_pass_through(db) as txn:
        return txn.execute_script(sql_script)

def list_tables(db=None):
    """
    List all tables in the specified database.

    Args:
        db (str, optional): The database instance to use.

    Returns:
        list: A list of table names in the database.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.list_tables()

def list_views(db=None):
    """
    List all views in the specified database.

    Args:
        db (str, optional): The database instance to use.

    Returns:
        list: A list of view names in the database.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.list_views()

def table_exists_p(table_name, db=None):
    """
    Check if a table exists in the specified database.

    Args:
        table_name (str): The name of the table to check.
        db (str, optional): The database instance to use.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    with _begin_or_pass_through(db) as txn:
        return txn.table_exists_p(table_name)

def drop_table(table_name, db=None, only_if_exists=False):
    """
    Drop a table from the database.

    Args:
        table_name (str): The name of the table to drop.
        db (str, optional): The database instance to use.
        only_if_exists (bool, optional): If True, do not raise an error if the table does not exist.

    Returns:
        None
    """
    with _begin_or_pass_through(db) as txn:
        return txn.drop_table(table_name, only_if_exists)

def query_chunked(sql, parameters=None, db=None, backend=None, **kws):
    """
    Execute a SQL query and yield results in chunks.

    Args:
        sql (str): The SQL query to execute.
        parameters (dict, optional): The parameters for the SQL query.
        db (str, optional): The database instance to use.
        backend (str, optional): The backend to use.
        **kws: Additional keyword arguments.

    Yields:
        DataFrame: Each chunk of the results as a DataFrame.
    """
    with _begin_or_pass_through(db) as txn:
        for df in txn.query_chunked(sql, parameters, backend, **kws):
            yield df

def query_group(sql, parameters=None, db=None, by=None, backend=None, **kws):
    """
    Execute a SQL query and yield results grouped by specified criteria.

    Args:
        sql (str): The SQL query to execute.
        parameters (dict, optional): The parameters for the SQL query.
        db (str, optional): The database instance to use.
        by (str, optional): The column or columns to group by.
        backend (str, optional): The backend to use.
        **kws: Additional keyword arguments.

    Yields:
        DataFrame: Each group of results as a DataFrame.
    """
    with _begin_or_pass_through(db) as txn:
        for df in txn.query_group(sql, parameters, by, backend, **kws):
            yield df

def copy_table(from_table_name, to_table_name=None,
               from_db=None, to_db=None, db=None,
               if_exists="replace", **kws):
    """
    Copy a table from one database instance to another.

    Args:
        from_table_name (str): The name of the source table to copy.
        to_table_name (str, optional): The name of the destination table. Defaults to the source table name.
        from_db (str, optional): The source database instance to copy from.
        to_db (str, optional): The destination database instance to copy to.
        db (str, optional): The current database instance.
        if_exists (str, optional): What to do if the destination table already exists. Defaults to "replace".
        **kws: Additional keyword arguments.

    Returns:
        None
    """
    if to_table_name is None:
        to_table_name = from_table_name.split(".")[-1]

    if from_db is None:
        from_db = db
    if to_db is None:
        to_db = db

    with _begin_or_pass_through(from_db) as from_txn:
        if from_db == to_db:
            logging.debug(f"copy table {from_table_name} as {to_table_name} inside db {from_txn.db_name()}")
            from_txn.copy_table(from_table_name, to_table_name, if_exists=if_exists, **kws)
        else:
            with _begin_or_pass_through(to_db) as to_txn:
                logging.debug(f"copy table {from_table_name} from db {from_txn.db_name()} as {to_table_name} in db {to_txn.db_name()}")
                if if_exists == "replace":
                    to_txn.drop_table(to_table_name)
                    if_exists="append"
                elif if_exists == "drop_rows":
                    to_txn.drop_table_rows(to_table_name)
                    if_exists="append"
                first = True
                for df in from_txn.read_table_chunked(from_table_name, **kws):
                    if first:
                        to_txn.create_table(to_table_name, df, if_exists=if_exists)
                        first = False
                    else:
                        to_txn.create_table(to_table_name, df, if_exists="append")

_key_dir_unpacked = {
    '>' : (True , True ), # Ascending, Strict
    '>=': (True , False),
    '<' : (False, True ),
    '<=': (False, False)
}

def update_table(from_table_name, to_table_name=None,
                 from_db=None, to_db=None, db=None,
                 key=None, key_dir=">=", **kws):
    """
    Update a table in the destination database from the source table.

    Args:
        from_table_name (str): The name of the source table to update from.
        to_table_name (str, optional): The name of the destination table to update. Defaults to the source table name.
        from_db (str, optional): The source database instance.
        to_db (str, optional): The destination database instance.
        db (str, optional): The current database instance.
        key (str): The key column used to identify new rows.
        key_dir (str, optional): The direction for the key comparison. Defaults to ">=".
        **kws: Additional keyword arguments.

    Returns:
        None
    """
    if to_table_name is None:
        to_table_name = from_table_name

    if from_db is None:
        from_db = db
    if to_db is None:
        to_db = db

    try:
        ascending, strict = _key_dir_unpacked[key_dir]
    except KeyError:
        raise ValueError(f"Invalid key_dir value {key_dir}")

    with _begin_or_pass_through(from_db) as from_txn:
        with _begin_or_pass_through(to_db) as to_txn:
            logging.debug(f"Updating table {from_table_name} from db {from_txn.db_name()} as {to_table_name} in db {to_txn.db_name()}")

            if to_txn.driver()._engine.dialect.has_table(to_txn._conn, to_table_name):
                count = to_txn.query_first_value(f"select count(*) from (select {key} from {to_table_name} limit 1) as t")
                if count > 0:
                    top_func = "max" if ascending else "min"
                    # FIXME: escape key identifier properly!
                    top = to_txn.query_first_value(f"select {top_func}({key}) from {to_table_name}")
                    if not strict:
                        # we don't know whether we already have all the rows with key=top, so we have to also update those!
                        to_txn.execute(f"delete from {to_table_name} where {key} = :top", parameters={'top': top})
                    for df in from_txn.query_chunked(f"select * from {from_table_name} where {key} {key_dir} :top",
                                                     parameters={'top': top}):
                        to_txn.create_table(to_table_name, df, if_exists="append")
                    return
            # No table, or table is empty

            for df in from_txn.read_table_chunked(from_table_name):
                to_txn.create_table(to_table_name, df, if_exists="append")

def engine(db=None):
    """
    Retrieve the database engine for the specified database instance.

    Args:
        db (str, optional): The name of the database instance to retrieve the engine for. Defaults to "work".

    Returns:
        engine: The database engine object.
    """
    return lookup(db).engine()

def load_backend(name, db=None):
    """
    Load a specific backend into the database driver.

    Args:
        name (str): The name of the backend to load.
        db (str, optional): The database instance to load the backend into. Defaults to "work".

    Returns:
        None
    """
    lookup(db).load_backend(name)
