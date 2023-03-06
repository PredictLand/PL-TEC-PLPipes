from plpipes.config import cfg
import logging
import plpipes.plugin
import pandas
import sqlalchemy.sql

_driver_registry = plpipes.plugin.Registry("db_driver", "plpipes.database.driver.plugin")

_db_registry = {}

_create_table_methods = {}
_create_table_methods_seed = {
    pandas.DataFrame: '_create_table_from_pandas',
    sqlalchemy.sql.elements.ClauseElement: '_create_table_from_sqlalchemy_clause',
    str: '_create_table_from_sql'
}

def lookup(name=None):
    if name is None:
        name = "work"
    if name not in _db_registry:
        _db_registry[name] = _init_driver(name)
    return _db_registry[name]

def _init_driver(name):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    driver_name = drv_cfg.get("driver", "sqlite")
    logging.debug(f"Initializing database instance {name} using driver {driver_name}")
    driver = _driver_registry.lookup(driver_name)
    return driver(name, drv_cfg)

def query(sql, parameters=None, db=None):
    return lookup(db).query(sql, parameters)

def execute(sql, parameters=None, db=None):
    lookup(db).execute(sql, parameters)

def create_table(table_name, sql_or_df, parameters=None, db=None, if_exists="replace"):
    return lookup(db).create_table(table_name, sql_or_df, parameters, if_exists)

def create_view(view_name, sql, parameters=None, db=None, if_exists="replace"):
    lookup(db).create_view(view_name, sql, parameters, if_exists)

def read_table(table_name, db=None):
    return lookup(db).read_table(table_name)

def execute_script(sql_script, db=None):
    lookup(db).execute_script(sql_script)

def download_table(from_table_name, to_table_name=None, from_db="input", to_db="work"):
    # TODO: use an iterative approach, without dumping everything into
    # a pandas dataframe.
    if to_table_name is None:
        to_table_name = from_table_name
    df = read_table(from_table_name, db=from_db)
    create_table(to_table_name, df, db=to_db)

def create_table_from_query_group_and_map(name,
                                          sql,
                                          by,
                                          function,
                                          parameters,
                                          out,
                                          db=None,
                                          if_exists="replace"):
    if isinstance(by, str):
        by = [by]
    if isinstance(parameters, str):
        parameters = [parameters]
    if isinstance(out, str):
        out = [out]
    lookup(db).create_table_from_query_group_and_map(name, sql, by,
                                                     function, parameters, out,
                                                     if_exists)

def query_group_and_map(sql,
                        by,
                        function,
                        parameters,
                        db=None,
                        if_exists="replace"):
    if isinstance(by, str):
        by = [by]
    if isinstance(parameters, str):
        parameters = [parameters]
    lookup(db).query_group_and_map(sql, by,
                                   function, parameters,
                                   if_exists)

def engine(db=None):
    return lookup(db).engine()

def connection(db=None):
    return lookup(db).connection()

def driver(db=None):
    return lookup(db)
