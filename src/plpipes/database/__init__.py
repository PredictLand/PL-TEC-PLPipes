from plpipes.config import cfg
import logging
import plpipes.plugin

_driver_registry = plpipes.plugin.Registry("db_driver", "plpipes.database.driver.plugin")

_db_registry = {}

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

def query(sql, parameters=None, db=None, backend=None, **kws):
    return lookup(db).query(sql, parameters, backend, kws)

def execute(sql, parameters=None, db=None):
    return lookup(db).execute(sql, parameters)

def create_table(table_name, sql_or_df, parameters=None, db=None, if_exists="replace", **kws):
    logging.debug(f"create table {table_name}")
    return lookup(db).create_table(table_name, sql_or_df, parameters, if_exists, kws)

def create_view(view_name, sql, parameters=None, db=None, if_exists="replace", **kws):
    return lookup(db).create_view(view_name, sql, parameters, if_exists, kws)

def read_table(table_name, db=None, backend=None, **kws):
    return lookup(db).read_table(table_name, backend, kws)

def execute_script(sql_script, db=None):
    return lookup(db).execute_script(sql_script)

def query_chunked(sql, parameters=None, db=None, backend=None, **kws):
    return lookup(db).query_chunked(sql, parameters, backend, kws)

def query_group(sql, parameters=None, db=None, by=None, backend=None, **kws):
    return lookup(db).query_group(sql, parameters, by, kws)

def create_table_from_query_and_map(to_table_name, sql, parameters=None, function=None,
                                    from_db=None, to_db=None, db=None,
                                    if_exists="replace"):
    if from_db is None:
        from_db = db
    if to_db is None:
        to_db = db

    gen = query_chunked(sql, parameters=parameters, db=from_db)
    if function is None:
        gen1 = gen
    else:
        def gen1():
            for df in gen:
                r = function(df)
                if not df.empty:
                    yield r
    create_table(to_table_name, gen1, if_exists=if_exists, db=to_db)

def download_table(from_table_name, to_table_name=None,
                   from_db="input", to_db="work",
                   if_exists="replace"):
    if to_table_name is None:
        to_table_name = from_table_name

    create_table_from_query_and_map(to_table_name,
                                    f"select * from {from_table_name}",
                                    from_db=from_db, to_db=to_db,
                                    if_exists=if_exists)

    # gen = query_chunked(f"select * from {from_table_name}", db=from_db, chunksize=chunksize)
    # create_table_from_query_and_map(to_table_name, gen, if_exists=if_exists, db=to_db)

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

def load_backend(name, db=None):
    from .driver import backend_lookup
    backend_lookup(name)

load_backend("pandas")
