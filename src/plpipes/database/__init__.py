from plpipes.config import cfg
import logging
import plpipes.plugin

_driver_registry = plpipes.plugin.Registry("db_driver", "plpipes.database.driver.plugin")

_db_registry = {}

def lookup(db=None):
    if db is None:
        db = "work"
    if db not in _db_registry:
        _db_registry[db] = _init_driver(db)
    return _db_registry[db]

def _init_driver(name):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    driver_name = drv_cfg.get("driver", "sqlite")
    logging.debug(f"Initializing database instance {name} using driver {driver_name}")
    driver = _driver_registry.lookup(driver_name)
    return driver(name, drv_cfg)

def begin(db=None):
    return lookup(db).begin()

def query(sql, parameters=None, db=None, backend=None, **kws):
    with begin(db) as txn:
        return txn.query(sql, parameters, backend, kws)

def execute(sql, parameters=None, db=None):
    with begin(db) as txn:
        return txn.execute(sql, parameters)

def create_table(table_name, sql_or_df, parameters=None, db=None, if_exists="replace", **kws):
    logging.debug(f"create table {table_name}")
    with begin(db) as txn:
        return txn.create_table(table_name, sql_or_df, parameters, if_exists, **kws)

def create_view(view_name, sql, parameters=None, db=None, if_exists="replace", **kws):
    with begin(db) as txn:
        return txn.create_view(view_name, sql, parameters, if_exists, **kws)

def read_table(table_name, db=None, backend=None, **kws):
    with begin(db) as txn:
        return txn.read_table(table_name, backend, **kws)

def execute_script(sql_script, db=None):
    with begin(db) as txn:
        return txn.execute_script(sql_script)

def query_chunked(sql, parameters=None, db=None, backend=None, **kws):
    with begin(db) as txn:
        return txn.query_chunked(sql, parameters, backend, **kws)

def query_group(sql, parameters=None, db=None, by=None, backend=None, **kws):
    with begin(db) as txn:
        return txn.query_group(sql, parameters, by, backend, **kws)

def download_table(from_table_name, to_table_name=None,
                   from_db="input", to_db="work",
                   if_exists="replace"):
    if to_table_name is None:
        to_table_name = from_table_name

    create_table_from_query_and_map(to_table_name,
                                    f"select * from {from_table_name}",
                                    from_db=from_db, to_db=to_db,
                                    if_exists=if_exists)

def engine(db=None):
    return lookup(db).engine()

def load_backend(name, db=None):
    lookup(db).load_backend(name)
