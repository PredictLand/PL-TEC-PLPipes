import plpipes
from plpipes import cfg
import pathlib

_driver_class = {}
_registry = {}

def lookup(name=None):
    if name is None:
        name = "work"
    if name not in _registry:
        _registry[name] = _init_driver(name)
    return _registry[name]

def _init_driver(name):
    drv_cfg = cfg.db.instance[name]
    return _driver_class[drv_cfg.get("driver", "sqlite")](name, drv_cfg)


class _Driver:
    def __init__(self, name, drv_cfg, conn=None):
        self._name = name
        self._cfg = drv_cfg
        self._conn = conn

    def query(self, sql, params=None):
        import pandas
        return pandas.read_sql_query(sql, self._conn, params=params)

    def execute(self, sql, params=None):
        self._conn.execute(sql, params)

class _SQLiteDriver(_Driver):

    def __init__(self, name, drv_cfg):

        # if there is an entry for the given name in cfg.fs we use
        # that, otherwise we store the db file in the work directory:
        root_dir = pathlib.Path(cfg.fs.get(name, "work"))
        fn = root_dir.joinpath(drv_cfg.get("file", f"{name}.sqlite")).absolute()
        fn.parent.mkdir(exist_ok=True, parents=True)
        import sqlite3
        conn = sqlite3.connect(fn)
        super().__init__(name, drv_cfg, conn=conn)

class _ODBCDriver(_Driver):

    def __init__(self, name, drv_cfg):
        import pyodbc

        connection_string = f"driver={drv_cfg.driver};Server={drv_cfg.server};Database={drv_cfg.database};UID={drv_cfg.user};PWD={drv_cfg.pwd}"
        conn = pyodbc.connect(connection_string)
        super().__init__(name, drv_cfg, conn=conn)

# Register drivers
_driver_class["sqlite"] = _SQLiteDriver
_driver_class["odbc"] = _ODBCDriver

def query(sql, *params, db=None):
    return lookup(db).query(sql, params)

def execute(sql, *params, db=None):
    lookup(db).execute(sql, params)
