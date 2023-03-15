import logging
import sqlalchemy

import sqlalchemy.sql as sas

from plpipes.database.sqlext import CreateTableAs, CreateViewAs, DropTable, DropView, Wrap
from plpipes.util.typedict import dispatcher

import plpipes.plugin

_backend_class_registry = plpipes.plugin.Registry("db_backend", "plpipes.database.backend.plugin")

class Driver(plpipes.plugin.Plugin):
    _default_backend_name = "pandas"
    _backend_subkeys = []

    @classmethod
    def _init_plugin(klass, key):
        super()._init_plugin(key)
        klass._backend_registry = {}
        klass._backend_subkeys = [key, *klass._backend_subkeys]
        klass.create_table = Driver.create_table.copy()

    @classmethod
    def _backend_lookup(klass, name):
        try:
            return klass._backend_registry[name]
        except KeyError:
            backend_class = _backend_class_registry.lookup(name, subkeys=klass._backend_subkeys)
            backend = backend_class()
            klass._backend_registry[name] = backend
            backend.register_handlers({'create_table': klass.create_table.td})
            logging.debug(f"backend {backend._plugin_name} for {klass._plugin_name} loaded")
            return backend

    def __init__(self, name, drv_cfg, url):
        self._name = name
        self._cfg = drv_cfg
        self._url = url
        self._engine = sqlalchemy.create_engine(url)
        self._last_key = 0
        self._default_backend = self._backend_lookup(self._cfg.get("backend", self._default_backend_name))

    def _backend(self, name):
        if name is None:
            return self._default_backend
        return self._backend_lookup(name)

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def query(self, sql, parameters, backend, kws):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query(self, sql, parameters, kws)

    def execute(self, sql, parameters=None):
        self._engine.execute(Wrap(sql), parameters)

    def execute_script(self, sql):
        logging.debug(f"database execute_script code: {repr(sql)}")
        conn = self._engine.raw_connection()
        try:
            conn.executescript(sql)
            conn.commit()
        finally:
            conn.close()

    def read_table(self, table_name, backend, kws):
        return self.query(f"select * from {table_name}", None, backend, kws)

    @dispatcher({str: '_create_table_from_str',
                 sas.elements.ClauseElement: '_create_table_from_clause'},
                ix=1)
    def create_table(self, table_name, sql_or_df, parameters, if_exists, kws):
        ...

    def _create_table_from_str(self, table_name, sql, parameters, if_exists, kws):
        return self._create_table_from_clause(table_name, Wrap(sql), parameters, if_exists, kws)

    def _create_table_from_clause(self, table_name, clause, parameters, if_exists, kws):
        with self._engine.begin() as conn:
            if_not_exists = False
            if if_exists == "replace":
                conn.execute(DropTable(table_name, if_exists=True))
            elif if_exists == "ignore":
                if_not_exists = True
            conn.execute(CreateTableAs(table_name, clause, if_not_exists=if_not_exists),
                         parameters)

    def create_view(self, view_name, sql, parameters, if_exists):
        with self._engine.begin() as conn:
            if_not_exists = False
            if if_exists == "replace":
                conn.execute(DropView(view_name, if_exists=True))
            elif if_exists == "ignore":
                if_not_exists = True
            conn.execute(CreateViewAs(view_name, Wrap(sql), if_not_exists=if_not_exists),
                         parameters)

    def engine(self):
        return self._engine

    def connection(self):
        return self._engine.begin()

    def url(self):
        return self._url

    def query_chunked(self, sql, parameters, backend, kws):
        return self._backend(backend).query_chunked(self, sql, parameters, kws)

    def query_group(self, sql, parameters, by, backend, kws):
        return self._backend(backend).query_group(self, sql, parameters, by, kws)

    def _pop_kw(self, kws, name, default=None):
        try:
            return kws.pop(name)
        except KeyError:
            return self._cfg.get(name, default)

    def load_backend(self, name):
        self._backend_lookup(name)
