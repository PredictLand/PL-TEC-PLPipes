import logging
import sqlalchemy

import sqlalchemy.sql as sas

from plpipes.database.sqlext import CreateTableAs, CreateViewAs, DropTable, DropView, Wrap
from plpipes.util.typedict import TypeDict

import plpipes.plugin

_backend_class_registry = plpipes.plugin.Registry("db_backend", "plpipes.database.backend.plugin")
_backend_registry = {}

_create_table__handlers = TypeDict({str: '_create_table_from_str',
                                    sas.elements.ClauseElement: '_create_table_from_clause'},
                                   ix=1)

def backend_lookup(name):
    if name not in _backend_registry:
        backend_class = _backend_class_registry.lookup(name)
        _backend_registry[name] = backend_class()
        _backend_registry[name].register_handlers({'create_table': _create_table__handlers})
    return _backend_registry[name]

class Driver:
    _default_backend_name = "pandas"

    def __init__(self, name, drv_cfg, url):
        self._name = name
        self._cfg = drv_cfg
        self._url = url
        self._engine = sqlalchemy.create_engine(url)
        self._last_key = 0
        backend_name = self._cfg.get("backend", self._default_backend_name)

        self._default_backend = backend_lookup(self._cfg.get("backend", self._default_backend_name))

        print(f"default backend name: {self._default_backend_name}, backend name: {backend_name}, backend: {self._default_backend}")

    def _backend(self, name):
        if name is None:
            return self._default_backend
        return backend_lookup(name)

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

    @_create_table__handlers.dispatcher
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
