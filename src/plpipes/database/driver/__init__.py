import logging
import sqlalchemy
import sqlalchemy.sql as sas
from contextlib import contextmanager

from plpipes.database.driver.transaction import Transaction
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
        klass._create_table = Driver._create_table.copy()

    @classmethod
    def _backend_lookup(klass, name):
        try:
            return klass._backend_registry[name]
        except KeyError:
            backend_class = _backend_class_registry.lookup(name, subkeys=klass._backend_subkeys)
            backend = backend_class()
            klass._backend_registry[name] = backend
            backend.register_handlers({'create_table': klass._create_table.td})
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
        logging.debug(f"looking up backend {name}")
        return self._backend_lookup(name)

    @contextmanager
    def begin(self):
        with self._engine.connect() as conn:
            with conn.begin():
                yield Transaction(self, conn)

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def _query(self, txn, sql, parameters, backend, kws):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query(txn, sql, parameters, kws)

    def _execute(self, txn, sql, parameters=None):
        txn._conn.execute(Wrap(sql), parameters)

    def _execute_script(self, txn, sql):
        logging.debug(f"database execute_script code: {repr(sql)}")
        txn._conn.executescript(sql)

    def _read_table(self, txn, table_name, backend, kws):
        return self._query(txn, f"select * from {table_name}", None, backend, kws)

    def _drop_table(self, txn, table_name, only_if_exists):
        txn._conn.execute(DropTable(table_name, if_exists=only_if_exists))

    @dispatcher({str: '_create_table_from_str',
                 sas.elements.ClauseElement: '_create_table_from_clause'},
                ix=2)
    def _create_table(self, txn, table_name, sql_or_df, parameters, if_exists, kws):
        ...

    def _create_table_from_str(self, txn, table_name, sql, parameters, if_exists, kws):
        return self._create_table_from_clause(txn, table_name, Wrap(sql), parameters, if_exists, kws)

    def _create_table_from_clause(self, txn, table_name, clause, parameters, if_exists, kws):
        if_not_exists = False
        if if_exists == "replace":
            self._drop_table(txn, table_name, True)
        elif if_exists == "ignore":
            if_not_exists = True
        txn._conn.execute(CreateTableAs(table_name, clause,
                                        if_not_exists=if_not_exists),
                          parameters)

    def _create_view(self, txn, view_name, sql, parameters, if_exists):
        if_not_exists = False
        if if_exists == "replace":
            txn._conn.execute(DropView(view_name, if_exists=True))
        elif if_exists == "ignore":
            if_not_exists = True
        txn._conn.execute(CreateViewAs(view_name, Wrap(sql),
                                       if_not_exists=if_not_exists),
                          parameters)

    def _copy_table(self, txn, from_table_name, to_table_name, if_exists, kws):
        return self._create_table_from_str(txn, to_table_name,
                                           f"select * from {from_table_name}", None,
                                           if_exists, kws)

    def engine(self):
        return self._engine

    def url(self):
        return self._url

    def _read_table_chunked(self, txn, table_name, backend, kws):
        return self._query_chunked(txn, f"select * from {table_name}", None, backend, kws)

    def _query_chunked(self, txn, sql, parameters, backend, kws):
        return self._backend(backend).query_chunked(txn, sql, parameters, kws)

    def _query_group(self, txn, sql, parameters, by, backend, kws):
        return self._backend(backend).query_group(txn, sql, parameters, by, kws)

    def _pop_kw(self, kws, name, default=None):
        try:
            return kws.pop(name)
        except KeyError:
            return self._cfg.get(name, default)

    def load_backend(self, name):
        self._backend_lookup(name)
