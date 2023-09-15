import logging
from contextlib import contextmanager
from plpipes.database.driver.transaction import Transaction
from plpipes.util.typedict import dispatcher
from plpipes.util.method_decorators import optional_abstract
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
        klass._create_table = klass._create_table.copy()

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

    def __init__(self, name, drv_cfg):
        self._name = name
        self._cfg = drv_cfg
        self._last_key = 0
        self._default_backend = self._backend_lookup(self._cfg.get("backend", self._default_backend_name))

    def _backend(self, name):
        if name is None:
            return self._default_backend
        logging.debug(f"looking up backend {name}")
        return self._backend_lookup(name)

    @optional_abstract
    @contextmanager
    def begin(self):
        ...

    @optional_abstract
    def _execute(self, txn, sql, parameters=None):
        ...

    @optional_abstract
    def _execute_script(self, txn, sql):
        ...

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def _query(self, txn, sql, parameters, backend, kws):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query(txn, sql, parameters, kws)

    def _query_first(self, txn, sql, parameters, backend, kws):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query_first(txn, sql, parameters, kws)

    def _query_first_value(self, txn, sql, parameters, backend, kws):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        return self._backend(backend).query_first_value(txn, sql, parameters, kws)

    @optional_abstract
    def _read_table(self, txn, table_name, backend, kws):
        ...

    @optional_abstract
    def _drop_table(self, txn, table_name, only_if_exists):
        ...

    @dispatcher({str: '_create_table_from_str',
                 list: '_create_table_from_records'},
                ix=2)
    def _create_table(self, txn, table_name, sql_or_df, parameters, if_exists, kws):
        ...

    @optional_abstract
    def _create_table_from_str(self, txn, table_name, sql, parameters, if_exists, kws):
        ...

    @optional_abstract
    def _create_table_from_clause(self, txn, table_name, clause, parameters, if_exists, kws):
        ...

    def _create_table_from_records(self, txn, table_name, records, parameters, if_exists, kws):
        backend = self._backend(kws.pop("backend", None))
        backend.create_table_from_records(txn, table_name, records, parameters, if_exists, kws)

    @optional_abstract
    def _create_view(self, txn, view_name, sql, parameters, if_exists, kws):
        ...

    @optional_abstract
    def _copy_table(self, txn, from_table_name, to_table_name, if_exists, kws):
        ...

    @optional_abstract
    def _read_table_chunked(self, txn, table_name, backend, kws):
        ...

    def _query_chunked(self, txn, sql, parameters, backend, kws):
        return self._backend(backend).query_chunked(txn, sql, parameters, kws)

    def _query_group(self, txn, sql, parameters, by, backend, kws):
        return self._backend(backend).query_group(txn, sql, parameters, by, kws)

    def load_backend(self, name):
        self._backend_lookup(name)
