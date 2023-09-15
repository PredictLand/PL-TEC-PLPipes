import logging

from plpipes.database.driver import Driver
from plpipes.database.driver.transaction import Transaction
from plpipes.plugin import plugin

from influxdb import DataFrameClient
from contextlib import contextmanager

def _check_backend(name):
    assert name in (None, "pandas", "multipandas", "autopandas")

DEFAULT_CHUNKSIZE = 5000

@plugin
class InfluxDB1BDriver(Driver):

    def __init__(self, name, drv_cfg):

        super().__init__(name, drv_cfg)

        host       = drv_cfg.setdefault('host', 'localhost')
        port       = drv_cfg.setdefault('port', '8086')
        database   = drv_cfg.get('database')
        username   = drv_cfg.get('user')
        password   = drv_cfg.get('password')
        ssl        = drv_cfg.setdefault('ssl', True)
        verify_ssl = drv_cfg.setdefault('verify_ssl', True)

        client = DataFrameClient(host=host, port=port, database=database,
                                 username=username, password=password,
                                 ssl=ssl, verify_ssl=verify_ssl)

        self._influxdb_client =  client

    def _backend(self, name):
        raise NotImplementedError(f"Method _backend not implemented (it shouldn't be needed anyway with this driver).")

    @contextmanager
    def begin(self):
        yield Transaction(self, None)

    def _make_result(self, r, backend):
        _check_backend(backend)
        if backend == "multipandas":
            return r
        elif len(r) == 1:
          return list(r.values())[0] # pandas & autopandas
        elif backend == "autopandas":
            return r
        else:
            raise ValueError("pandas backend selected but the response contains more than one dataset (consider using multipandas or autopandas backends instead)")

    def _query(self, txn, query_text, parameters, backend, kws):
        kws.setdefault("dropna", False)
        r = self._influxdb_client.query(query_text, params=parameters, **kws)
        return self._make_result(r, backend)

    def _select_all_from_table_query(self, table_name):
        table_name = table_name.replace('"', '\\"')
        q = f'select * from "{table_name}"'
        logging.debug(f"read table query: {q}")
        return q

    def _read_table(self, txn, table_name, backend, kws):
        return self._query(txn,
                           self._select_all_from_table_query(table_name),
                           None, backend, kws)

    def engine(self):
        return self._influxdb_client

    def _query_chunked(self, txn, query_text, parameters, backend, kws):
        kws.setdefault("dropna", False)
        # FIXME: support for chunked queries is broken in the latest
        # versions of the client module, so we just don't do them!
        #
        # for r in self._influxdb_client.query(query_text, params=parameters,
        #                                      chunked=True, chunk_size=DEFAULT_CHUNKSIZE):
        #     yield self._make_result(r, backend)

        yield self._query(txn, query_text, parameters, backend, kws)

    def _read_table_chunked(self, txn, table_name, backend, kws):
        query_text = self._select_all_from_table_query(table_name)
        return self._query_chunked(txn, query_text, None, backend, kws)

