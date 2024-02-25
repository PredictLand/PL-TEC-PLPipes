import logging

from plpipes.database.driver import Driver
from plpipes.database.driver.transaction import Transaction
from plpipes.plugin import plugin
from contextlib import contextmanager
from plpipes.spark import spark_session

import pyspark.sql.functions
import pyspark.sql.types

def _check_backend(name):
    assert name in (None, "spark")

@plugin
class SparkDriver(Driver):

    _default_backend_name = "spark"
    _transaction_factory = Transaction

    @classmethod
    def _init_plugin(klass, *args, **kwargs):
        super()._init_plugin(*args, **kwargs)
        klass._create_table.td.lazy_register_cb = klass._lazy_register_backend

    @classmethod
    def _lazy_register_backend(klass, td, class_name):
        logging.debug(f'lazy_register_backends: {td} {class_name}')
        if class_name == 'pandas.core.frame.DataFrame':
            klass._backend_lookup('pandas')
            if class_name == 'builtins.dict':
                klass._backend_lookup('dict')
        else:
            return False
        return True

    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg)
        self._spark_session = spark_session()
        logging.debug(f"Initialized spark driver {name}, using spark session {self._spark_session}")

        #date_format_udf = pyspark.sql.functions.udf(pyspark.sql.functions.date_format,
        #                                            pyspark.sql.types.StringType())
        #self._spark_session.udf.register("strftime",
        #                                 date_format_udf)
        #                                 #pyspark.sql.types.StringType())


        # self._database_name = drv_cfg.get('database_name', name)
        # self._session.sql(f'CREATE DATABASE IF NOT EXISTS {self._database_name}')
        # self._session.sql(f'USE {self._database_name}')

    @contextmanager
    def begin(self):
        yield self._transaction_factory(self, self._spark_session)

    @classmethod
    def sanitize_table_name(klass, table_name):
        if '`' in table_name:
            raise ValueError(f"Table name {table_name} contains backticks")
        return f"`{table_name}`"

    def _table_exists_p(self, table_name):
        return spark_session().catalog.tableExists(table_name)

    def _read_table(self, txn, table_name, backend, kws):
        return self._backend(backend).read_table(txn, table_name, kws)

    def _create_table_from_str(self, txn, table_name, sql, parameters, if_exists, kws):
        if if_exists == "replace":
            self._drop_table(txn, table_name, True)
        elif if_exists == "ignore":
            if self._table_exists_p(table_name):
                return
        else:
            raise ValueError(f"Bad value {if_exists} for if_exists")

        table_name = self.sanitize_table_name(table_name)
        sql = f"CREATE TABLE {table_name} AS {sql}"
        self._spark_session.sql(sql)

        # self._spark_session.sql(f"describe formatted {table_name}").show()

    def _drop_table(self, txn, table_name, only_if_exists):
        if only_if_exists and not self._table_exists_p(table_name):
            return
        self._spark_session.sql(f"DROP TABLE {self.sanitize_table_name(table_name)}")
