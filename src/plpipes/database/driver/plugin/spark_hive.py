import logging

from plpipes.database.driver import Driver
from plpipes.database.driver.transaction import Transaction
from plpipes.plugin import plugin
from plpipes.spark import spark
from contextlib import contextmanager

def _check_backend(name):
    assert name in (None, "spark")

@plugin
class SparkDriver(Driver):

    _default_backend_name = "spark"
    _transaction_factory = Transaction

    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg)
        self._session_name = drv_cfg.setdefault('session_name', 'work')
        self._session = spark(self._session_name)
        # self._database_name = drv_cfg.get('database_name', name)
        # self._session.sql(f'CREATE DATABASE IF NOT EXISTS {self._database_name}')
        # self._session.sql(f'USE {self._database_name}')

    @contextmanager
    def begin(self):
        yield self._transaction_factory(self, None)
