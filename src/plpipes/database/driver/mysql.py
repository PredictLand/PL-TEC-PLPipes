import logging
import sqlalchemy.engine
from urllib.parse import urlunparse, urlparse

from plpipes.database.driver.sqlalchemy import SQLAlchemyDriver
from plpipes.plugin import plugin

def first(*args):
    for a in args:
        if a:
            return a
    return None

_default_sqla_subdriver = { "mysql": "mysqldb",
                            "mariadb": "mariadbconnector" }

def first(*args):
    for a in args:
        if a:
            return a
    return None

class MySQLBaseDriver(SQLAlchemyDriver):

    def _default_sqla_driver(self):
        return "mysql"

    def __init__(self, name, drv_cfg):
        dsad = self._default_sqla_driver()
        cs = urlparse(drv_cfg.get("connection_string", f"{dsad}:"))
        if cs.scheme is None or cs.scheme == '':
            sqla_driver = dsad
            more = []
        else:
            sqla_driver, *more = cs.scheme.split("+", 2)

        sqla_subdriver = first(*more, _default_sqla_subdriver.get(sqla_driver))
        if sqla_subdriver is not None:
            sqla_driver += "+" + sqla_subdriver

        host     = drv_cfg.setdefault('host',     first(cs.hostname))
        port     = drv_cfg.setdefault('port',     first(cs.port))
        database = drv_cfg.setdefault('database', first(cs.path))
        user     = drv_cfg.setdefault('user',     first(cs.username))
        password = drv_cfg.setdefault('password', first(cs.password))

        sqla_driver = drv_cfg.setdefault('sqlalchemy_driver', sqla_driver)

        url = sqlalchemy.engine.URL.create(sqla_driver,
                                           username=user, password=password,
                                           host=host, port=port,
                                           database=database)

        logging.debug(f"SQLAlchemy MySQL (or derivative) url: {url}")

        super().__init__(name, drv_cfg, url)
