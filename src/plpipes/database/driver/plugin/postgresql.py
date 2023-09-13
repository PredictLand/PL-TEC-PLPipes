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

@plugin
class PostgreSQLDriver(SQLAlchemyDriver):
    def __init__(self, name, drv_cfg):
        cs = urlparse(drv_cfg.get("connection_string", "postgresql:"))
        try:
            sqla_driver = cs.scheme.split("+", 2)[1]
        except IndexError:
            sqla_driver = "psycopg2"

        host     = drv_cfg.setdefault('host',     first(cs.hostname))
        port     = drv_cfg.setdefault('port',     first(cs.port))
        database = drv_cfg.setdefault('database', first(cs.path))
        user     = drv_cfg.setdefault('user',     first(cs.username))
        password = drv_cfg.setdefault('password', first(cs.password))

        sqla_driver = drv_cfg.setdefault('sqlalchemy_driver', first(sqla_driver, "psycopg2"))

        # create(drivername: str, username: Optional[str] = None,
        # password: Optional[str] = None, host: Optional[str] = None,
        # port: Optional[int] = None, database: Optional[str] = None,
        # query: Mapping[str, Union[Sequence[str], str]] = {}) → URL¶

        url = sqlalchemy.engine.URL.create("postgresql+" + sqla_driver,
                                           username=user, password=password,
                                           host=host, port=port,
                                           database=database)

        logging.debug(f"SQLAlchemy PostgreSQL url: {url}")

        super().__init__(name, drv_cfg, url)
