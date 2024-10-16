import logging
import sqlalchemy.engine
from urllib.parse import urlunparse, urlparse

from plpipes.database.driver.mysql import MySQLBaseDriver
from plpipes.plugin import plugin

@plugin
class MariaDBDriver(MySQLBaseDriver):

    def _default_sqla_driver(self):
        return "mariadb"
