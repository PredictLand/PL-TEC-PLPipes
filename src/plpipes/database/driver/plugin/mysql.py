import logging
import sqlalchemy.engine
from urllib.parse import urlunparse, urlparse

from plpipes.database.driver.mysql import MySQLBaseDriver
from plpipes.plugin import plugin

@plugin
class MySQLDriver(MySQLBaseDriver):

    def _default_sqla_driver(self):
        return "mysql"
