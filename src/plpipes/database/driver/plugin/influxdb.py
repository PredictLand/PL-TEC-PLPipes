import logging
import sqlalchemy.engine
from urllib.parse import urlunparse, urlparse

from plpipes.database.driver.sqlalchemy import SQLAlchemyDriver
from plpipes.plugin import plugin

@plugin
class InfluxDBDriver(SQLAlchemyDriver):
    def __init__(self, name, drv_cfg):
        cs = urlparse(drv_cfg.get('connection_string', 'datafusion+flightsql:'))

        if "@" in cs.netloc:
            credentials, _ = cs.split("@")
            user, password = credentials.split(":")
        else:
            user = None
            password = None
        
        host = drv_cfg.setdefault('host', cs.hostname)
        port = drv_cfg.setdefault('port', cs.port if cs.port else 8086)
        user = drv_cfg.setdefault('user', user)
        password = drv_cfg.setdefault('password', password)

        url = sqlalchemy.engine.URL.create(# 'datafusion+flightsql',
                                           'datafusion',
                                           host=host, port=port,
                                           username=user, password=password,
                                           query={#"insecure": "true"
                                               'disable_server_verification': 'true'})

        super().__init__(name, drv_cfg, url)
