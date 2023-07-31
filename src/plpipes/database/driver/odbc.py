import sqlalchemy.engine

from plpipes.database.driver import Driver

class ODBCDriver(Driver):
    def __init__(self, name, drv_cfg):
        url = sqlalchemy.engine.URL.create(drv_cfg['sql_alchemy_driver'],
                                           query={'odbc_connect': drv_cfg['connection_string']})
        super().__init__(name, drv_cfg, url)
