import sqlalchemy.engine

from plpipes.database.driver.sqlalchemy import SQLAlchemyDriver

class ODBCDriver(SQLAlchemyDriver):
    def __init__(self, name, drv_cfg, **kwargs):
        url = sqlalchemy.engine.URL.create(drv_cfg['sql_alchemy_driver'],
                                           query={'odbc_connect': drv_cfg['connection_string']})
        super().__init__(name, drv_cfg, url, **kwargs)
