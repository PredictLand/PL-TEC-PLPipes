import logging
from plpipes.database.driver.odbc import ODBCDriver
import struct

_SQL_COPT_SS_ACCESS_TOKEN = 1256

class AzureSQLDriver(ODBCDriver):
    def __init__(self, name, drv_cfg):
        driver = drv_cfg.setdefault('driver_odbc', '{ODBC Driver 18 for SQL Server}')
        server = drv_cfg['server']
        database = drv_cfg['database']
        low_level_proto = drv_cfg.setdefault('low_level_proto', 'tcp')
        port = drv_cfg.setdefault('port', 1433)
        cs = f"Driver={driver};Server={server};Database={database}"
        drv_cfg.setdefault('connection_string', cs)
        drv_cfg.setdefault('sql_alchemy_driver', 'mssql+pyodbc')

        
        import plpipes.cloud.azure.auth
        creds_account_name = drv_cfg['credentials']
        credential = plpipes.cloud.azure.auth.credentials(creds_account_name)

        token = credential.get_token('https://database.windows.net').token.encode("utf-16-le")
        token_struct = struct.pack(f"<I{len(token)}s", len(token), token)

        super().__init__(name, drv_cfg, connect_args={'attrs_before': {_SQL_COPT_SS_ACCESS_TOKEN: token_struct}})

