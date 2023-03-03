import logging
from plpipes.database.driver.odbc import ODBCDriver

class SQLServerDriver(ODBCDriver):
    def __init__(self, name, drv_cfg):
        if "connection_string" not in drv_cfg:
            driver = drv_cfg.setdefault('driver_odbc', '{ODBC Driver 18 for SQL Server}')
            port = drv_cfg.setdefault('port', 1433)
            low_level_proto = drv_cfg.setdefault('low_level_proto', 'tcp')
            encrypt = 'yes' if drv_cfg.setdefault('encrypt', True) else 'no'
            trusted_server_certificate = 'yes' if drv_cfg.setdefault('trusted_server_sertificate', True) else 'no'
            timeout = drv_cfg.setdefault('timeout', 60)
            server = drv_cfg['server']
            database = drv_cfg['database']
            uid = drv_cfg['user']
            password = drv_cfg['password']

            cs_log = f"Driver={driver};Server={low_level_proto}:{server},{port};Database={database};Uid={uid};Pwd=**********;Encrypt={encrypt};TrustedServerCertificate={trusted_server_certificate};Connection Timeout={timeout}"
            logging.debug(f"ODBC connection string for {name} is {cs_log}")
            cs     = f"Driver={driver};Server={low_level_proto}:{server},{port};Database={database};Uid={uid};Pwd={password};Encrypt={encrypt};TrustedServerCertificate={trusted_server_certificate};Connection Timeout={timeout}"
            drv_cfg.setdefault('connection_string', cs)
        drv_cfg.setdefault('sql_alchemy_driver', 'mssql+pyodbc')
        super().__init__(name, drv_cfg)
