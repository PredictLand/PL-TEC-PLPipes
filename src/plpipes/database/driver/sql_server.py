import logging
from plpipes.database.driver.odbc import ODBCDriver

class SQLServerDriver(ODBCDriver):
    def __init__(self, name, drv_cfg):
        if "connection_string" not in drv_cfg:
            driver = drv_cfg.setdefault('driver_odbc', '{ODBC Driver 18 for SQL Server}')
            port = drv_cfg.get('port')
            low_level_proto = drv_cfg.get('low_level_proto', 'tcp')
            encrypt = 'yes' if drv_cfg.setdefault('encrypt', True) else 'no'
            trusted_server_certificate = 'yes' if drv_cfg.setdefault('trusted_server_certificate', True) else 'no'
            trusted_connection = 'yes' if drv_cfg.setdefault('trusted_connection', True) else 'no'
            timeout = drv_cfg.setdefault('timeout', 60)
            server = drv_cfg['server']
            database = drv_cfg['database']
            uid = drv_cfg.get('user')
            password = drv_cfg.get('password')

            cs = {'Driver': driver,
                  'Database': database,
                  'TrustedServerCertificate': trusted_server_certificate,
                  'Connection Timeout': str(timeout),
                  'TrustedConnection': trusted_connection}

            if "\\" not in server:
                server = f"{low_level_proto}:{server}"
                if port is not None:
                    server += f",{port}"
                cs['Encrypt'] = encrypt
            cs['Server'] = server

            if uid is not None:
                cs['Uid'] = uid
            if password is not None:
                cs['Pwd'] = password

            cs = ";".join([f"{k}={v}" for k,v in cs.items()])
            # logging.info(f"Connection string: {cs}")

            drv_cfg['connection_string'] = cs
        drv_cfg.setdefault('sql_alchemy_driver', 'mssql+pyodbc')
        super().__init__(name, drv_cfg)
