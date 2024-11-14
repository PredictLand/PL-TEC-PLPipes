from plpipes.plugin import plugin
from plpipes.tool.dbeaver.con import Con
from urllib.parse import quote

class SQLServerCon(Con):
    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg)

        host = drv_cfg['server']
        database = drv_cfg['database']
        port = drv_cfg.get('port', '1433')
        low_level_proto = quote(drv_cfg.setdefault('low_level_proto', 'tcp'))

        encrypt = quote(drv_cfg.setdefault('encrypt', 'yes'))
        trust_server_certificate = quote(drv_cfg.setdefault('trust_server_certificate', 'no'))
        connection_timeout = drv_cfg.setdefault('connection_timeout')
        user = drv_cfg.get('uid')
        password = drv_cfg.get('password')

        url  = f"jdbc:sqlserver://{quote(host)}:{quote(port)};databaseName={quote(database)}"
        url += f";encrypt={encrypt};"
        url += f"trustServerCertificate={trust_server_certificate}"
        if connection_timeout:
            url += f";loginTimeout={quote(connection_timeout)}"
        if user:
            url += f";user={quote(user)}"
        if password:
            url += f";password={quote(password)}"

        if "|" in password:
            raise ValueError(f"Password for database instance {self.name} contains a pipe character, which is not allowed")

        self.host = host
        self.server = host
        self.port = port
        self.database = database
        #self.url = url
        self.driver = "microsoft"
        self.auth = "native"
        self.user = user
        self.password = password
        self.connect = False
        self.create = True
