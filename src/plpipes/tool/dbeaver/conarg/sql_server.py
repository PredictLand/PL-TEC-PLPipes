from plpipes.plugin import plugin
from plpipes.tool.dbeaver.conarg import ConArg
from urllib.parse import quote

class SQLServerConArg(ConArg):
    def __init__(self, name, db_drv):
        super().__init__(name, db_drv)
        db_cfg = self._cfg
        host = db_cfg['server']
        database = db_cfg['database']
        port = db_cfg.get('port', '1433')
        user = db_cfg.get('uid')
        password = db_cfg.get('password')
        if "|" in password:
            raise ValueError(f"Password for database instance {self.name} contains a pipe character, which is not allowed")

        self.host = host
        self.server = host
        self.port = port
        self.database = database
        self.driver = "microsoft"
        self.auth = "native"
        self.user = user
        self.password = password
        self.connect = False
        self.create = True
