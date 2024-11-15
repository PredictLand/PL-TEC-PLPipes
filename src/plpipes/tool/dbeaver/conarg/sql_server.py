from plpipes.plugin import plugin
from plpipes.tool.dbeaver.conarg import ConArg

class SQLServerConArg(ConArg):
    def __init__(self, name, db_drv):
        super().__init__(name, db_drv)
        db_cfg = self._cfg
        host = db_cfg['server']
        database = db_cfg['database']
        port = db_cfg.get('port', '1433')
        user = db_cfg.get('uid')
        password = db_cfg.get('password')

        self.host = host
        self.server = host
        self.port = port
        self.database = database
        self.driver = "microsoft"
        self.auth = "native"
        self.user = user
        self.password = password
