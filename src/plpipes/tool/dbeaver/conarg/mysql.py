from plpipes.plugin import plugin
from plpipes.tool.dbeaver.conarg import ConArg

class MySQLBaseConArg(ConArg):
    def __init__(self, name, db_drv):
        super().__init__(name, db_drv)
        db_cfg = self._cfg
        host = db_cfg['host']
        database = db_cfg['database']
        port = db_cfg.get('port', '3306')
        user = db_cfg.get('user')
        password = db_cfg.get('password')

        self.host = host
        self.server = host
        self.port = port
        self.database = database
        self.auth = "native"
        self.user = user
        self.password = password

        self.driver = "microsoft"

