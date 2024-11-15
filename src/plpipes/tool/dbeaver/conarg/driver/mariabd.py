from plpipes.plugin import plugin
from plpipes.tool.dbeaver.conarg.mysql import MySQLBaseConArg

@plugin
class MariaDBConArg(MySQLBaseConArg):

    def __init__(self, name, db_drv):
        super().__init__(name, db_drv)
        self.driver = "mariadb"

