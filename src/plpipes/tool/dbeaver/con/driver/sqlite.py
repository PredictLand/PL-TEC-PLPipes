from plpipes.plugin import plugin
import plpipes.database as db
import pathlib

from plpipes.tool.dbeaver.con import Con

@plugin
class SQLiteCon(Con):
    def __init__(self, name, drv_cfg):

        super().__init__(name, drv_cfg)
        self._fn = db.lookup(name)._fn
        self.driver = "sqlite"
        self.database = str(pathlib.Path(self._fn).absolute())


    def active(self):
        if pathlib.Path(self._fn).exists():
            return True

    def __str__(self):
        args = []
        for arg in self._con_args:
            if hasattr(self, arg):
                args.append(f"{arg}={getattr(self, arg)}")
        return "|".join(args)
