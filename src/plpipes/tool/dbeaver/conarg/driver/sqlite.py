from plpipes.plugin import plugin
import plpipes.database as db
import pathlib

from plpipes.tool.dbeaver.conarg import ConArg

@plugin
class SQLiteConArg(ConArg):
    def __init__(self, name, db_drv):

        super().__init__(name, db_drv)
        self._fn = fn = db_drv.backing_filename()
        self.database = str(pathlib.Path(fn).absolute())
        self.driver = "sqlite"

    def active(self):
        if pathlib.Path(self._fn).exists():
            return True

    def __str__(self):
        args = []
        for arg in self._con_args:
            if hasattr(self, arg):
                args.append(f"{arg}={getattr(self, arg)}")
        return "|".join(args)
