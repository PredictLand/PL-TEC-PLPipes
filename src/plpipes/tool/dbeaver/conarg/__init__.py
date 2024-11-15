import plpipes.plugin

class ConArg(plpipes.plugin.Plugin):
    _con_args = ['name', 'driver', 'url', 'host', 'port', 'server', 'database', 'user', 'password', 'auth']
    _con_boolean_args = ['save', 'connect', 'openConsole', 'create']

    #def _init_plugin(self, name, drv_cfg):
    #    pass

    def __init__(self, name, db_drv):
        self.name = name
        self._db_drv = db_drv
        self._cfg = db_drv.config()
        super().__init__()

    def active(self):
        return True

    def conargs(self):
        args = {}
        for arg in self._con_args:
            value = getattr(self, arg, None)
            if value is not None:
                args[arg] = value

        for arg in self._con_boolean_args:
            value = getattr(self, arg, None)
            if value is not None:
                args[arg] = "true" if value else "false"
        return args

