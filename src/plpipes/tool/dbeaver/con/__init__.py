import plpipes.plugin

class Con(plpipes.plugin.Plugin):
    _con_args = ['name', 'driver', 'url', 'host', 'port', 'server', 'database', 'user', 'password', 'auth']
    _con_boolean_args = ['save', 'connect', 'openConsole', 'create']

    #def _init_plugin(self, name, drv_cfg):
    #    pass

    def __init__(self, name, drv_cfg):
        self.name = name
        self._cfg = drv_cfg
        super().__init__()

    def active(self):
        return True

    def con_args(self):
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

