import mako

class _ConfigWrapper:
    def __init__(self, tree, frame):
        self._tree = tree
        self._frame = frame

    def __getitem__(self, key):
        tree = self._config.cd(key)
        return _ConfigWrapper(tree, self._frame)
        return self._config[key]

class Template:

    def __init__(self, string):
        self._template = mako.template.Template(string)

    def render(self, config, key, frame):
        return self._template.render()
