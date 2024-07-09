from abc import abstractmethod

class Magic:
    @abstractmethod
    def resolve(self, config, key, frame):
        pass

    def __copy__(self):
        return self

    def __deepcopy__(self, memo):
        return self

class InterpolateMagic(Magic):
    def __init__(self, string):
        self._string = string
        self._template = None

    def resolve(self, config, key, frame):
        if self._template is None:
            from . import interpolate
            self._template = interpolate.Template(self._string)
        return self._template.render(config, key, frame)

class GlobMagic(Magic):
    def __init__(self, glob):
        self._glob = glob

    def resolve(self, config, key, frame):
        return self._glob

    def __lt__(self, other):
        if isinstance(other, GlobMagic):
            return self.glob < other.glob
        if isinstance(other, str):
            return True
        return NotImplemented()

    def __gt__(self, other):
        if isinstance(other, GlobMagic):
            return self.glob > other.glob
        if isinstance(other, str):
            return False
        return NotImplemented()

    def __str__(self):
        return self._glob

    def __repr__(self):
        return f"Glob({self._glob!r})"

class LinkMagic(Magic):
    def __init__(self, link):
        self._link = link

    def __str__(self):
        return self._link

    def __repr__(self):
        return f"Link({self._link!r})"

AsteriskMagic = GlobMagic("*")
