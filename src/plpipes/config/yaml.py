import yaml
from . import magic

class _Loader(yaml.SafeLoader):
    pass

def _load_interpolate_magic(loader, node):
    return magic.InterpolateMagic(loader.construct_scalar(node))

def _load_glob_magic(loader, node):
    if node.value != "*":
        raise ValueError("Glob magic only supports '*'. Send a feature request if you need more!")
    return magic.AsteriskMagic

def _load_link_magic(loader, node):
    return magic.LinkMagic(loader.construct_scalar(node))

_Loader.add_constructor('!interpolate', _load_interpolate_magic)
_Loader.add_constructor('!glob', _load_glob_magic)
_Loader.add_constructor('!link', _load_link_magic)
#CustomLoader.add_constructor('!glob', magic.Glob.load)
#CustomLoader.add_constructor('!env', magic.Env.load)
#CustomLoader.add_constructor('!file', magic.File.load)
#CustomLoader.add_constructor('!link', magic.Link.load)#


def load(stream):
    return yaml.load(stream, Loader=_Loader)
