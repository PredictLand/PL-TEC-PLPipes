import contextvars
import logging

_current_registry = contextvars.ContextVar('registry')

def plugin(key):
    def p(klass):
        _current_registry.get()._add(key, klass)
        return klass
    return p

class Registry():
    def __init__(self, name, path):
        self._name = name
        self._path = path
        self._registry = {}

    def _add(self, key, obj):
        self._registry[key] = obj

    def lookup(self, key):
        if key not in self._registry:
            token = _current_registry.set(self)
            try:
                module = self._path + "." + key
                logging.debug(f"loading class {module} for key {key} in registry {self._name}")
                __import__(module)
            finally:
                _current_registry.reset(token)
        return self._registry[key]
