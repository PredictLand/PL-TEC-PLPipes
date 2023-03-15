import contextvars
import logging

from plpipes.util.contextvar import set_context_var

_current_registry = contextvars.ContextVar('registry')
_current_key = contextvars.ContextVar('key')

def plugin(klass):
    klass._init_plugin(_current_key.get())
    _current_registry.get()._add(klass)
    return klass

class Registry():
    def __init__(self, name, path):
        self._name = name
        self._path = path
        self._registry = {}

    def _add(self, obj):
        key = _current_key.get()
        self._registry[key] = obj

    def lookup(self, key):
        if key not in self._registry:
            with set_context_var(_current_registry, self), \
                 set_context_var(_current_key, key):
                module = self._path + "." + key
                logging.debug(f"loading class {module} for key {key} in registry {self._name}")
                __import__(module)
        return self._registry[key]

class Plugin():
    @classmethod
    def _init_plugin(klass, key):
        klass._plugin_name = key
