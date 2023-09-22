import contextvars
import logging

from plpipes.util.contextvar import set_context_var

_current_registry = contextvars.ContextVar('registry')
_current_key = contextvars.ContextVar('key')

def plugin(klass):
    """
    A decorator that initializes a plugin and adds it to the registry.

    Args:
        klass: The class to be registered as a plugin.

    Returns:
        The decorated class.
    """
    klass._init_plugin(_current_key.get())
    _current_registry.get()._add(klass)
    return klass

class Registry():
    """
    A class to manage a registry of plugins.

    Attributes:
        _name: The name of the registry.
        _path: The import path for the plugins.
        _registry: A dictionary holding registered plugins.
    """

    def __init__(self, name, path):
        """
        Initializes the Registry with a name and path.

        Args:
            name: The name of the registry.
            path: The import path where plugins are located.
        """
        self._name = name
        self._path = path
        self._registry = {}

    def _add(self, obj):
        """
        Adds an object to the registry using the current key.

        Args:
            obj: The object to be added to the registry.
        """
        key = _current_key.get()
        self._registry[key] = obj

    def lookup(self, key, subkeys=None):
        """
        Looks up a registered object by its key and optional subkeys.

        Args:
            key: The main key for the lookup.
            subkeys: Optional list of subkeys to further refine the lookup.

        Returns:
            The object associated with the given key and subkeys.

        Raises:
            ModuleNotFoundError: If the module corresponding to the key cannot be found.
        """
        if subkeys:
            long_key = "__".join([key, subkeys[0]])
        else:
            long_key = key
        if long_key not in self._registry:
            try:
                with set_context_var(_current_registry, self), \
                     set_context_var(_current_key, long_key):
                    module = self._path + "." + long_key
                    logging.debug(f"loading class {module} for key {long_key} in registry {self._name}")
                    __import__(module)
            except ModuleNotFoundError:
                if subkeys:
                    self._registry[long_key] = self.lookup(key, subkeys[1:])
                else:
                    raise
        return self._registry[long_key]

class Plugin():
    """
    A base class for plugins to be registered in the Registry.
    """

    @classmethod
    def _init_plugin(klass, key):
        """
        Initializes the plugin with its key.

        Args:
            klass: The class being initialized as a plugin.
            key: The key associated with this plugin.
        """
        klass._plugin_name = key
