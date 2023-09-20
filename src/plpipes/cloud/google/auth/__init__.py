from plpipes.config import cfg
import plpipes.plugin

_backend_class_registry = plpipes.plugin.Registry("google_auth_backend",
                                                  "plpipes.cloud.google.auth.plugin")

_registry = {}

def credentials(account_name):
    if account_name not in _registry:
        _registry[account_name] = _init_backend(account_name)
    return _registry[account_name].credentials()

def _init_backend(account_name):
    acfg = cfg.cd("cloud.google.auth").cd(account_name)

    backend_name = acfg.get("driver", "oauth2")
    backend_class = _backend_class_registry.lookup(backend_name)
    return backend_class(account_name, acfg)

