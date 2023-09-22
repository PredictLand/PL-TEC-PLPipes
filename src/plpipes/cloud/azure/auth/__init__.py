"""
Module for Azure authentication management within the plpipes framework.

This module provides functionalities to manage Azure credentials using different authentication methods,
including interactive browser and Azure CLI methods. Credential objects can be retrieved based on the 
configured authentication account.
"""

from plpipes.config import cfg
import plpipes.plugin

_backend_class_registry = plpipes.plugin.Registry("azure_auth_backend",
                                                  "plpipes.cloud.azure.auth.plugin")

_registry = {}

def credentials(account_name):
    """
    Retrieves the credential object associated with the given account name.

    Args:
        account_name (str): The name of the authentication account.

    Returns:
        Credential object for the specified account.
    """
    if account_name not in _registry:
        _registry[account_name] = _init_backend(account_name)
    return _registry[account_name].credentials()

def _init_backend(account_name):
    """
    Initializes the authentication backend based on the account configuration.

    Args:
        account_name (str): The name of the authentication account.

    Returns:
        An instance of the backend class for the specified account.
    """
    acfg = cfg.cd("cloud.azure.auth").cd(account_name)

    backend_name = acfg.get("driver", "interactive_browser")
    backend_class = _backend_class_registry.lookup(backend_name)
    return backend_class(account_name, acfg)
