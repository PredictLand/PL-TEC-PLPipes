from plpipes.config import cfg
import plpipes.plugin

import openai
from openai import *
openai.api_key = cfg["cloud.openai.auth.api_key"]

_provider_registry = plpipes.plugin.Registry("openai_provider", "plpipes.cloud.openai.provider.plugin")

_client_registry = {}

def openai_client(name=None):
    if name is None:
        name = "default"

    if name not in _client_registry:
        _client_registry[name] = _init_client(name)
    return _client_registry[name]

def _init_client(name):
    client_cfg = cfg.cd(f"cloud.openai.client.{name}")
    provider_name = client_cfg.get("provider", "openai")
    provider_class = _provider_registry.lookup(provider_name)
    client = provider_class.get_client(name, client_cfg)
    return client
