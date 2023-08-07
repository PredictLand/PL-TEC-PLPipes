from plpipes.plugin import plugin
from plpipes.cloud.azure.auth.base import AuthenticatorBase
from azure.identity import AzureCliCredential
import logging
import os
import sys
import subprocess

from plpipes.exceptions import AuthenticationError

# TODO: Move this into some utility package!
class _TempEnv:

    def __init__(self, **envs):
        self._new_envs = envs
        self._old_envs = {}

    def __enter__(self):
        for k, v in self._new_envs.items():
            self._old_envs[k] = os.environ.get(k)
            if v is None:
                try:
                    del os.environ[k]
                except KeyError:
                    pass
            else:
                os.environ[k] = v

    def __exit__(self, _1, _2, _3):
        for k, v in self._old_envs.items():
            if v is None:
                try:
                    del os.environ[k]
                except KeyError:
                    pass
            else:
                os.environ[k] = v


class MyAzureCliCredential(AzureCliCredential):

    def __init__(self, *args, az_config_dir=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._az_config_dir = az_config_dir

    def get_token(self, *args, **kwargs):
        with _TempEnv(AZURE_CONFIG_DIR=self._az_config_dir):
            logging.debug(f"Running az command with env {os.environ}")
            return super().get_token(*args, **kwargs)

@plugin
class AzureCliAuthenticator(AuthenticatorBase):

    def _authenticate(self):
        if self._cfg.get("private", True):
            path = self._private_path("az-config", create=False)
            with _TempEnv(AZURE_CONFIG_DIR=str(path)):
                if not path.exists():
                    path.mkdir(parents=True)
                    az_login = "az login"
                    if sys.platform.startswith("win"):
                        cmd = ['cmd', '/c', az_login]
                    else:
                        cmd = ['/bin/sh', '-c', az_login]
                    logging.debug(f"running {cmd} with env {os.environ}")
                    subprocess.run(cmd, check=True)

                try:
                    return MyAzureCliCredential(az_config_dir=str(path))
                except Exception as ex:
                    raise AuthenticationError(f"Authentication failed, you may need to remove cached az credentials at {path}") from ex
        else:
            return AzureCliCredential()
