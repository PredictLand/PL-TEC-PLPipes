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


class _CredentialWrapper():

    def __init__(self, *args, az_config_dir=None, **kwargs):
        self.__credential = None
        self.__az_config_dir = az_config_dir
        self.__ctor_args = args
        self.__ctor_kwargs = kwargs

    def __cred(self, renew=False):
        if self.__credential is None or renew:
            self.__credential = AzureCliCredential(*self.__args, **self.__kwargs)
        return self.__credential

    def __az_login(self):
        with _TempEnv(AZURE_CONFIG_DIR=str(self.__az_config_dir)):
            if sys.platform.startswith("win"):
                cmd = ['cmd', '/c', az_login]
            else:
                cmd = ['/bin/sh', '-c', az_login]
            logging.debug(f"running {cmd} with env {os.environ}")
            subprocess.run(cmd, check=True)
        return self.__cred(renew=True)
        
    def __getattr__(self, name):
        try:
            cred = self.__cred()
            may_retry = True
        except:
            cred = self.__az_login()
            may_retry = False

        attr = getattr(cred, name)
        if not callable(attr):
            return attr

        def method_wrapper(*args, **kwargs):
            with _TempEnv(AZURE_CONFIG_DIR=str(self.__az_config_dir)):
                try:
                    if may_retry:
                        try:
                            return attr(*args, **kwargs)
                        except:
                            if self.__az_config_dir is None or __az_config_dir.isdir():
                                logging.exception(f"Authentication failed when calling '{name}', running 'az login' in order to refresh credentials")                

                        cred = self.__az_login()
                        attr = getattr(cred)
                    return attr(*args, **kwargs)

                except:
                    if path:
                        msg = f"Authentication with Azure CLI failed when calling '{name}', you may need to remove cached private az credentials at {path}"
                    else:
                        msg = "Authentication with Azure CLI failed"
                        logging.exception(msg)
                    raise

        return method_wrapper

@plugin
class AzureCliAuthenticator(AuthenticatorBase):
    def _authenticate(self):
        private = self._cfg.get("private", True)
        path = self._private_path("az-config", create=False) if private else None
        return _CredentialWrapper(az_config_dir=path)
