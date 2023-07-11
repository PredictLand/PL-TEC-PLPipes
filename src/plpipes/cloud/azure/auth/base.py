import pathlib

from plpipes.exceptions import AuthenticationError
from plpipes.plugin import Plugin

class AuthenticatorBase(Plugin):

    def __init__(self, account_name, acfg):
        self._account_name = account_name
        self._cfg = acfg
        self._credentials = None

    def credentials(self):
        if self._credentials is None:
            try:
                self._credentials = self._authenticate()
            except AuthenticationError:
                raise
            except Exception as ex:
                raise AuthenticationError(f"Authentication for account {self._account_name} failed") from ex
        return self._credentials

    def _credentials_cache_filename(self):
        return pathlib.Path.home() / f".config/plpipes/cloud/azure/auth/{self._account_name}.json"
