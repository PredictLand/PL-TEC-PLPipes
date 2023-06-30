from plpipes.plugin import plugin
from plpipes.cloud.azure.auth.base import AuthenticatorBase
from azure.identity import ClientSecretCredential
import logging

from plpipes.exceptions import AuthenticationError


@plugin
class IBCAuthenticator(AuthenticatorBase):

    def _authenticate(self):
        return ClientSecretCredential(tenant_id=self._cfg["tenant_id"],
                                      client_id=self._cfg["client_id"],
                                      client_secret=self._cfg["client_secret"])
