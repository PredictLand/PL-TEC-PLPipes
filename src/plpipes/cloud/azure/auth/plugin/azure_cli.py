from plpipes.plugin import plugin
from plpipes.cloud.azure.auth.base import AuthenticatorBase
from azure.identity import AzureCliCredential
import logging

from plpipes.exceptions import AuthenticationError

@plugin
class AzureCliAuthenticator(AuthenticatorBase):

    def _authenticate(self):
        try:
            return AzureCliCredential()
        except Exception as ex:
            raise AuthenticatorError("Azure CLI authentication failed") from ex
