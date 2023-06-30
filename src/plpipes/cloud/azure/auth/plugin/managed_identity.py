from plpipes.plugin import plugin
from plpipes.cloud.azure.auth.base import AuthenticatorBase
from azure.identity import ClientSecretCredential
import logging

from plpipes.exceptions import AuthenticationError


@plugin
class MIAuthenticator(AuthenticatorBase):
    def _authenticate(self):
        return ManagedIdentityCredential()

