import logging

from plpipes.plugin import plugin
from plpipes.cloud.google.auth.base import AuthenticatorBase

from plpipes.exceptions import AuthenticationError

import google.oauth2.credentials
from google_auth_oauthlib.flow import InstalledAppFlow

@plugin
class OAuth2Authenticator(AuthenticatorBase):

    def _authenticate(self):
        auf = self._credentials_cache_filename()
        scopes = self._cfg.get("scopes", ['https://www.googleapis.com/auth/cloud-platform'])
        try:
            if auf.is_file():
                creds = google.oauth2.credentials.Credentials.from_authorized_user_file(auf, scopes=scopes)
                return creds
        except:
            logging.exception(f"Cached credentials {auf} failed!")

        installed = self._cfg.to_tree("installed")
        logging.info(f"installed config: {installed}\nfull config: {self._cfg.to_tree()}")

        flow = InstalledAppFlow.from_client_config(client_config={'installed': installed},
                                                   scopes=scopes)
        creds = flow.run_local_server(port=0)
        with open(auf, "w") as f:
            f.write(creds.to_json())

        return creds
