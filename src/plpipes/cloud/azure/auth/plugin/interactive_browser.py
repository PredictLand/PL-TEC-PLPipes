
from plpipes.plugin import plugin
from plpipes.cloud.azure.auth.base import AuthenticatorBase
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions, AuthenticationRecord
import logging

from plpipes.exceptions import AuthenticationError


@plugin
class IBCAuthenticator(AuthenticatorBase):

    def _authenticate(self):
        ar_fn = self._credentials_cache_filename()
        try:
            with open(ar_fn, "r") as f:
                ar = AuthenticationRecord.deserialize(f.read())
        except:
            logging.debug(f"Couldn't load authentication record for {self._account_name} from {ar_fn}")
            ar = None
        authentication_callback_port = self._cfg.setdefault("authentication_callback_port", 8082)
        redirect_uri = f"http://localhost:{authentication_callback_port}"

        allow_unencrypted_storage = self._cfg.setdefault("allow_unencrypted_storage", False)
        cache_persistence_options = TokenCachePersistenceOptions(allow_unencrypted_storage=allow_unencrypted_storage)

        expected_user = self._cfg.get("username")

        cred = InteractiveBrowserCredential(tenant_id=self._cfg["tenant_id"],
                                            client_id=self._cfg["client_id"],
                                            client_credential=self._cfg.get("client_secret"),
                                            prompt=self._cfg.get("prompt", "login"),
                                            login_hint=expected_user,
                                            redirect_uri=redirect_uri,
                                            cache_persistence_options=cache_persistence_options,
                                            authentication_record=ar)
        if "scopes" in self._cfg:
            scopes = self._cfg["scopes"]
            if isinstance(scopes, str):
                scopes = scopes.split(" ")

            logging.debug(f"Calling authenticate(scopes={scopes})")
            ar = cred.authenticate(scopes=scopes)

            if expected_user not in (None, ar.username):
                raise AuthenticationError(f"Authenticating as user {expected_user} expected but {ar.username} found!")
            try:
                logging.debug(f"Saving authentication record to {ar_fn}")
                ar_fn.parent.mkdir(parents=True, exist_ok=True)
                with open(ar_fn, "w") as f:
                    f.write(ar.serialize())
            except:
                logging.warning(f"Unable to save authentication record for {account_name} at {ar_fn}", exc_info=True)
        else:
            logging.warning(f"'{cfg_path}.scopes' not configured, credentials for {account_name} are not going to be cached!")

        return cred

