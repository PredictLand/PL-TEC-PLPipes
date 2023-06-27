
import os
import sys
from pathlib import Path
import google_auth_oauthlib  

sys.path.append(str(Path(os.getcwd()).joinpath("src")))


from plpipes.config import cfg
import logging

from plpipes.exceptions import AuthenticationError

_registry = {}
def credentials(account_name):
    if account_name not in _registry:
        _authenticate(account_name)
    return _registry[account_name]

def _authenticate(account_name):
    cfg_path = f"cloud.gcp.auth.{account_name}"
    acfg = cfg.cd(cfg_path)
    if "scopes" in acfg:
        scopes = acfg["scopes"]
        if isinstance(scopes, str):
            scopes = scopes.split(" ")
    else:
        scopes = ""
    cred = google_auth_oauthlib.get_user_credentials(scopes = scopes,
                                                            client_id = acfg["client_id"],
                                                            client_secret = acfg["client_secret"])
    _registry[account_name] = cred
