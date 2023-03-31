from plpipes.config import cfg
from azure.identity import InteractiveBrowserCredential, TokenCachePersistenceOptions, AuthenticationRecord
import pathlib
import logging

from plpipes.exceptions import AuthenticationError

_registry = {}

def credentials(account_name):
    if account_name not in _registry:
        _authenticate(account_name)
    return _registry[account_name]

def _authenticate(account_name):
    cfg_path = f"cloud.azure.auth.{account_name}"
    acfg = cfg.cd(cfg_path)
    ar_fn = pathlib.Path.home() / f".config/plpipes/cloud/azure/auth/{account_name}.json"
    try:
        with open(ar_fn, "r") as f:
            ar = AuthenticationRecord.deserialize(f.read())
    except:
        logging.debug(f"Couldn't load authentication record for {account_name} from {ar_fn}")
        ar = None

    authentication_callback_port = acfg.setdefault("authentication_callback_port", 8082)
    redirect_uri = f"http://localhost:{authentication_callback_port}"

    allow_unencrypted_storage = acfg.setdefault("allow_unencrypted_storage", False)
    cache_persistence_options = TokenCachePersistenceOptions(allow_unencrypted_storage=allow_unencrypted_storage)

    expected_user = acfg.get("username")

    logging.warn(f"prompt: {acfg.get('prompt', 'login')}")

    cred = InteractiveBrowserCredential(tenant_id=acfg["tenant_id"],
                                        client_id=acfg["client_id"],
                                        client_credential=acfg.get("client_secret"),
                                        prompt=acfg.get("prompt", "login"),
                                        login_hint=expected_user,
                                        redirect_uri=redirect_uri,
                                        cache_persistence_options=cache_persistence_options,
                                        authentication_record=ar)


    import httpx

    client = httpx.Client(http2=True, timeout=None, trust_env=True)
    # client.httpcore_logger.enable("httpx")

    # Make a request to the Graph API
    response = client.get('https://graph.microsoft.com/v1.0/groups',
                          headers={'Authorization': f'Bearer {cred.get_token("https://graph.microsoft.com/.default").token}'})

    # Log the response status code and content
    print(f'Response status code: {response.status_code}')
    print(f'Response content: {response.content}')

    # Print the HTTP request and response logs
    # print(client.httpcore_logger.logs)

    if "scopes" in acfg:
        scopes = acfg["scopes"]
        if isinstance(scopes, str):
            scopes = scopes.split(" ")

        logging.debug("Calling authenticate(scopes={scopes})")
        ar = cred.authenticate(scopes=scopes)

        if expected_user not in (None, ar.username):
            AuthenticationError(f"Authenticating as user {expected_user} expected but {ar.username} found!")
        try:
            logging.debug(f"Saving authentication record to {ar_fn}")
            ar_fn.parent.mkdir(parents=True, exist_ok=True)
            with open(ar_fn, "w") as f:
                f.write(ar.serialize())
        except:
            logging.warning(f"Unable to save authentication record for {account_name} at {ar_fn}", exc_info=True)
    else:
        logging.warning(f"'{cfg_path}.scopes' not configured, credentials for {account_name} are not going to be cached!")

    _registry[account_name] = cred
