# Azure

## Authentication

Package
[`plpipes.cloud.azure.auth`](reference/plpipes/cloud/azure/auth.pm)
provides an easy way to manage Azure credentials.

### API

Credential objects of type `azure.identity.MsalCredential` can be
retrieved using function `credentials` as follows:

```python
import plpipes.cloud.azure.auth
cred = plpipes.cloud.azure.auth.credentials("predictland")
```

### Configuration

Authentication accounts are declared in the configuration files and
instantiated by the module on demand (which for some kind of
authentication methods may require user interaction).

For instance, the following configuration snippet defines the
authorization account `predictland`.

```yaml
cloud:
  azure:
    auth:
      predictland:
        type: interactive_browser
        tenant_id: 01234567-89ab-cdef-0123-456789abcdef
        client_id: 01234567-89ab-cdef-0123-456789abcdef
        client_secret: super-super-super-super-secret
        authentication_callback_port: 8283
        username: elvis@predictland.com
        scopes:
          - "https://graph.microsoft.com/.default"
```

The meaning of every key is as follows:

- `type`: indicates the type of authentication to be used. It
    defaults to `InteractiveBrowserCredential`.

- `scopes`: the list of scopes (groups of permissions) to be
    requested. This entry is optional, as most Azure services would re-ask
    for the credentials with the scopes they need.

Every driver may also accept and/or require additional configuration
entries:

#### `interactive_browser`

Launches a browser and lets the use authenticate using her
account. Credentials are cached when possible.

- `client_id` and `client_secret`: are the application credentials
    which must be registered in Azure Active Directory (AAD). See
    [Register
    Application](https://learn.microsoft.com/en-us/azure/healthcare-apis/register-application)
    at MS Learn website.

- `tenant_id`: the tenant where the application has been registered.

- `username`: expected user, optional. Note that when Azure shows the
    login page to the user, it allows her to login with any account
    registered in the tenant AD. When this option is used, the framework
    ensures that the user logs with the expected one. Otherwise it throws
    an error.

- `authentication_callback_port`: The framework starts an HTTP server
    at the given port in order to receive the data from the user browser
    (afterwards it is stopped). The port must be the same used to register
    the application in AAD.


#### `az_cli`

Uses Azure command line client (`az`) for authentication.

Accepted entries are as follows:

- `private` (defaults to true): whether to use a private `az`
    configuration for this login or the global one for the user.

    In the later case, the global configuration must be initialized by
    the user calling `az login`.

