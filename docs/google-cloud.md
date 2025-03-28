# Google Cloud

*Note: This is a work in progress*

## Authentication

### API

Credential objects of type `google.auth.credentials.Credentials` can
be retrieved using function `credentials` as follows:

```python
import plpipes.cloud.cloud.auth
cred = plpipes.cloud.cloud.auth.credentials("predictland")
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
        type: oauth2
        scopes:
          - "https://www.googleapis.com/auth/cloud-platform"
        ...
```

The meaning of every key is as follows:

- `type`: name of the authentication backend.
- `scopes`: list of scope for which access is being requested.

Every backend requires a different set of additional options:

#### `oauth2`

- `installed`: the additional entries required by
    `google_auth_oauthlib.flow.InstalledAppFlow.from_client_config`:

    - `client_id`
    - `project_id`
    - `auth_uri`
    - `token_uri`,
    - `auth_provider_x509_cert_url`
    - `client_secret`
    - `redirect_uris`

    Those options can be retrieved from the JSON file generated by
    GoogleCloud when a new OAuth2 installed applications is registered
    ([GoogleCloud Console](https://console.cloud.google.com/welcome) →
    APIs & Services → Credentials → Create Credentials → OAuth Client ID →
    Desktop App → Download JSON).

Example:

```yaml
google:
  auth:
    predictland:
      type: oauth2
      installed:
        client_id: "..."
        project_id: "predictland"
        auth_uri: "https://accounts.google.com/o/oauth2/auth"
        token_uri: "https://oauth2.googleapis.com/token"
        auth_provider_x509_cert_url: "https://www.googleapis.com/oauth2/v1/certs"
        client_secret: "..."
        redirect_uris: ["http://localhost"]
      scopes:
        - https://www.googleapis.com/auth/cloud-platform
```
