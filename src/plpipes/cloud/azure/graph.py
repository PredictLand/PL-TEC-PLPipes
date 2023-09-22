from plpipes.config import cfg

import plpipes.cloud.azure.auth
from dateutil.parser import isoparse as __dt
import json
import pathlib
import logging
import httpx
import os
import time

from plpipes.exceptions import CloudFSError, CloudAccessError

_GRAPH_URL = "https://graph.microsoft.com/v1.0"

_TRANSITORY_HTTP_CODES = {
    httpx.codes.REQUEST_TIMEOUT,
    httpx.codes.TOO_MANY_REQUESTS,
    httpx.codes.INTERNAL_SERVER_ERROR,
    httpx.codes.BAD_GATEWAY,
    httpx.codes.SERVICE_UNAVAILABLE,
    httpx.codes.GATEWAY_TIMEOUT
}

_graph_registry = {}
_fs_registry = {}

def _dt(t):
    """Parse a datetime string into a datetime object.

    Args:
        t (str): The datetime string to parse.

    Returns:
        datetime: Parsed datetime object or None if input is None.
    """
    if t is None:
        return None
    try:
        r = __dt(t)
        logging.debug(f"datetime parsed {t} --> {r}")
        return r
    except Exception:
        logging.exception(f"Unable to parse datetime {t}")

def _cred(account_name):
    """Retrieve the credentials for the specified account name.

    Args:
        account_name (str): The name of the account.

    Returns:
        Credential: Credentials object for the account.
    """
    creds_account_name = cfg.setdefault(f"cloud.azure.graph.{account_name}.credentials",
                                        account_name)
    return plpipes.cloud.azure.auth.credentials(creds_account_name)

def graph(account_name):
    """Get the GraphClient object for the specified account name.

    Args:
        account_name (str): The name of the account.

    Returns:
        GraphClient: Instance of GraphClient for the specified account.
    """
    if account_name not in _graph_registry:
        _init_graph(account_name)
    return _graph_registry[account_name]

def _init_graph(account_name):
    """Initialize the GraphClient for the specified account name.

    Args:
        account_name (str): The name of the account.
    """
    from msgraph.core import GraphClient
    graph = GraphClient(credential=_cred(account_name))
    _graph_registry[account_name] = graph

def fs(account_name):
    """Get the file system object for the specified account name.

    Args:
        account_name (str): The name of the account.

    Returns:
        _FS: Instance of the file system object for the specified account.
    """
    if account_name not in _fs_registry:
        _init_fs(account_name)
    return _fs_registry[account_name].root()

def _init_fs(account_name):
    """Initialize the file system for the specified account name.

    Args:
        account_name (str): The name of the account.
    """
    _fs_registry[account_name] = _FS(account_name)

class _Node:
    """Base class for file system nodes."""

    def __init__(self, fs, path):
        """Initialize a file system node.

        Args:
            fs (_FS): The file system object.
            path (Path): The path of the node.
        """
        self._fs = fs
        self._path = path

    def is_file(self, path=""):
        """Check if the given path is a file.

        Args:
            path (str): The path to check.

        Returns:
            bool: True if it is a file, False otherwise.
        """
        e = self.go(path, missing_ok=True)
        return False if e is None else e._is_file()

    def is_dir(self, path=""):
        """Check if the given path is a directory.

        Args:
            path (str): The path to check.

        Returns:
            bool: True if it is a directory, False otherwise.
        """
        e = self.go(path, missing_ok=True)
        return False if e is None else e._is_dir()

    def __is_file(self):
        """Check if this node is a file.

        Returns:
            bool: False by default.
        """
        return False

    def _is_dir(self):
        """Check if this node is a directory.

        Returns:
            bool: False by default.
        """
        return False

    def go(self, path, missing_ok=False):
        """Navigate to a child node based on the given path.

        Args:
            path (str): The path to navigate to.
            missing_ok (bool): If True, return None if the path does not exist.

        Returns:
            _Node: The resulting node after navigating.
        """
        if path is None or path == "":
            return self

        e = self
        parts = [x for x in path.split("/") if x != '']
        for ix, p in enumerate(parts):
            try:
                e = e._go(p)
            except Exception as ex:
                if missing_ok:
                    return None
                msg = f"Unable to go into {path}"
                logging.exception(msg)
                raise CloudFSError(msg) from ex
        return e

    def __str__(self):
        """String representation of the node.

        Returns:
            str: The string representation of the node.
        """
        attr = ", ".join([f"{k}={v}" for k, v in self.__dict__.items() if k not in ('_fs')])
        return f"{type(self).__name__[1:]}({attr})"

    def get(self, path="", **kwargs):
        """Retrieve the resource at the current node.

        Args:
            path (str): The path to the resource.
            **kwargs: Additional arguments for the get operation.
        """
        self.go(path)._get(**kwargs)

    def rget(self, path="", **kwargs):
        """Recursively retrieve resources from the current node.

        Args:
            path (str): The path to retrieve.
            **kwargs: Additional arguments for the rget operation.
        """
        self.go(path)._rget(**kwargs)

    def _get(self, **_):
        """Abstract method to get the object data, raises an exception by default.

        Raises:
            Exception: Indicates that this operation is not supported.
        """
        raise Exception(f"Can't get object {self._path}")

class _FileNode(_Node):
    """Class representing a file node."""

    def _is_file(self):
        """Indicate that this is a file node.

        Returns:
            bool: True for file nodes.
        """
        return True

class _DirNode(_Node):
    """Class representing a directory node."""

    def ls(self):
        """List the contents of the directory.

        Returns:
            dict: A dictionary of file-name and entry pairs.
        """
        return {}

    def names(self):
        """Get the names of the contents in the directory.

        Returns:
            list: A list of names in the directory.
        """
        return list(self.ls().keys())

    def _is_dir(self):
        """Indicate that this is a directory node.

        Returns:
            bool: True for directory nodes.
        """
        return True

    def _rget(self, dest=None, dir=None, name=None, **kwargs):
        """Recursively retrieve resources from the directory.

        Args:
            dest (Path): Destination path for the retrieved resources.
            dir (Path): Directory path for the retrieval.
            name (str): Name of the resource.
            **kwargs: Additional arguments for the retrieval operation.
        """
        if dest is None:
            if name is None:
                name = pathlib.Path(self._path).name
            if dir is None:
                dest = name
            else:
                dest = pathlib.Path(dir) / name
        dest = pathlib.Path(cfg["fs.work"]) / dest
        dest.mkdir(parents=True, exist_ok=True)
        for name, child in self.ls().items():
            child._rget(dir=dest, name=name, **kwargs)

class _SyntheticNode:
    """Class representing a synthetic node."""

    def is_remote(self):
        """Check if the node is remote.

        Returns:
            bool: False, as it is synthetic.
        """
        return False

    def is_synchetic(self):
        """Check if this node is synthetic.

        Returns:
            bool: True, since it is synthetic.
        """
        return True

class _RemoteNode:
    """Class representing a remote node."""

    def _init_remote(self, res, drive=None):
        """Initialize the remote node with resource data.

        Args:
            res (dict): Resource data to initialize the node.
            drive (_DriveNode): The parent drive node.
        """
        if res:
            self.id = res["id"]
            self.size = res.get("size", 0)
            self.created = _dt(res.get("createdDateTime"))
            self.modified = _dt(res.get("lastModifiedDateTime"))
            self._res = res
        self._drive = drive
        logging.debug(f"node initialized from {json.dumps(res)}")

    def _is_remote(self):
        """Indicate that this is a remote node.

        Returns:
            bool: True for remote nodes.
        """
        return True

    def _is_synthetic(self):
        """Indicate that this is not a synthetic node.

        Returns:
            bool: False for remote nodes.
        """
        return False

    def _url(self, path=""):
        """Get the URL for the remote resource.

        Args:
            path (str): Path to append to the URL.

        Returns:
            str: Constructed URL for the resource.
        """
        drive = self._drive or self
        return drive._mkurl(self.id, path)

    def _child_drive(self):
        """Get the child drive node.

        Returns:
            _DriveNode: The child drive node.
        """
        return self._drive

    def update(self):
        """Update the remote node's resource data by fetching it again."""
        res = self._fs._get(self._url())
        self._init_remote(res, self._drive)

class _SyntheticDirNode(_DirNode, _SyntheticNode):
    """Class representing a synthetic directory node."""
    _child_classes = {}

    def ls(self):
        """List the entries in the synthetic directory.

        Returns:
            dict: Entries in the synthetic directory.
        """
        entries = {}
        for k in self._child_classes.keys():
            try:
                entries[k] = self._go(k)
            except Exception:
                logging.exception(f"Unable to get data for entry {k} in synthetic dir")
        return entries

    def _go(self, name):
        """Navigate to a child node by its name in synthetic directory.

        Args:
            name (str): Name of the child node.

        Returns:
            _Node: The child node.
        """
        klass = self._child_classes[name]
        return klass(self._fs, self._path / name)

    def _child_drive(self):
        """Get the child drive node for the synthetic directory.

        Returns:
            _SyntheticDirNode: The synthetic directory itself.
        """
        return self

class _RemoteFileNode(_FileNode, _RemoteNode):
    """Class representing a remote file node."""

    def __init__(self, fs, path, res=None, drive=None):
        """Initialize a remote file node.

        Args:
            fs (_FS): The file system object.
            path (Path): The path of the remote file.
            res (dict): The resource data for the file.
            drive (_DriveNode): The parent drive node.
        """
        super().__init__(fs, path)
        self._init_remote(res, drive)

    def _get_to_file(self, path, force_update=False, **kwargs):
        """Download the content of the remote file to a local file.

        Args:
            path (Path): The local destination path.
            force_update (bool): If True, force update the file despite its status.
            **kwargs: Additional arguments for the download operation.

        Returns:
            bool: True if the file was downloaded, False if it was cached.
        """
        self.update()
        if (not force_update and path.is_file()):
            st = path.stat()
            if st.st_mtime >= self.modified.timestamp() and \
               st.st_size == self.size:
                return False
        self._fs._get_to_file(self._url("/content"), path, follow_redirects=True, **kwargs)
        os.utime(path, (time.time(), self.modified.timestamp()))
        return True

    def _get(self, dest=None, dir=None, name=None, **kwargs):
        """Retrieve the remote file and store it to a specified location.

        Args:
            dest (Path): The destination path to save the file.
            dir (Path): The directory path for the file.
            name (str): Name of the file to save as.
            **kwargs: Additional arguments for the retrieval operation.
        """
        if dest is None:
            if name is None:
                name = pathlib.Path(self._path).name
            if dir is None:
                dest = name
            else:
                dest = pathlib.Path(dir) / name
        dest = pathlib.Path(cfg["fs.work"]) / dest  # when relative, use work as the root
        dest.parent.mkdir(parents=True, exist_ok=True)
        updated = self._get_to_file(dest, **kwargs)
        msg = f"File {self._path} copied to {dest}"
        if not updated:
            msg += " (cached)"
        logging.info(msg)

    def _rget(self, **kwargs):
        """Recursively retrieve the file, which is the same operation as get here.

        Args:
            **kwargs: Additional arguments for the retrieval operation.
        """
        self._get(**kwargs)

class _RemoteDirNode(_DirNode, _RemoteNode):
    """Class representing a remote directory node."""

    def __init__(self, fs, path, res=None, drive=None):
        """Initialize a remote directory node.

        Args:
            fs (_FS): The file system object.
            path (Path): The path of the remote directory.
            res (dict): The resource data for the directory.
            drive (_DriveNode): The parent drive node.
        """
        super().__init__(fs, path)
        self._init_remote(res, drive)

    def _go(self, name):
        """Navigate to a child node by its name in remote directory.

        Args:
            name (str): Name of the child node.

        Returns:
            _Node: The child node.
        """
        return self._res2node(name, self._list_children()[name])

    def ls(self):
        """List the entries in the remote directory.

        Returns:
            dict: A dictionary of file-name and entry pairs.
        """
        return {name: self._res2node(name, value)
                for name, value in self._list_children().items()}

    def _res2node(self, name, res):
        """Convert a resource dictionary to a corresponding node.

        Args:
            name (str): Name of the resource.
            res (dict): Resource data.

        Returns:
            _Node: The resulting node based on the resource type.

        Raises:
            CloudFSError: If the resource type is unknown.
        """
        for k, klass in self._child_classes.items():
            if k in res:
                try:
                    return klass(self._fs, self._path / name, res, self._child_drive())
                except Exception as ex:
                    msg = f"Unable to instantiate object of type {klass}"
                    logging.exception(msg)
                    raise CloudFSError(msg) from ex
        print(json.dumps(res, indent=True))
        raise Exception(f"Unknown remote entry type {json.dumps(res)}")

    def _children_url(self):
        """Get the URL to list the children of the remote directory.

        Returns:
            str: The URL for the children of the directory.
        """
        return self._url("/children")

    def _list_children(self):
        """List the children resources in the remote directory.

        Returns:
            dict: A dictionary of children resources.
        """
        r = self._fs._get(self._children_url())
        return {v["name"]: v for v in r["value"]}

class _FolderNode(_RemoteDirNode):
    """Class representing a folder node in a remote directory."""
    _child_classes = {}

    def _init_remote(self, res, drive=None):
        """Initialize the folder node with resource data.

        Args:
            res (dict): Resource data to initialize the node.
            drive (_DriveNode): The parent drive node.
        """
        super()._init_remote(res, drive)
        if res:
            self.child_count = res.get("folder", {}).get("childCount", 0)

_FolderNode._child_classes['folder'] = _FolderNode
_FolderNode._child_classes['file'] = _RemoteFileNode

class _MeNode(_FolderNode):
    """Class representing the 'me' node in the OneDrive."""

    def __init__(self, fs, path):
        """Initialize the me node.

        Args:
            fs (_FS): The file system object.
            path (Path): The path of the me node.
        """
        super().__init__(fs, path)
        res = self._fs._get("/me/drive/root")
        self._init_remote(res)

    def _children_url(self):
        """Get the URL to list the children of the me node.

        Returns:
            str: The URL for the children of the me node.
        """
        return "/me/drive/root/children"

    def _mkurl(self, id, path):
        """Construct the URL for a resource in the me node.

        Args:
            id (str): The ID of the resource.
            path (str): Additional path to append.

        Returns:
            str: The constructed URL for the resource.
        """
        return f"/me/drive/items/{id}{path}"

    def _child_drive(self):
        """Get the child drive node for the me node.

        Returns:
            _MeNode: The me node itself.
        """
        return self

class _DriveNode(_FolderNode):
    """Class representing a drive node."""

    def _mkurl(self, id, path):
        """Construct the URL for a resource in the drive node.

        Args:
            id (str): The ID of the resource.
            path (str): Additional path to append.

        Returns:
            str: The constructed URL for the resource.
        """
        return f"/drives/{self.id}/items/{id}{path}"

class _GroupNode(_FolderNode):
    """Class representing a group node in the directory."""

    def _children_url(self):
        """Get the URL to list the children of the group node.

        Returns:
            str: The URL for the children of the group node.
        """
        return f"/groups/{self.id}/drive/root/children"

    def _mkurl(self, id, path):
        """Construct the URL for a resource in the group node.

        Args:
            id (str): The ID of the resource.
            path (str): Additional path to append.

        Returns:
            str: The constructed URL for the resource.
        """
        return f"/groups/{self.id}/drive/items/{id}{path}"

    def _child_drive(self):
        """Get the child drive node for the group node.

        Returns:
            _GroupNode: The group node itself.
        """
        return self

class _GroupsNode(_RemoteDirNode):
    """Class representing the groups node in the directory."""
    _child_classes = {'groupTypes': _GroupNode}

    def _children_url(self):
        """Get the URL to list the children of the groups node.

        Returns:
            str: The URL for the children of the groups node.
        """
        return "/groups"

    def _list_children(self):
        """List the children resources in the groups node.

        Returns:
            dict: A dictionary of children resources.
        """
        r = self._fs._get(self._children_url())
        children = {}
        for v in r["value"]:
            name = v.get("mailNickname")
            if name is None:
                name = v["displayName"]
            children[name] = v
        return children

class _TeamNode(_RemoteDirNode):
    """Class representing a team node."""

    pass

class _TeamsNode(_RemoteDirNode):
    """Class representing the teams node in the directory."""
    _child_classes = {'id': _TeamNode}

    def _children_url(self):
        """Get the URL to list the children of the teams node.

        Returns:
            str: The URL for the children of the teams node.
        """
        return "/teams"

class _DrivesNode(_RemoteDirNode):
    """Class representing the drives node in the directory."""
    _child_classes = {'folder': _DriveNode}

class _SiteNode(_DirNode):
    """Class representing a site node in the directory."""
    _child_classes = {'drives': _DrivesNode}

    def ls(self):
        """List the contents of the site node.

        Returns:
            dict: A dictionary of file-name and entry pairs.
        """
        return {k: self._go(k) for k in self._child_classes.keys()}

    def _child_drive(self):
        """Get the child drive node for the site node.

        Returns:
            _SiteNode: The site node itself.
        """
        return self

class _SitesNode(_RemoteDirNode):
    """Class representing the sites node in the directory."""
    _child_classes = {'root': _SiteNode}

    def _children_url(self):
        """Get the URL to list the children of the sites node.

        Returns:
            str: The URL for the children of the sites node.
        """
        return "/sites"

class _RootNode(_SyntheticDirNode):
    """Class representing the root node of the file system."""
    _child_classes = {'me': _MeNode,
                      'sites': _SitesNode,
                      'groups': _GroupsNode}

class _FS:
    """Class representing the file system for a specific account."""

    def __init__(self, account_name):
        """Initialize the file system with the account name.

        Args:
            account_name (str): The name of the account.
        """
        self._account_name = account_name
        self._cred = _cred(account_name)
        self._token = None
        self._get_token()  # init token!
        self._client = httpx.Client()

    def _get_token(self):
        """Retrieve the access token for the account.

        Returns:
            str: The access token.
        """
        if (self._token is None) or (self._token.expires_on - time.time() < 60):
            self._token = self._cred.get_token("https://graph.microsoft.com/.default")
        return self._token.token

    def root(self):
        """Get the root node of the file system.

        Returns:
            _RootNode: The root node of the file system.
        """
        return _RootNode(self, pathlib.Path("/"))

    def _get(self, url, **kwargs):
        """Send a GET request to the specified URL.

        Args:
            url (str): The URL to send the request to.
            **kwargs: Additional arguments for the request.

        Returns:
            dict: The JSON response from the server.

        Raises:
            ValueError: If the response status code indicates an error.
        """
        res = self._send_raw('GET', url, **kwargs)
        if res.status_code < 300:
            return res.json()
        raise ValueError(f"Invalid response from server, status code: {res.status_code}")

    def _getd(self, url, **kwargs):
        """Debug method to get the response data in a pretty-printed format.

        Args:
            url (str): The URL to fetch data from.
            **kwargs: Additional arguments for the request.
        """
        r = self._get(url, **kwargs)
        print(json.dumps(r, indent=True))

    def _get_to_file(self, url, path, max_retries=None, **kwargs):
        """Download content from the specified URL to a local file.

        Args:
            url (str): The URL to download from.
            path (Path): The local path to save the file.
            max_retries (int): Maximum number of retries for the download.
            **kwargs: Additional arguments for the download operation.

        Returns:
            bool: True if the file was downloaded, False if it was cached.
        """
        if max_retries is None:
            max_retries = cfg.setdefault("net.http.max_retries", 5)
        for i in range(max_retries):
            last = i + 1 >= max_retries
            if i:
                delay = cfg.setdefault("net.http.retry_delay", 2)
                time.sleep(delay)
            try:
                res = self._send_raw('GET', url, stream=True, max_retries=1, **kwargs)
                logging.debug(f"copying response body from {res}")
                with open(path, "wb") as f:
                    for chunk in res.iter_bytes():
                        if len(chunk) > 0:
                            f.write(chunk)
                return True
            except httpx.HTTPStatusError as ex:
                if last or ex.response.status_code not in _TRANSITORY_HTTP_CODES:
                    raise
            except (httpx.RequestError,
                    httpx.StreamError):
                if last:
                    raise

    def _send_raw(self, method, url, headers={}, data=None, content=None, timeout=None,
                  max_retries=None, stream=False, accepted_codes=None,
                  follow_redirects=False, **kwargs):
        """Send a raw HTTP request.

        Args:
            method (str): The HTTP method (e.g., 'GET', 'POST').
            url (str): The URL to send the request to.
            headers (dict): HTTP headers to include in the request.
            data (dict): Data to include for POST requests.
            content (str): Content to send with the request.
            timeout (int): Request timeout in seconds.
            max_retries (int): Maximum number of retries for the request.
            stream (bool): Whether to stream the response.
            accepted_codes (set): Set of accepted HTTP status codes.
            follow_redirects (bool): Whether to follow redirects.
            **kwargs: Additional arguments for the request.

        Returns:
            httpx.Response: The HTTP response object.

        Raises:
            CloudFSError: If the HTTP request fails or returns an error code.
        """
        headers = {**headers, "Authorization": f"Bearer {self._get_token()}"}
        if url.startswith("/"):
            url = f"{_GRAPH_URL}{url}"
        if data is not None:
            content = json.dumps(data)
            headers["Content-Type"] = "application/json"

        if timeout is None:
            timeout = cfg.setdefault("net.http.timeout", 30)
        if max_retries is None:
            max_retries = cfg.setdefault("net.http.max_retries", 5)

        req = self._client.build_request(method, url, headers=headers,
                                         content=content, timeout=timeout,
                                         **kwargs)

        res = None
        attempt = 0
        while True:
            attempt += 1
            if attempt > 1:
                delay = cfg.setdefault("net.http.retry_delay", 2)
                time.sleep(delay)
            try:
                res = self._client.send(req, stream=stream, follow_redirects=follow_redirects)
            except httpx.RequestError as ex:
                if attempt < max_retries:
                    continue
                raise CloudFSError(f"HTTP call {method} {url} failed") from ex

            code = res.status_code
            if accepted_codes is None:
                if code < 300:
                    return res
            else:
                if code in accepted_codes:
                    return res

            msg = f"HTTP call {method} {url} failed with code {code}"
            if code in _TRANSITORY_HTTP_CODES and attempt < max_retries:
                logging.warn(f"{msg}, retrying (attempt: {attempt})")
                continue

            if code == 403:
                msg = f"Access to {url} forbidden"
                logging.error(msg)
                raise CloudAccessError(msg)

            logging.error(msg)
            raise CloudFSError(msg)
