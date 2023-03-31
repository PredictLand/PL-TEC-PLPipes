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
_cred_registry = {}

def _dt(t):
    if t is None:
        return None
    try:
        r = __dt(t)
        logging.debug(f"datetime parsed {t} --> {r}")
        return r
    except:
        logging.exception(f"Unable to parse datetime {t}")

def _cred(account_name):
    if account_name not in _cred_registry:
        _init_cred(account_name)
    return _cred_registry[account_name]

def _init_cred(account_name):
    cfg_path = f"cloud.azure.graph.{account_name}"
    gcfg = cfg.cd(cfg_path)
    creds_account_name = gcfg.setdefault("credentials", account_name)
    _cred_registry[account_name] = plpipes.cloud.azure.auth.credentials(creds_account_name)

def graph(account_name):
    if account_name not in _graph_registry:
        _init_graph(account_name)
    return _graph_registry[account_name]

def _init_graph(account_name):
    from msgraph.core import GraphClient
    graph = GraphClient(credential=_cred(account_name))
    _graph_registry[account_name] = graph

def fs(account_name):
    if account_name not in _fs_registry:
        _init_fs(account_name)
    return _fs_registry[account_name].root()

def _init_fs(account_name):
    _fs_registry[account_name] = _FS(account_name)

class _Node:
    def __init__(self, fs, path):
        self._fs = fs
        self._path = path

    def is_file(self):
        return False

    def is_dir(self):
        return False

    def go(self, path):
        e = self
        parts = [x
                 for x in path.split("/")
                 if x != '']
        for ix, p in enumerate(parts):
            try:
                e = e._go(p)
            except Exception as ex:
                msg = f"Unable to go into {path}"
                logging.exception(msg)
                raise CloudFSError(msg) from ex
        return e

    def __str__(self):
        attr = ", ".join([f"{k}={v}"
                          for k, v in self.__dict__.items()
                          if k not in ('_fs')])
        return f"{type(self).__name__[1:]}({attr})"

    def get(self, path="", **kwargs):
        self.go(path)._get(**kwargs)

    def rget(self, path="", **kwargs):
        self.go(path)._rget(**kwargs)

    def _get(self, **_):
        raise Exception(f"Can't get object {self._path}")

class _FileNode(_Node):
    def is_file(self):
        return True

class _DirNode(_Node):
    def ls(self):
        return {}

    def names(self):
        return list(self.ls().keys())

    def is_dir(self):
        return True

    def _rget(self, dest=None, dir=None, name=None, **kwargs):
        if dest is None:
            if dir is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            dest = pathlib.Path(dir) / name
        else:
            dest = pathlib.Path(dest)
        dest.mkdir(parents=True, exist_ok=True)
        for name, child in self.ls().items():
            child._rget(dir=dest, name=name, **kwargs)

class _SyntheticNode:
    def is_remote(self):
        return False

    def is_synchetic(self):
        return True

class _RemoteNode:
    def _init_remote(self, res, drive=None):
        if res:
            self.id = res["id"]
            self.size = res.get("size", 0)
            self.created = _dt(res.get("createdDateTime"))
            self.modified = _dt(res.get("lastModifiedDateTime"))
            self._res = res
        self._drive = drive
        logging.debug(f"node initialized from {json.dumps(res)}")

    def _is_remote(self):
        return True

    def _is_synthetic(self):
        return False

    def _url(self, path=""):
        drive = self._drive or self
        return drive._mkurl(self.id, path)

    def _child_drive(self):
        return self._drive

    def update(self):
        res = self._fs._get(self._url())
        self._init_remote(res, self._drive)

class _SyntheticDirNode(_DirNode, _SyntheticNode):
    _child_classes = {}

    def ls(self):
        return {k: self._go(k) for k in self._child_classes.keys()}

    def _go(self, name):
        klass = self._child_classes[name]
        return klass(self._fs, self._path / name)

    def _child_drive(self):
        return self

class _RemoteFileNode(_FileNode, _RemoteNode):
    def __init__(self, fs, path, res=None, drive=None):
        super().__init__(fs, path)
        self._init_remote(res, drive)

    def _get_to_file(self, path, force_update=False, **kwargs):
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
        if dest is None:
            if name is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            dest = pathlib.Path(dir) / name
        updated = self._get_to_file(dest, **kwargs)
        msg = f"File {self._path} copied to {dest}"
        if not updated:
            msg += " (cached)"
        logging.info(msg)

    def _rget(self, **kwargs):
        self._get(**kwargs)

class _RemoteDirNode(_DirNode, _RemoteNode):

    def __init__(self, fs, path, res=None, drive=None):
        super().__init__(fs, path)
        self._init_remote(res, drive)

    def _go(self, name):
        return self._res2node(name, self._list_children()[name])

    def ls(self):
        return {name: self._res2node(name, value)
                for name, value in self._list_children().items()}

    def _res2node(self, name, res):
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
        return self._url("/children")

    def _list_children(self):
        r = self._fs._get(self._children_url())
        return {v["name"]: v for v in r["value"]}

class _FolderNode(_RemoteDirNode):
    _child_classes = {}

    def _init_remote(self, res, drive=None):
        super()._init_remote(res, drive)
        if res:
            self.child_count = res.get("folder", {}).get("childCount", 0)

_FolderNode._child_classes['folder'] = _FolderNode
_FolderNode._child_classes['file'] = _RemoteFileNode

class _MeNode(_FolderNode):
    def __init__(self, fs, path):
        super().__init__(fs, path)
        res = self._fs._get("/me/drive/root")
        self._init_remote(res)

    def _children_url(self):
        return "/me/drive/root/children"

    def _mkurl(self, id, path):
        return f"/me/drive/items/{id}{path}"

    def _child_drive(self):
        return self

class _DriveNode(_FolderNode):
    def _mkurl(self, id, path):
        return f"/drives/{self.id}/items/{id}{path}"

class _GroupNode(_FolderNode):
    def _children_url(self):
        return f"/groups/{self.id}/drive/root/children"

    def _mkurl(self, id, path):
        return f"/groups/{self.id}/drive/items/{id}{path}"

    def _child_drive(self):
        return self

class _GroupsNode(_RemoteDirNode):
    _child_classes = {'groupTypes': _GroupNode}

    def _children_url(self):
        return "/groups"

    def _list_children(self):
        r = self._fs._get(self._children_url())
        children = {}
        for v in r["value"]:
            name = v.get("mailNickname")
            if name is None:
                name = v["displayName"]
            children[name] = v
        return children

class _TeamNode(_RemoteDirNode):
    pass

class _TeamsNode(_RemoteDirNode):
    _child_classes = {'id': _TeamNode}

    def _children_url(self):
        return "/teams"

class _DrivesNode(_RemoteDirNode):
    _child_classes = {'folder': _DriveNode}

class _SiteNode(_DirNode):
    _child_classes = {'drives': _DrivesNode}

    def ls(self):
        return {k: self._go(k) for k in self._child_classes.keys()}

    def _child_drive(self):
        return self

class _SitesNode(_RemoteDirNode):
    _child_classes = {'root': _SiteNode}

    def _children_url(self):
        return "/sites"


class _RootNode(_SyntheticDirNode):
    _child_classes = {'me': _MeNode,
                      'sites': _SitesNode,
                      'groups': _GroupsNode}


_transitory_http_codes = {
    httpx.codes.REQUEST_TIMEOUT,
    httpx.codes.TOO_MANY_REQUESTS,
    httpx.codes.INTERNAL_SERVER_ERROR,
    httpx.codes.BAD_GATEWAY,
    httpx.codes.SERVICE_UNAVAILABLE,
    httpx.codes.GATEWAY_TIMEOUT
}

class _FS:
    def __init__(self, account_name):
        self._account_name = account_name
        self._token = _cred(account_name).get_token("https://graph.microsoft.com/.default").token
        self._client = httpx.Client()

    def root(self):
        return _RootNode(self, pathlib.Path("/"))

    def _get(self, url, **kwargs):
        res = self._send_raw('GET', url, **kwargs)
        if res.status_code < 300:
            return res.json()
        raise ValueError(f"Invalid response from server, status code: {res.status_code}")

    def _getd(self, url, **kwargs):
        r = self._get(url, **kwargs)
        print(json.dumps(r, indent=True))

    def _get_to_file(self, url, path, max_retries=None, **kwargs):
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
                if last or ex.response.status_code not in _transitory_http_codes:
                    raise
            except (httpx.RequestError,
                    httpx.StreamError):
                if last:
                    raise

    def _send_raw(self, method, url, headers={},
                  data=None, content=None, timeout=None,
                  max_retries=None, stream=False,
                  accepted_codes=None,
                  follow_redirects=False, **kwargs):
        headers = {**headers, "Authorization": f"Bearer {self._token}"}
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
                logging.warn(f"{msg}, retrying (attempt: {attempt}")
                continue

            if code == 403:
                msg = f"Access to {url} forbidden"
                logging.error(msg)
                raise CloudAccessError(msg)

            logging.error(msg)
            raise CloudFSError(msg)
