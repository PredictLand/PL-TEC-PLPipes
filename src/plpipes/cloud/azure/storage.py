import os
import sys
from pathlib import Path
from azure.storage.blob import BlobServiceClient


sys.path.append(str(Path(os.getcwd()).joinpath("src")))

from plpipes.config import cfg

import plpipes.cloud.azure.auth
from dateutil.parser import isoparse as __dt
import json
import pathlib
import logging
import httpx
import time

from plpipes.exceptions import CloudFSError, CloudAccessError

credentials = plpipes.cloud.azure.auth._cred("predictland")
blob_service_client = BlobServiceClient(account_url="https://developing.blob.core.windows.net/", credentials=credentials)

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
    cfg_path = f"cloud.azure.storage.{account_name}"
    gcfg = cfg.cd(cfg_path)
    creds_account_name = gcfg.setdefault("credentials", account_name)
    _cred_registry[account_name] = plpipes.cloud.azure.auth.credentials(creds_account_name)

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

    def is_synthetic(self):
        return True

# Can be a remote container or a remote blob
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

# Blob
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
                name = pathlib.Path(self._path).name
            if dir is None:
                dest = name
            else:
                dest = pathlib.Path(dir) / name
        dest = pathlib.Path(cfg["fs.work"]) / dest # when relative, use work as the root
        dest.parent.mkdir(parents=True, exist_ok=True)
        updated = self._get_to_file(dest, **kwargs)
        msg = f"File {self._path} copied to {dest}"
        if not updated:
            msg += " (cached)"
        logging.info(msg)

    def _rget(self, **kwargs):
        self._get(**kwargs)

# List of containers, Container
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
                    return klass(self._fs, self._path / name, res)
                except Exception as ex:
                    msg = f"Unable to instantiate object of type {klass}"
                    logging.exception(msg)
                    raise CloudFSError(msg) from ex
        print(json.dumps(res, indent=True))
        raise Exception(f"Unknown remote entry type {json.dumps(res)}")

    def _list_children(self):
        r = self._fs._get(self._children_url())
        return {v["name"]: v for v in r["value"]}



class _Blob(_RemoteFileNode):
    def __init__(self, container_name, blob_name):
        self._fs = fs
        self._path = f"https://predictland.blob.core.windows.net/{container_name}/{blob_name}"
        self._name = blob_name
        self._container_client = blob_service_client.get_container_client(self._name)
    
    def _get(self):
        blob_client = self._container_client.get_blob_client(self._name)
        downloaded_blob = blob_client.download_blob()
        print(f"Downloaded: {downloaded_blob.name}")

class _Container(_RemoteDirNode):
    def __init__(self, path, name):
        self._fs = fs
        self._path = f"https://predictland.blob.core.windows.net/{name}"
        self._name = name
        super().__init__(fs, path)
        res = self._fs._get(self._path)
        self._init_remote(res)
        self._container_client = blob_service_client.get_container_client(self._name)
        _child_classes = {
            'blob': _Blob
        }

    def ls(self):
        blobs_list = self._container_client.list_blobs()
        for blob in blobs_list:
            print(blob.name)
    
    def _rget(self):
        blobs_list = self._container_client.list_blobs()
        for blob in blobs_list:
            downloaded_blob = blob_service_client.download_blob()
            print(f"Downloaded: {blob.name}")

class _ContainersDir(_RemoteDirNode):
    def __init__(self, path, name):
        self._fs = fs
        self._path = f"https://predictland.blob.core.windows.net/?comp=list"
        self._name = name
        super().__init__(fs, path)
        res = self._fs._get(self._path)
        self._init_remote(res)
        self._child_classes = {
            'container': _Container
        }

    def ls(self):
        container_list = blob_service_client.list_container()
        for container in container_list:
            print(container.name)


class _Queue():
    pass

class _QueuesDir(_RemoteDirNode):
    _child_classes = {
        'queue': _Queue
    }


class _File(_RemoteFileNode):
    pass

class _FileShare(_RemoteDirNode):
    _child_classes = {
        'file': _File
    }

class _FileSharesDir(_RemoteDirNode):
    _child_classes = {
        'file_share': _FileShare
    }


class _Table():
    pass

class _TablesDir(_RemoteDirNode):
    _child_classes = {
        'table': _Table
    }


class _Disk():
    pass

class _DisksDir(_RemoteDirNode):
    _child_classes = {
        'disk': _Disk
    }



class _RootNode(_SyntheticDirNode):
    _child_classes = {
        'containers': _ContainersDir,
        'queues': _QueuesDir,
        'fileshares': _FileSharesDir,
        'tables': _TablesDir
    }

    # _child_classes_2 = {
    #     'containers': _RemoteDirNode,
    #     'datalakes': _RemoteDirNode,
    #     'queues': _RemoteDirNode,
    #     'fileshares': _RemoteDirNode,
    #     'tables': _RemoteDirNode,
    #     'disks': _RemoteDirNode
    # }


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
        credentials = _cred(account_name)
        self._session = BlobServiceClient(account_url="https://developing.blob.core.windows.net/", credentials=credentials)
        self._client = httpx.Client()

    def root(self):
        return _RootNode(self, pathlib.Path("/"))
    
    def see_resources(self):
        return {name 
                for name, value in self.root()._child_classes.keys()}

    def _get(self, url, **kwargs):
        res = self._send_raw('GET', url, **kwargs)
        if res.status_code < 300:
            # logging.debug(f"response: {res.text}")
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
            url = f"{_AZURE_STORAGE_URL}{url}"
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
            if code in _transitory_http_codes and attempt < max_retries:
                logging.warn(f"{msg}, retrying (attempt: {attempt}")
                continue

            if code == 403:
                msg = f"Access to {url} forbidden"
                logging.error(msg)
                raise CloudAccessError(msg)

            logging.error(msg)
            raise CloudFSError(msg)
