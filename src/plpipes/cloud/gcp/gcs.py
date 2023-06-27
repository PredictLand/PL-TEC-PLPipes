import os
import sys
from pathlib import Path

sys.path.append(str(Path(os.getcwd()).joinpath("src")))

from plpipes.config import cfg

import plpipes.cloud.gcp.auth
from dateutil.parser import isoparse as __dt
import json
import pathlib
import logging
import httpx
import time

from plpipes.exceptions import CloudFSError, CloudAccessError

_fs_registry = {}
_cred_registry = {}
_gcs_registry = {}

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
    cfg_path = f"cloud.gcp.gcs.{account_name}"
    gcfg = cfg.cd(cfg_path)
    creds_account_name = gcfg.setdefault("credentials", account_name)
    _cred_registry[account_name] = plpipes.cloud.gcp.auth.credentials(creds_account_name)

def fs(account_name):
    if account_name not in _fs_registry:
        _init_fs(account_name)
    return _fs_registry[account_name].root()

def _init_fs(account_name):
    _fs_registry[account_name] = _FS(account_name)

def s3(account_name):
    if account_name not in _gcs_registry:
        _init_s3(account_name)
    return _gcs_registry[account_name]

def _init_s3(account_name):
    credential=_cred(account_name)
    _gcs_registry[account_name] = s3

class _Node:
    def __init__(self, fs, path, account_name):
        self._fs = fs
        self._path = path
        self._session = _cred(account_name).client("s3")

    def is_file(self):
        return False

    def is_dir(self):
        return False
    
    def is_bucket(self):
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


class _RemoteBucketNode(_Node):
    def __init__(self, path, name):
        self._fs = fs
        self._path = path
        self._name = name
        self._child_classes = {}

    def is_bucket(self):
        return True
    
    def is_remote(self):
        return True
    
    def list_objects(self):
        response = s3.list_objects_v2(Bucket = self._name)
        for object in response["Content"]:
            print(object["Key"])
        return response["Content"]
    
    def _go():
        pass

    def _get(self, object_name, file_name):
        s3.download_file(self.name, object_name, file_name)
        print(f"Downloaded: {object_name}")

    def _rget(self):
        response = self.list_objects()
        for object in response:
            key = object['Key']
            file_name = 'DESTINATION_FOLDER' + key
            s3.download_file(self._name, key, file_name)
            print(f"Downloaded: {key}")
    
    def move_object_between_buckets(self):
        pass

    def upload_file(self, file_name):
        s3.upload_file(file_name=file_name, bucket=self._name)

class _RemoteBucketsDirNode():
    _child_classes = {'buckets': _RemoteBucketNode}
    def list_buckets():
        response = s3.list_buckets()
        print("Buckets in your S3 subscription: ")
        for bucket in response['Buckets']:
            print(f"{bucket['Name']}")
        return response["Buckets"]


class _RootNode():
    _child_classes = {'buckets': _RemoteBucketsDirNode}
    

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
        self._session = _cred(account_name).client("s3")
        self._client = httpx.Client()

    def root(self):
        return _RootNode(self, pathlib.Path("/"))
    

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
        if data is not None:
            content = json.dumps(data)
            headers["Content-Type"] = "application/json"

        if timeout is None:
            timeout = cfg.setdefault("net.http.timeout", 30)
        if max_retries is None:
            max_retries = cfg.setdefault("net.http.max_retries", 5)

        req = self._client.build_request(method, url, 
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
