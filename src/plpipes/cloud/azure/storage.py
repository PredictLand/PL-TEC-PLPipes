import os
import sys
from pathlib import Path
from azure.storage.blob import BlobServiceClient
from azure.storage.fileshare import ShareClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.subscription import SubscriptionClient

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
from itertools import tee

_fs_registry = {}
_cred_registry = {}
_subscriptions_registry = {}

def find_key_by_value(dictionary, value):
    for key, val in dictionary.items():
        if val == value:
            return key
    return None

def extract_storage_element_name(path, storage_element):
    components = path.split("\\")
    extracted_string = None
    for i in range(len(components)):
        if (((storage_element == "fileshare" and components[i] == "fileshares") or
        (storage_element == "container" and components[i] == "containers")) and 
        (i + 1 < len(components))):
            extracted_string = components[i + 1]
            break
    return extracted_string

def create_directories_from_string(string):
    directories = string.split('/')
    current_directory = ''
    for directory in directories[:-1]:
        current_directory = os.path.join(current_directory, directory)
        if not os.path.exists(current_directory):
            os.mkdir(current_directory)
        os.chdir(current_directory)

def get_parent_file(path):
    components = path.split("\\")
    return '/'.join(components[3:])

def characters_after_last_slash(input_string):
    return input_string.rsplit("\\", 1)[-1]

def get_dest(dest, name, dir, object_path):
    if dest is None:
        if name is None:
            name = pathlib.Path(object_path).name
        if dir is None:
            dest = name
        else:
            dest = pathlib.Path(dir) / name
    return dest

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
    

def fs(account_name, subscription_name, storage_account_name, resource_group_name):
    if account_name not in _fs_registry:
        _init_fs(account_name, subscription_name, storage_account_name, resource_group_name)
    return _fs_registry[account_name].root()

def _init_fs(account_name, subscription_name, storage_account_name, resource_group_name):
    _fs_registry[account_name] = _FS(account_name, subscription_name, storage_account_name, resource_group_name)

def show_subscriptions(account_name):
    credentials = _cred(account_name)
    try:
        subscription_client = SubscriptionClient(credentials)
        subscriptions = subscription_client.subscriptions.list()

    except Exception as e:
        print(f"An error occurred while creating the subscription client: {e}")
    
    print(f"These are the subscriptions available with your user {account_name}")

    for i, subscription in enumerate(subscriptions, start = 1):
        print(f"---------------Subscription {i}---------------")
        print(f"Subscription ID: {subscription.subscription_id}")
        print(f"Subscription Name: {subscription.display_name}")
        print()
        _subscriptions_registry[subscription.display_name] = subscription.subscription_id

def show_resource_groups(account_name, subscription_name):
    credentials = _cred(account_name)
    try:
        resourceClient = ResourceManagementClient(credentials, _subscriptions_registry[subscription_name])
        resource_groups = resourceClient.resource_groups.list()

    except Exception as e:
        print(f"An error occurred while creating the resource client: {e}")

    print(f"These are the resources groups available in the subscription {subscription_name}")
    for groups in resource_groups.by_page():
        for i, group in enumerate(groups, start = 1):
            print(f"---------------Resource group {i}---------------")
            print(f"Resource group name: {group.name}")
            print()
        

def show_storage_accounts(account_name, resource_group_name, subscription_name):
    credentials = _cred(account_name)
    try:
        storage_client = StorageManagementClient(credentials, _subscriptions_registry[subscription_name])
        storage_accounts = list(storage_client.storage_accounts.list_by_resource_group(resource_group_name))
    
    except Exception as e:
        print(f"An error occurred while creating the storage client: {e}")
    

    print(f"These are the storage accounts available with your user {account_name} within the subscription {subscription_name} and the resource group {resource_group_name}")
    
    for i, account in enumerate(storage_accounts, start = 1):
        print(f"---------------Storage Account {i} from subscription {subscription_name} and resource group {resource_group_name}---------------")
        print(f"Storage Account Name: {account.name}")
        print()


class _Node:
    def __init__(self, fs, path):
        self._fs = fs
        self._path = path
        self._childs = {}
        

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
    def __init__(self, fs, path):
        pass

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

    def ls(self):
        return {k: self._go(k) for k in self._child_classes.keys()}

    def _go(self, name):

        if type(self) == _ContainersDir:
            klass = self._containers[name]
            return klass(self._fs, self._path / name, find_key_by_value(self._containers, self._containers[name]))
        
        elif type(self) == _Container:
            print(self._blobs)
            klass = self._blobs[name]
            return klass(self._fs, self._path / name, extract_storage_element_name(str(self._path), "container"), name)

        elif type(self) == _Blob:
            self._child_classes[name] = _Blob
            klass = self._child_classes[name]
            return klass(self._fs, self._path, extract_storage_element_name(str(self._path), "container"), get_parent_file(str(self._path)))
        

        elif type(self) == _FileSharesDir:
            klass = self._fileshares[name]
            return klass(self._fs, self._path / name, find_key_by_value(self._fileshares, self._fileshares[name]))

        elif type(self) == _FileShare:
            klass = self._files[name]
            return klass(self._fs, self._path / name, name, "")
        
        elif type(self) == _File:
            self._child_classes[name] = _File
            klass = self._child_classes[name]
            return klass(self._fs, self._path / name, extract_storage_element_name(str(self._path), "fileshare"), get_parent_file(str(self._path)) + "/" + name)
        

        else:
            klass = self._child_classes[name]
            return klass(self._fs, self._path / name)

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

        dest = pathlib.Path(cfg["fs.work"]) / get_dest(dest, name, dir, self._path) 
        dest.parent.mkdir(parents=True, exist_ok=True)

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


class _Blob(_SyntheticDirNode):
    def __init__(self, fs, path, container_name, blob_name):
        self._fs = fs
        self._path = path
        self._container_name = container_name
        self._blob_name = blob_name
        self._BlobServiceClient = BlobServiceClient(account_url=f"https://{self._fs._storage_account_name}.blob.core.windows.net", credential=self._fs._credentials)
        self._BlobClient = self._BlobServiceClient.get_blob_client(container = self._container_name, blob = self._blob_name)
        self._child_classes = {}

    def _get(self, dest=None, dir=None, name=None, **kwargs):

        
        if dest is None:
            if dir is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            container_dest = pathlib.Path(dir) / name
        else:
            container_dest = pathlib.Path(dest)
       
        container_dest.parent.mkdir(parents=True, exist_ok=True)
        
        
        try:
            with open(container_dest, "wb") as blob:
                download_stream = self._BlobClient.download_blob()
                blob.write(download_stream.readall())
                print(f"Downloaded: {name}")
        except Exception as e:
            print(f"An error occurred while downloading the blob: {e}")



class _Container(_SyntheticDirNode):
    
    def __init__(self, fs, path, name):
        self._path = path
        self._name = name
        self._fs = fs
        self._BlobClient = BlobServiceClient(account_url=f"https://{self._fs._storage_account_name}.blob.core.windows.net", credential=self._fs._credentials)
        self._ContainerClient = self._BlobClient.get_container_client(self._name)
        self._Blobs_List = self._ContainerClient.list_blobs()
        self._blobs = {}

    def ls(self):
        for blob in self._Blobs_List:
            print(blob.name)
            self._blobs[blob.name] = _Blob
    
    def _rget(self, dest=None, dir=None, name=None, **kwargs):

        if dest is None:
            if dir is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            container_dest = pathlib.Path(dir) / name
        else:
            container_dest = pathlib.Path(dest)
       
        container_dest.parent.mkdir(parents=True, exist_ok=True)

        

        try:
            for blob in self._Blobs_List:
                # file in the first level of the container
                if '/' not in blob.name and '.' in blob.name:
                    if not os.path.exists(container_dest):
                        os.mkdir(container_dest)
                    os.chdir(container_dest)
                    with open(blob.name, "wb") as file:
                        blob_client = self._BlobClient.get_blob_client(container = self._name, blob = blob.name)
                        downloaded_blob = blob_client.download_blob()
                        file.write(downloaded_blob.readall())
                        print(f"Downloaded: {blob.name}")

                # file not in the first level of the container
                if '/' in blob.name:
                    # enter in the container directory
                    if not os.path.exists(container_dest):
                        os.mkdir(container_dest)
                    os.chdir(container_dest)

                    create_directories_from_string(blob.name)
                    
                    with open(blob.name.rsplit('/', 1)[1], "wb") as file:
                        blob_client = self._BlobClient.get_blob_client(container = self._name, blob = blob.name)
                        downloaded_blob = blob_client.download_blob()
                        file.write(downloaded_blob.readall())
                        print(f"Downloaded: {blob.name}")

                
        except Exception as e:
            print(f"An error occurred while downloading the blob: {e}")


class _ContainersDir(_SyntheticDirNode):
    
    def __init__(self, fs, path):
        self._fs = fs
        self._path = path
        self._BlobClient = BlobServiceClient(account_url=f"https://{self._fs._storage_account_name}.blob.core.windows.net", credential=self._fs._credentials)
        self._Containers_List = self._BlobClient.list_containers()
        self._containers = {}

    def ls(self):
        for container in self._Containers_List:
            container_name = container.name
            print(container_name)
            self._containers[container_name] = _Container


class _Queue(_SyntheticDirNode):
    pass

class _QueuesDir(_SyntheticDirNode):
    _child_classes = {
        'queue': _Queue
    }


class _File(_SyntheticDirNode):
    def __init__(self, fs, path, fileshare_name, file_name):
        self._fs = fs
        self._path = path
        self._fileshare_name = fileshare_name
        self._file_name = file_name
        self._ShareClient = ShareClient(account_url=f"https://{self._fs._storage_account_name}.file.core.windows.net", credential=self._fs._credentials, token_intent = "backup", share_name = self._fileshare_name)
        self._FileClient = self._ShareClient.get_file_client(self._file_name)
        self._child_classes = {}
    
    def _get(self, dest=None, dir=None, name=None, **kwargs):

        
        if dest is None:
            if dir is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            container_dest = pathlib.Path(dir) / name
        else:
            container_dest = pathlib.Path(dest)
       
        container_dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(container_dest, "wb") as file:
                download_stream = self._FileClient.download_file()
                file.write(download_stream.readall())
                print(f"Downloaded: {name}")
                
        except Exception as e:
            print(f"An error occurred while downloading the file: {e}")

class _FileShare(_SyntheticDirNode):

    def __init__(self, fs, path, name):
        self._path = path
        self._name = name
        self._fs = fs
        self._ShareClient = ShareClient(account_url=f"https://{self._fs._storage_account_name}.file.core.windows.net", share_name=self._name, credential=self._fs._credentials, token_intent = 'backup')
        self._FilesList = list(self._ShareClient.list_directories_and_files())
        self._files = {}

    def ls(self):
        for file in self._FilesList:
            print(file.name)
            self._files[file.name] = _File
    
    def _rget(self, dest=None, dir=None, name=None, **kwargs):
        
        if dest is None:
            if dir is None:
                dir = cfg["fs.work"]
            if name is None:
                name = pathlib.Path(self._path).name
            container_dest = pathlib.Path(dir) / name
        else:
            container_dest = pathlib.Path(dest)
        container_dest.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            for file in self._FilesList:
                # file in the first level of the file share
                if '/' not in file.name and '.' in file.name:
                    # os.chdir(container_dest)
                    container_dest.mkdir(parents=True, exist_ok=True)
                    file_path = container_dest / file.name
                    with open(file_path, "wb") as file:
                        file_client = self._ShareClient.get_file_client(file.name)
                        download_file = file_client.download_file()
                        file.write(download_file.readall())
                        print(f"Downloaded: {characters_after_last_slash(file.name)}")
            

                # file not in the first level of the container
                elif '.' not in file.name:
                    # enter in the container directory
                    if not os.path.exists(container_dest / file.name):
                        dest = container_dest / file.name
                        dest.mkdir(parents=True)
                        os.chdir(dest)
                    else:
                        os.chdir(container_dest / file.name)
                    directory = self._ShareClient.get_directory_client(file.name)
                    children_files = list(directory.list_directories_and_files())
                    
                    for children_file in children_files:
                        with open (children_file["name"], "wb") as file_to_download:
                            file_client = self._ShareClient.get_file_client(file.name + "/" + children_file.name)
                            downloaded_file = file_client.download_file()
                            file_to_download.write(downloaded_file.readall())
                            print(f"Downloaded:  {children_file.name}")
                    
            
        except Exception as e:
            print(f"An error occurred while downloading the file: {e}")


class _FileSharesDir(_SyntheticDirNode):
    
    def __init__(self, fs, path):
        self._fs = fs
        self._path = path
        self._ServiceClient = StorageManagementClient(self._fs._credentials, _subscriptions_registry[self._fs._subscription_name])
        
        self._FileSharesList = self._ServiceClient.file_shares.list(resource_group_name=self._fs._resource_group_name, account_name = self._fs._storage_account_name)
        self._fileshares = {}
    
    def ls(self):
        for share in self._FileSharesList:
            print(share.name)
            self._fileshares[share.name] = _FileShare
        

class _Table():
    pass

class _TablesDir(_SyntheticDirNode):
    _child_classes = {
        'table': _Table
    }


class _Disk():
    pass

class _DisksDir(_SyntheticDirNode):
    _child_classes = {
        'disk': _Disk
    }

class _RootNode(_SyntheticDirNode):
    def __init__(self, fs, path):
        self._fs = fs
        self._path = path
    _child_classes = {
        'containers': _ContainersDir,
        'queues': _QueuesDir,
        'fileshares': _FileSharesDir,
        'tables': _TablesDir
    }


_transitory_http_codes = {
    httpx.codes.REQUEST_TIMEOUT,
    httpx.codes.TOO_MANY_REQUESTS,
    httpx.codes.INTERNAL_SERVER_ERROR,
    httpx.codes.BAD_GATEWAY,
    httpx.codes.SERVICE_UNAVAILABLE,
    httpx.codes.GATEWAY_TIMEOUT
}

class _FS:
    def __init__(self, account_name, subscription_name, storage_account_name, resource_group_name):
        self._credentials = _cred(account_name)
        self._account_name = account_name
        self._subscription_name = subscription_name
        self._storage_account_name = storage_account_name
        self._resource_group_name = resource_group_name
        self._token = self._credentials.get_token("https://storage.azure.com/.default").token
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
        headers = {**headers, "Authorization": f"Bearer {self._token}"}

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
