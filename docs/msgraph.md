
### Microsoft Graph

Interaction with MS Graph API, which provides access to OneDrive,
SharePoint and Teams resources, is available through the package
[`plpipes.cloud.azure.graph`](reference/plpipes/cloud/azure/graph.md).

#### API

- `graph(account_name)`: returns an object of class
   `msgraph.code.GraphClient`. Note that the Python Azure SDK is still in
   beta, in a state of flush and so, this method may return objects of a
   different class in the future.

- `fs(account_name)`: returns an object that allows to access MS Graph
    resources as a file system.

##### File-system view

The file system facade class exposes MS Graph resources as a file
system.

Resources are exposed under different routes as follows:

- `me`: Business user OneDrive drive.

- `groups`: Teams group drives.


The file system objects returned by `fs` support the following
methods:

- `go(path)`: You can think of this method as a change dir (`cd`)
    operation with the particularity that it also allows one to descend
    into file-like resources.

    The returned value is a new file system object with the root at
    `path`.

- `ls(path)`: Return a dictionary of file-name and entry pairs
    representing the entries under the directory `path`.

- `names(path)`: Similar to `ls` but returns only the names of the
    entries.

- `is_file()` and `is_dir()`: Determines where the current file system
    object is pointing to a file or a directory respectively.

- `get(path="", dest=None, dir=None, name=None)`: downloads the remote
    object pointed by the current file system object.

    When `dest` is given it is used as the local destination path.

    Alternatively, when `dest` is not given, `dir` and `name` values (or
    their defaults) are used to construct the local destination
    path. `name` defaults to the remote file name. `dir` defaults to the
    working directory (i.e. `cfg['fs.work']`).

- `rget(path="", dest=None, dir=None, name=None)`: recursively downloads
    the remote object (typically a directory) to the current file
    system.

Example usage:

```python
import plpipes.cloud.azure.graph

fs = plpipes.cloud.azure.graph.fs("predictland")
group_drive = fs.go("groups/HAL/General")
group_drive.rget("input-data")
```

#### Configuration

Currently, the only supported configuration parameter is `credentials`
with must be the name of an Azure authentication account defined under
`cloud.azure.auth`. When not given, it defaults to the one of the same
name.

```yaml
cloud:
  azure:
    graph:
      predictland:
        credentials: predictland
```
