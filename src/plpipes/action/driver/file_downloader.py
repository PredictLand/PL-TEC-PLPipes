import logging
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
import httpx
import datetime

from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg

class _FileDownloader(Action):
    def do_it(self):
        url = self._cfg["url"]
        method = self._cfg.get("method", "get")
        target = self._cfg.get("target")
        if target is None:
            url_parts = urlsplit(url)
            target = Path(urlsplit(url).path).name
        target = Path(cfg["fs.work"]) / target

        max_retries = self._cfg.get("max_retries", 5)
        timeout = self._cfg.get("timeout", 10)
        update = self._cfg.get("update", True)

        if target.exists() and not update:
            target.unlink()

        try:
            return _download_file(url, target, method, max_retries, timeout)
        except:
            logging.error(f"Unable to download file from {url}")
            raise

register_class("file_downloader", _FileDownloader)


def _parse_http_date(str):
    return datetime.datetime.strptime(str, "%a, %d %b %Y %H:%M:%S %Z")


def _download_file(url, destination, method="get", max_retries=3, timeout=30):

    local_file_size = 0
    remote_file_size = 0

    got_header = False
    retries = 0
    with httpx.Client() as client:
        while retries <= max_retries:
            try:
                if not got_header:
                    if destination.is_file():
                        st = destination.stat()
                        if st.st_size > 0:
                            resp = client.head(url, timeout=timeout)
                            logging.debug(f"HEAD response: {resp.status_code}, headers: {resp.headers}")
                            resp.raise_for_status()
                            retries = 0
                            try:
                                remote_file_size = int(resp.headers['Content-Length'])
                                remote_last_modified = _parse_http_date(resp.headers['Last-Modified'])
                                if remote_last_modified <= datetime.datetime.fromtimestamp(st.st_mtime):
                                    if remote_file_size == st.st_size:
                                        return True
                                    else:
                                        local_file_size = st.st_size
                            except:
                                logging.debug("Unable to retrieve metadata for remote file", exc_info=True)
                                pass
                    got_header = True

                headers = {}
                if local_file_size > 0:
                    logging.info(f"Resumming download from {local_file_size}")
                    headers['Range'] = f'bytes={local_file_size}-'

                with client.stream(method.upper(), url, timeout=timeout, headers=headers) as resp:
                    logging.debug(f"{method} response: {resp.status_code}, headers: {resp.headers}")
                    resp.raise_for_status()
                    retries = 0
                    if local_file_size > 0:
                        if 'Content-Range' not in resp.headers:
                            logging.info("Server doesn't support resuming downloads. Starting from the beginning.")
                            local_file_size = 0
                            continue
                    with destination.open("rb+" if local_file_size else "wb") as f:
                        f.seek(local_file_size)
                        for chunk in resp.iter_bytes():
                            f.write(chunk)
                            local_file_size = f.tell()
                    try:
                        remote_last_modified = httpx.utils.parse_date_time(resp.headers['Last-Modified'])
                        destination.touch(times=(remote_last_modified, remote_last_modified))
                    except:
                        logging.warning("Unable to set modification time of downloaded file")
                    return True

            except (httpx.HTTPError, httpx.TimeoutException) as e:
                if retries >= max_retries:
                    raise
                retries += 1
                logging.info(f'Retry {retries}/{max_retries} downloading file')
                time.sleep(1)
