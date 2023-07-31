
from ..helpers import download_xlsx

def download(path, params, db, acfg):
    if path == "codmun20":
        data = download_xlsx("https://www.ine.es/daco/daco42/codmun/codmun20/20codmun.xlsx", db, "ine_codmun20", header=1)

    else:
        raise ValueError("Unknown/unsupported path for INE downloader")
