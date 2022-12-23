


def download(path, keys, db, acfg):
    if path == "codmun20":
        data = download_xlsx("https://www.ine.es/daco/daco42/codmun/codmun20/20codmun.xlsx", db, "ine_codmun20", header_row=2, data_start_row=3)

    else:
        raise ValueError("Unknown/unsupported path for INE downloader")
