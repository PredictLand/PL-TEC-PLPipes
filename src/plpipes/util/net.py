def download_json(url, headers={}):
    return requests.get(url, headers=headers).json()

def download_to_file(url):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    r = requests.get(url)
    tmp.write(r.content)
    tmp.close()
    return tmp.name

