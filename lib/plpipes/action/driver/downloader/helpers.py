
import requests
import pandas as pd

from plpipes.database import create_table

def download(url):
    tmp = tempfile.NamedTemporaryFile()
    r = request.get(url)
    tmp.write(r.content)
    close(tmp)
    return tmp.name

def download_xlsx(url, db, table_name, **read_excel_params):
    fn = download(url)
    df = pd.read_excel(fn, **read_excel_params)
    create_table(table_name, df, db=db)
