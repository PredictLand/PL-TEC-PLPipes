
import requests
import pandas as pd
import tempfile
import requests

from plpipes.database import create_table

def download_to_file(url):
    tmp = tempfile.NamedTemporaryFile(delete=False)
    r = requests.get(url)
    tmp.write(r.content)
    tmp.close()
    return tmp.name

def download_xlsx(url, db, table_name, **read_excel_params):
    fn = download_to_file(url)
    df = pd.read_excel(fn, **read_excel_params)
    create_table(table_name, df, db=db)
