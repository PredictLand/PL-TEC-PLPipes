import pandas as pd
import io
import requests

def eur_usd():
    url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata"
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')),
                     parse_dates=["TIME_PERIOD"])

    return df

    
