import pandas as pd

def eur_usd():
    df = pd.read_csv("https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata",
                     parse_dates=["TIME_PERIOD"])
    return df

    
