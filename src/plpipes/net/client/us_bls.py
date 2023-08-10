import pandas as pd
import httpx
import datetime
import logging

from plpipes.config import cfg

_url_v2="https://api.bls.gov/publicAPI/v2/timeseries/data/"

def _get_series(series, start_year=1957, end_year=None):
    if end_year is None:
        end_year = datetime.datetime.now().year
    dfs = []
    year = start_year
    api_key = cfg["net.client.us_bls.api_key"]
    while year <= end_year:
        top_year = min(year + 19, end_year)
        logging.debug(f"Retrieving series {series} data from {year} to {top_year} from US BLS")
        r = httpx.post(_url_v2, json={"seriesid": [series],
                                      "startyear": str(year),
                                      "endyear": str(top_year),
                                      "registrationkey": api_key })
        df = pd.DataFrame(r.json()["Results"]["series"][0]["data"])
        dfs.append(df)
        year += 20

    df = pd.concat(dfs[::-1], axis=0, ignore_index=True)
    df.columns = df.columns.str.replace('-', '_')
    return df.reset_index(drop=True)

def us_inflation(**kwargs):
    df = _get_series("CUUR0000SA0L1E", **kwargs)
    df["month"] = df['period'].str[1:].astype(int)
    df['date'] = pd.to_datetime(df['year'].astype(str) + '-' + df['month'].astype(str) + '-01').dt.date
    df = df.sort_values(by='date')
    return df.reset_index(drop=True)


