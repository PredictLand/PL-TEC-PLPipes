import pandas as pd
import httpx

from plpipes.config import cfg

def petroleum_prices():
    r = httpx.get("https://api.eia.gov/v2/petroleum/pri/spt/data/?frequency=weekly&data[0]=value&facets[product][]=EPCBRENT&facets[product][]=EPCWTI&sort[0][column]=period&sort[0][direction]=desc",
                  timeout=120,
                  params = {'api_key': cfg['net.client.eia.api_key']})
    df = pd.DataFrame(r.json()['response']['data'])
    df.columns = df.columns.str.replace('-', '_')
    df['period'] = pd.to_datetime(df['period']).dt.date
    return df
