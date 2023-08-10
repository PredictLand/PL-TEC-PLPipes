import eurostat

import pandas as pd

def eu_hicp():
    df = eurostat.get_data_df("PRC_HICP_MIDX")
    df = df.melt(id_vars=['freq', 'unit', 'coicop', 'geo\TIME_PERIOD'],
                 var_name='period',
                 value_name='value')
    df.rename(columns={"geo\TIME_PERIOD": "geo__time_period"}, inplace=True)
    df["period"] = pd.to_datetime(df["period"]).dt.date
    return df

