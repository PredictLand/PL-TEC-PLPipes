import geopandas
import pandas
import random
import sqlalchemy.sql as sas
from sqlalchemy.sql.expression import func as saf

from plpipes.database.backend.pandas import PandasBackend
from plpipes.database.sqlext import AsSubquery
from plpipes.util.database import split_table_name
from plpipes.plugin import plugin

DEFAULT_CHUNKSIZE = 5000

@plugin
class GeoPandasBackend(PandasBackend):

    def register_handlers(self, handlers):
        handlers["create_table"].register(geopandas.GeoDataFrame, self._create_table_from_geopandas)

    def _create_table_from_geopandas(self, driver, table_name, df, parameters, if_exists, kws):
        chunksize = driver._pop_kw(kws, "chunksize", DEFAULT_CHUNKSIZE)
        schema, table_name = split_table_name(table_name)
        return df.to_postgis(table_name, self._engine.connect(),
                             schema=schema, if_exists=if_exists,
                             index=False, chunksize=chunksize, **kws)

    def _df_read_sql(self, sqla, engine, wkb_geom_col=None, geom_col=None, **kws):
        if geom_col is not None:
            wrapped_col = f"{geom_col}__wrapped{random.randint(0,10000)}"
            wrapped_sql = (sas.select("*",
                                      saf.Hex(saf.ST_AsBinary(sas.column(geom_col))).label(wrapped_col))
                           .select_from(AsSubquery(sqla)))

            df = geopandas.read_postgis(wrapped_sql, engine, geom_col=wrapped_col, **kws)
            df[geom_col] = df[wrapped_col]
            df.drop([wrapped_col], axis=1, inplace=True)
            return df
        elif wkb_geom_col is not None:
            return geopandas.read_postgis(wrapped_sql, engine, geom_col=wkb_geom_col, **kws)
        else:
            return pandas.read_sql(sqla, engine, **kws)

    def _df_concat(self, dfs):
        if isinstance(dfs[0], geopandas.GeoDataFrame):
            return geopandas.concat(dfs)
        else:
            return pandas.concat(dfs)
