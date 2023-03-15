import geopandas
import pandas
import random
import sqlalchemy.sql as sas
from sqlalchemy.sql.expression import func as saf

from plpipes.database.backend.geopandas import GeoPandasBackend
from plpipes.database.sqlext import AsSubquery
from plpipes.plugin import plugin

@plugin
class GeoPandasSpatialiteBackend(GeoPandasBackend):

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
