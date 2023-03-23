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

    def _df_read_sql(self, txn, sqla, wkb_geom_col=None, geom_col=None, **kws):
        if geom_col is not None:
            wrapped_col = f"{geom_col}__wrapped{random.randint(0,10000)}"
            wrapped_sql = (sas.select("*",
                                      saf.Hex(saf.ST_AsBinary(sas.column(geom_col))).label(wrapped_col))
                           .select_from(AsSubquery(sqla)))

            df = geopandas.read_postgis(wrapped_sql, txn._conn, geom_col=wrapped_col, **kws)
            df.drop([geom_col], axis=1, inplace=True)
            df.rename(columns={wrapped_col: geom_col}, inplace=True)
            df.set_geometry(geom_col, inplace=True)
            return df
        elif wkb_geom_col is not None:
            return geopandas.read_postgis(wrapped_sql, txn._conn, geom_col=wkb_geom_col, **kws)
        else:
            return pandas.read_sql(sqla, txn._conn, **kws)
