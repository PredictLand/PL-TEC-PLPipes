import logging
import pandas
import sqlalchemy.sql as sas

from plpipes.database.backend import Backend
from plpipes.database.sqlext import AsSubquery, Wrap
from plpipes.util.database import split_table_name

DEFAULT_CHUNKSIZE = 5000

class PandasBackend(Backend):
    def query(self, txn, sql, parameters, kws):
        return self._df_read_sql(txn, Wrap(sql), params=parameters, **kws)

    def query_chunked(self, txn, sql, parameters, kws):
        chunksize = txn._driver._pop_kw(kws, "chunksize", DEFAULT_CHUNKSIZE)
        for chunk in self._df_read_sql(txn, Wrap(sql), params=parameters, chunksize=chunksize, **kws):
            yield chunk

    def query_group(self, txn, sql, parameters, by, kws):
        wrapped_sql = sas.select("*").select_from(AsSubquery(Wrap(sql))).order_by(*[sas.column(c) for c in by])

        tail = None
        for chunk in self.query_chunked(txn, wrapped_sql, parameters, kws):
            if tail is not None:
                chunk = self._df_concat([tail, chunk])
            groups = [g for _, g in chunk.groupby(by)]
            tail = groups.pop()
            for group in groups:
                group = group.reset_index()
                yield group
        if tail is not None:
            yield tail

    def register_handlers(self, handlers):
        handlers["create_table"].register(pandas.DataFrame, self._create_table_from_pandas)

    def _create_table_from_pandas(self, txn, table_name, df, paramaters, if_exists, kws):
        logging.debug(f"Creating table {table_name} from pandas dataframe (shape: {df.shape})")
        chunksize = txn._driver._pop_kw(kws, "chunksize", DEFAULT_CHUNKSIZE)
        schema, table_name = split_table_name(table_name)
        df.to_sql(table_name, txn._conn,
                  schema=schema, if_exists=if_exists,
                  index=False, chunksize=chunksize,
                  **kws)

    def _df_read_sql(self, txn, sqla, **kws):
        return pandas.read_sql(sqla, txn._conn, **kws)

    def _df_concat(self, dfs):
        return pandas.concat(dfs)
