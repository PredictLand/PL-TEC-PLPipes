import logging

from plpipes.database.backend import Backend
from plpipes.plugin import plugin
from pyspark.sql.dataframe import DataFrame

@plugin
class SparkBackend(Backend):
    def query(self, txn, sql, parameters, kws):
        return self._df_read_sql(txn, sql, parameters, **kws)

    def _df_read_sql(self, txn, sql, parameters, **kws):
        return txn._driver._session.sql(sql, args=parameters, **kws)

    def register_handlers(self, handlers):
        handlers['create_table'].register(DataFrame, self._create_table_from_spark)

    def _create_table_from_spark(self, txn, table_name, df, parameters, if_exists, kws):
        logging.debug(f"Creating table {table_name} from spark dataframe")

        if if_exists == 'replace':
            mode = 'overwrite'
        elif if_exists == 'append':
            mode = 'append'
        else:
            raise ValueError(f"Bad value {if_exists} for if_exists")

        df.write.saveAsTable(table_name, mode=mode)
