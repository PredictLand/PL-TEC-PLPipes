import logging

from plpipes.database.backend import Backend
from plpipes.plugin import plugin
from plpipes.spark import spark_session

class SparkBackendBase(Backend):
    def query(self, txn, sql, parameters, kws):
        df = txn._conn.sql(sql, args=parameters, **kws)
        return self._coerce_output_df(df)

    def read_table(self, txn, table_name, kws):
        df = txn._conn.read.table(table_name)
        return self._coerce_output_df(df)

    def _coerce_output_df(self, df):
        return df

    def _create_table_from_spark(self, txn, table_name, df, parameters, if_exists, kws):
        logging.debug(f"Creating table {table_name} from spark dataframe, if_exists={if_exists}")
        if if_exists == 'replace':
            txn.drop_table(table_name, True)
            mode = 'overwrite'
        elif if_exists == 'append':
            mode = 'append'
        elif if_exists == 'ignore':
            mode = 'ignore'
        else:
            raise ValueError(f"Bad value {if_exists} for if_exists")

        logging.debug(f"Writing dataframe to table {table_name} with mode {mode}")
        df.write.mode(mode).saveAsTable(table_name)
