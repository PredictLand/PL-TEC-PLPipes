from plpipes.plugin import plugin
from plpipes.database.backend.spark import SparkBackendBase
from plpipes.spark import spark_session

import pandas

@plugin
class PandasSparkHiveBatckend(SparkBackendBase):
    def _coerce_output_df(self, df):
        return df.toPandas()

    def _create_table_from_pandas(self, txn, table_name, df, *args):
        df = spark_session().createDataFrame(df)
        return self._create_table_from_spark(txn, table_name, df, *args)

    def register_handlers(self, handlers):
        handlers['create_table'].register(pandas.DataFrame, self._create_table_from_pandas)
