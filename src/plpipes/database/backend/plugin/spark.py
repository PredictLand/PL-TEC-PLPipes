from plpipes.database.backend.spark import SparkBackendBase
from plpipes.plugin import plugin

from pyspark.sql.dataframe import DataFrame

@plugin
class SparkBackend(SparkBackendBase):

    def register_handlers(self, handlers):
        handlers['create_table'].register(DataFrame, self._create_table_from_spark)

