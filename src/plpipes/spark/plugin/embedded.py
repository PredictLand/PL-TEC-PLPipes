from plpipes.plugin import Plugin, plugin
import plpipes.filesystem as fs
import pyspark.sql

def _clean_path(path):
    return str(path.resolve()).replace("\\", "/")

@plugin
class SparkEmbeddedPlugin(Plugin):

    def init_spark_session(self, ssc):
        home = fs.path(ssc.get("home", "spark"), mkdir=True)

        builder = pyspark.sql.SparkSession.builder \
                                          .appName(ssc.get("app_name", 'work')) \
                                          .config('spark.sql.warehouse.dir',
                                                  _clean_path(home / 'spark-warehouse')) \
                                          .config('javax.jdo.option.ConnectionURL',
                                                  f"jdbc:derby:;databaseName={_clean_path(home / 'metastore-db')};create=true") \
                                          .config('spark.logLevel', ssc.get('log_level', 'WARN')) \
                                          .config('spark.sql.catalogImplementation', 'hive')

        for k, v in ssc.to_flat_dict('extra').items():
            builder = builder.config(k, v)

        builder = builder.enableHiveSupport()

        return builder.getOrCreate()
