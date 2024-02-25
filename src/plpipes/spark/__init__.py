from plpipes.config import cfg
import plpipes.filesystem as fs
import plpipes.plugin
import logging


_spark_session = None

_driver_class_registry = plpipes.plugin.Registry("spark_plugin", "plpipes.spark.plugin")

def spark_session():
    global _spark_session
    if _spark_session is None:
        _spark_session = _init_spark_session()
    return _spark_session

def __dir_to_config(*args, url=False, **kwargs):
    s = str(fs.path(*args, **kwargs).resolve()).replace("\\", "/")
    if url:
        return f'file://{s}'
    return s

def _init_spark_session():
    ssc = cfg.cd("spark")
    driver_name = ssc.get("driver", "embedded")
    driver_class = _driver_class_registry.lookup(driver_name)
    driver = driver_class()
    return driver.init_spark_session(ssc)

    #cfg.setdefault_lazy('spark.sql.warehouse.dir', lambda: __dir_to_config("spark/spark-warehouse", mkdir=True, url=True))
    #cfg.setdefault_lazy('spark.metastore_db.dir', lambda: __dir_to_config("spark/metastore_db", mkparentdir=True))
    ##cfg.setdefault('spark.driver.extraJavaOptions', f'-Duser.dir={cfg["spark.home"]}')
    #cfg.setdefault('spark.sql.catalogImplementation', 'hive')
    #cfg.setdefault('spark.logLevel', 'WARN')

    #builder = pyspark.sql.SparkSession.builder.appName(cfg.get("spark.app_name", 'work'))
    #for k, v in cfg.to_flat_dict('spark').items():
    #        logging.info(f'Setting Spark configuration spark.{k}: {v}')
    #        builder = builder.config(f'spark.{k}', v)

    #builder = builder.config('javax.jdo.option.ConnectionURL',
    #                         f'jdbc:derby:;databaseName={cfg["spark.metastore_db.dir"]};create=true')

    #if cfg.get("enable_hive", False):
    #    builder = builder.enableHiveSupport()

    #ss = builder.getOrCreate()
    # dump SparkSession configuration:
    #for k, v in ss.sparkContext.getConf().getAll():
    #    logging.info(f'SparkSession configuration: {k}: {v}')

    #return ss
