from plpipes.config import cfg
from pyspark.sql import SparkSession
import logging

_spark_registry = {}

def spark(name=None):
    if name is None:
        name = "work"
    if name not in _spark_registry:
        _spark_registry[name] = _init_spark_session(name)
    return _spark_registry[name]

def _init_spark_session(name):
    scfg = cfg.cd(f"spark.{name}")
    # if 'spark.sql.warehouse.dir' not in scfg:
    #     import plpipes.filesystem as fs
    #     whd = fs.path("spark-warehouse", mkdir=True).resolve()
    #     logging.debug(f'spark warehouse dir: {whd}')
    #     scfg['spark.sql.warehouse.dir'] = str(whd).replace("\\", "/")

    if 'spark.driver.extraJavaOptions' not in scfg:
        import plpipes.filesystem as fs
        wd = fs.path("spark", mkdir=True).resolve()
        logging.debug(f'spark working dir: {wd}')
        scfg["spark.driver.extraJavaOptions"] = f'-Duser.dir={wd}'

    scfg.setdefault('spark.sql.catalogImplementation', 'hive')

    scfg.setdefault('spark.logLevel', 'WARN')

    builder = SparkSession.builder.appName(cfg.get("app_name", name))

    for k, v in scfg.to_flat_dict().items():
        if k.startswith('spark.'):
            logging.debug(f'Setting Spark configuration {k}: {v}')
            builder = builder.config(k, v)

    if scfg.get("enable_hive", False):
        builder = builder.enableHiveSupport()

    session = builder.getOrCreate()
    # session.sparkContext.setLogLevel(scfg.setdefault('logging.level', 'WARN'))

    return session

