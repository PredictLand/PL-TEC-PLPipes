from plpipes.config import cfg
from pyspark.sql import SparkSession
import logging

_spark_session = None

def spark_session():
    global _spark_session
    if _spark_session is None:
        _spark_session = _init_spark_session()
    return _spark_session

def _init_spark_session():
    scfg = cfg.cd(f"spark")
    # if 'spark.sql.warehouse.dir' not in scfg:
    #     import plpipes.filesystem as fs
    #     whd = fs.path("spark-warehouse", mkdir=True).resolve()
    #     logging.debug(f'spark warehouse dir: {whd}')
    #     scfg['spark.sql.warehouse.dir'] = str(whd).replace("\\", "/")

    if 'spark.driver.extraJavaOptions' not in scfg:
        import plpipes.filesystem as fs
        wd = fs.path("spark", mkdir=True).resolve()
        logging.debug(f'spark working dir: {wd}')
        scfg["driver.extraJavaOptions"] = f'-Duser.dir={wd}'

    scfg.setdefault('sql.catalogImplementation', 'hive')
    scfg.setdefault('logLevel', 'WARN')
    builder = SparkSession.builder.appName(scfg.get("app_name", 'work'))

    for k, v in scfg.to_flat_dict().items():
            logging.debug(f'Setting Spark configuration spark.{k}: {v}')
            builder = builder.config(f'spark.{k}', v)

    if scfg.get("enable_hive", False):
        builder = builder.enableHiveSupport()

    session = builder.getOrCreate()
    return session

