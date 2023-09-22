from plpipes.config import cfg
import plpipes.filesystem as fs
import plpipes.plugin
import logging

_spark_session = None

_driver_class_registry = plpipes.plugin.Registry("spark_plugin", "plpipes.spark.plugin")

def spark_session():
    """Retrieve or create a Spark session.

    This function checks if a Spark session already exists and returns it.
    If not, it initializes a new Spark session based on the configuration
    specified in the 'spark' section of the configuration object.

    Returns:
        Spark session instance.
    """
    global _spark_session
    if _spark_session is None:
        _spark_session = _init_spark_session()
    return _spark_session

def __dir_to_config(*args, url=False, **kwargs):
    """Convert directory path to a string representation.

    This function constructs a file path from given arguments and
    options, resolving the path and converting it to a URL if specified.

    Args:
        *args: Arguments to build the file path.
        url (bool): If True, returns the path as a URL. Defaults to False.
        **kwargs: Additional keyword arguments for path construction.

    Returns:
        str: The constructed file path or URL.
    """
    s = str(fs.path(*args, **kwargs).resolve()).replace("\\", "/")
    if url:
        return f'file://{s}'
    return s

def _init_spark_session():
    """Initialize a new Spark session.

    This function retrieves the Spark configuration from the global
    configuration object, looks up the specified driver class,
    and initializes a Spark session. It uses the configuration
    provided for Spark settings.

    Returns:
        Spark session instance.
    """
    ssc = cfg.cd("spark")
    driver_name = ssc.get("driver", "embedded")
    driver_class = _driver_class_registry.lookup(driver_name)
    driver = driver_class()
    return driver.init_spark_session(ssc)
