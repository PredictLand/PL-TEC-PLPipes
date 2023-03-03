from plpipes.database.driver.localdb import LocalDBDriver
from plpipes.plugin import plugin

@plugin("duckdb")
class DuckDBDriver(LocalDBDriver):
    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg, "duckdb")
