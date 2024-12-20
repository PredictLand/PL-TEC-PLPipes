from plpipes.database.driver.filedb import FileDBDriver
from plpipes.plugin import plugin

import sqlalchemy.sql as sas

@plugin
class DuckDBDriver(FileDBDriver):
    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg, "duckdb")

    def _list_tables_query(self):
        return sas.select(sas.column("table_name").label("name")) \
                  .select_from(sas.table("tables", schema="information_schema")) \
                  .where(sas.and_(sas.column("table_schema") == "main",
                                  sas.column("table_type") == "BASE TABLE"))

    def _list_views_query(self):
        return sas.select(sas.column("table_name").label("name")) \
                  .select_from(sas.table("tables", schema="information_schema")) \
                  .where(sas.and_(sas.column("table_schema") == "main",
                                  sas.column("table_type") == "VIEW"))

