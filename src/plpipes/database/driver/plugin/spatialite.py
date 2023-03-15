import logging
import os

from plpipes.database.driver.sqlite import SQLiteDriver
from plpipes.plugin import plugin

from sqlalchemy import event

@plugin
class SpatialiteDriver(SQLiteDriver):

    _default_backend_name = "geopandas"

    def __init__(self, name, drv_cfg):
        os.environ["SPATIALITE_SECURITY"] = "relaxed"

        super().__init__(name, drv_cfg)

        @event.listens_for(self._engine, "connect")
        def init_spatialite_connection(conn, cr):
            logging.debug("Initializing SQLite connection, loading mod_spatialite")
            conn.enable_load_extension(True)
            conn.execute("select load_extension('mod_spatialite')")

        self.execute("select InitSpatialMetaData()", None)
