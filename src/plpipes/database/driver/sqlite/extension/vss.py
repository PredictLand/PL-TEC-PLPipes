import sqlite_vss

from plpipes.plugin import plugin, Plugin
from sqlalchemy import event

@plugin
class VssExtension(Plugin):

    def __init__(self, driver, extension_name, drv_cfg):

        @event.listens_for(driver._engine, "connect")
        def init_vss(conn, cr):
            conn.enable_load_extension(True)
            sqlite_vss.load(conn)

