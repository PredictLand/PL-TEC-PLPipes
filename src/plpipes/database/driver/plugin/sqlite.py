from plpipes.database.driver.sqlite import SQLiteDriver
from plpipes.plugin import plugin

plugin("sqlite")(SQLiteDriver)
