from plpipes.database.driver.sql_server import SQLServerDriver
from plpipes.plugin import plugin

@plugin
class AzureSQL(SQLServerDriver):
    pass
