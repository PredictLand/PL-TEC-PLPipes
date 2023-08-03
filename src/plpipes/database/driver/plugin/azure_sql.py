from plpipes.database.driver.azure_sql import AzureSQLDriver
from plpipes.plugin import plugin

plugin(AzureSQLDriver)

# TODO: Having the code in parallel clases is not really required. Move it here!
