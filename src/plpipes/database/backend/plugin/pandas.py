from plpipes.database.backend.pandas import PandasBackend
from plpipes.plugin import plugin

plugin("pandas")(PandasBackend)

