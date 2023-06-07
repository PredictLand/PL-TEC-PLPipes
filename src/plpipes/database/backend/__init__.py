from plpipes.plugin import Plugin

class Backend(Plugin):
    def query(self, engine, sql, paramaters, kws):
        raise NotImplementedError("This function is not yet implemented.")

    def query_chunked(self, engine, sql, parameter, kws):
        raise NotImplementedError("This function is not yet implemented.")

    def query_group(self, engine, sql, parameters, by, kws):
        raise NotImplementedError("This function is not yet implemented.")

    def query_first(self, engine, sql, parameters, kws):
        raise NotImplementedError("This function is not yet implemented.")

    def query_first_value(self, engine, sql, parameters, kws):
        raise NotImplementedError("This function is not yet implemented.")
