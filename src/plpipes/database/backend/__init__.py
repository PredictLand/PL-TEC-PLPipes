

class Backend:
    def query(self, engine, sql, paramaters):
        ...

    def query_chunked(self, engine, sql, parameters, chunksize):
        ...

    def query_group(self, engine, sql, parameters, by, chunksize):
        ...



