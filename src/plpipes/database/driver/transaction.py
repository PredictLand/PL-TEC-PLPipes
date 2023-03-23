

class Transaction:
    def __init__(self, driver, conn):
        self._driver = driver
        self._conn = conn

    def driver(self):
        return self._driver

    def connection(self):
        return self._conn

    def execute(self, sql, parameters=None, db=None):
        self._driver._execute(self, sql, parameters)

    def execute_script(self, sql_script):
        return self._driver._execute_script(self, sql_script)

    def create_table(self, table_name, sql_or_df, parameters=None, if_exists="replace", **kws):
        return self._driver._create_table(self, table_name, sql_or_df,
                                          parameters, if_exists, kws)

    def create_view(self, view_name, sql, parameters=None, if_exists="replace", **kws):
        return self._driver._create_view(self, view_name, sql, parameters, if_exists, kws)

    def read_table(self, table_name, backend=None, **kws):
        return self._driver._read_table(self, table_name, backend, kws)

    def query(self, sql, parameters=None, db=None, backend=None, **kws):
        return self._driver._query(self, sql, parameters, backend, kws)

    def query_chunked(self, sql, parameters=None, backend=None, **kws):
        return self._driver._query_chunked(self, sql, parameters, backend, kws)

    def query_group(self, sql, parameters=None, by=None, backend=None, **kws):
        return self._driver._query_group(self, sql, parameters, by, backend, kws)
    
