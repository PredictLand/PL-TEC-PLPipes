import logging
import sqlalchemy
import pandas

_create_table_methods = {
    pandas.DataFrame: '_create_table_from_pandas',
    str:              '_create_table_from_sql'
}

class Driver:
    def __init__(self, name, drv_cfg, url):
        self._name = name
        self._cfg = drv_cfg
        self._url = url
        self._engine = sqlalchemy.create_engine(url)
        self._last_key = 0

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def query(self, sql, parameters):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        import pandas
        return pandas.read_sql_query(sql, self._engine, params=parameters)

    def execute(self, sql, parameters):
        logging.debug(f"database execute: {repr(sql)}, parameters: {str(parameters)[0:40]}")

        with self._engine.begin() as conn:
            conn.execute(sqlalchemy.sql.text(sql))

    def execute_script(self, sql):
        logging.debug(f"database execute_script code: {repr(sql)}")
        conn = self._engine.raw_connection()
        try:
            conn.executescript(sql)
            conn.commit()
        finally:
            conn.close()

    def read_table(self, table_name):
        return self.query(f"select * from {table_name}", None)

    def _create_table_from_pandas(self, table_name, df, _, if_exists):
        df.to_sql(table_name, self._engine, if_exists=if_exists, index=False, chunksize=1000)

    def _drop_table_if_exists(self, table_name):
        self.execute(f"drop table if exists {table_name}")

    def _drop_view_if_exists(self, view_name):
        self.execute(f"drop view if exists {view_name}")

    def _create_table_from_sql(self, table_name, sql, parameters, if_exists):
        if if_exists == "replace":
            self._drop_table_if_exists(table_name)
        create_sql = f"create table {table_name} as {sql}"
        logging.debug(f"database create table from sql code: {repr(create_sql)}, parameters: {str(parameters)[0:40]}")
        self.execute(create_sql, parameters)

    def _create_view_from_sql(self, view_name, sql, parameters, if_exists):
        if if_exists == "replace":
            self._drop_view_if_exists(view_name)
        create_sql = f"create view {view_name} as {sql}"
        logging.debug(f"database create view from sql code: {repr(create_sql)}, parameters: {str(parameters)[0:40]}")
        self.execute(create_sql, parameters)

    def create_table_from_schema(self, table_name, schema, if_exists):
        create_sql = "create table"
        if if_exists == "replace":
            self._drop_table_if_exists(table_name, if_exists)
        elif if_exists == "ignore":
            create_sql += " if not exists"
        create_sql += f" {table_name} ({schema})"
        logging.debug(f"database create table from schema: {repr(create_sql)}")
        self.execute(create_sql)

    def engine(self):
        return self._engine

    def connection(self):
        return self._engine.begin()

    def url(self):
        return self._url
