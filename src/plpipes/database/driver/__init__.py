import logging
import sqlalchemy
import pandas

from plpipes.database.sqlext import CreateTableAs, CreateViewAs, DropTable, DropView
from plpipes.util.typedict import dispatcher

def _wrap(sql):
    if isinstance(sql, str):
        return sqlalchemy.sql.text(sql)
    return sql

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
        return pandas.read_sql_query(sqlalchemy.sql.text(sql), self._engine, params=parameters)

    def execute(self, sql, parameters=None):
        self._engine.execute(_wrap(sql), parameters)

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

    @dispatcher({pandas.DataFrame: '_create_table_from_pandas',
                 str: '_create_table_from_str',
                 sqlalchemy.sql.elements.ClauseElement: '_create_table_from_clause'},
                ix=1)
    def create_table(self, table_name, sql_or_df, parameters, if_exists):
        ...

    def _create_table_from_pandas(self, table_name, df, _, if_exists):
        if "." in table_name:
            schema, table_name = table_name.split(".", 1)
        else:
            schema = None
        df.to_sql(table_name, self._engine,
                  schema=schema, if_exists=if_exists,
                  index=False, chunksize=1000)

    def _create_table_from_str(self, table_name, sql, parameters, if_exists):
        return self._create_table_from_clause(table_name, sqlalchemy.sql.text(sql), parameters, if_exists)

    def _create_table_from_clause(self, table_name, clause, parameters, if_exists):
        with self._engine.begin() as conn:
            if_not_exists = False
            if if_exists == "replace":
                conn.execute(DropTable(table_name, if_exists=True))
            elif if_exists == "ignore":
                if_not_exists = True
            conn.execute(CreateTableAs(table_name, clause, if_not_exists=if_not_exists),
                         parameters)

    @dispatcher({sqlalchemy.sql.elements.ClauseElement: '_create_view_from_clause',
                 str: '_create_view_from_str'},
                ix=1)
    def create_view(self, table_name, sql, parameters, if_exists):
        ...

    def _create_view_from_str(self, view_name, sql, parameters, if_exists):
        return self._create_view_from_clause(view_name, sqlalchemy.sql.text(sql), parameters, if_exists)

    def _create_view_from_clause(self, view_name, clause, parameters, if_exists):
        with self._engine.begin() as conn:
            if_not_exists = False
            if if_exists == "replace":
                conn.execute(DropView(view_name, if_exists=True))
            elif if_exists == "ignore":
                if_not_exists = True
            conn.execute(CreateViewAs(view_name, clause, if_not_exists=if_not_exists),
                         parameters)

    def engine(self):
        return self._engine

    def connection(self):
        return self._engine.begin()

    def url(self):
        return self._url
