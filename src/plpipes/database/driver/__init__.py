import logging
import sqlalchemy
import pandas
import types

import sqlalchemy.sql as sas

from plpipes.database.sqlext import CreateTableAs, CreateViewAs, DropTable, DropView, AsSubquery
from plpipes.util.typedict import dispatcher

def _wrap(sql):
    if isinstance(sql, str):
        return sas.text(sql)
    return sql

def _split_table_name(table_name):
    if "." in table_name:
        schema, table_name = table_name.split(".", 1)
    else:
        schema = None
    return (schema, table_name)

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
        return pandas.read_sql_query(sas.text(sql), self._engine, params=parameters)

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
                 sas.elements.ClauseElement: '_create_table_from_clause',
                 types.GeneratorType: '_create_table_from_generator'},
                ix=1)
    def create_table(self, table_name, sql_or_df, parameters, if_exists):
        ...

    def _create_table_from_pandas(self, table_name, df, _, if_exists):
        schema, table_name = _split_table_name(table_name)
        df.to_sql(table_name, self._engine.connect(),
                  schema=schema, if_exists=if_exists,
                  index=False, chunksize=1000)

    def _create_table_from_str(self, table_name, sql, parameters, if_exists):
        return self._create_table_from_clause(table_name, sas.text(sql), parameters, if_exists)

    def _create_table_from_clause(self, table_name, clause, parameters, if_exists):
        with self._engine.begin() as conn:
            if_not_exists = False
            if if_exists == "replace":
                conn.execute(DropTable(table_name, if_exists=True))
            elif if_exists == "ignore":
                if_not_exists = True
            conn.execute(CreateTableAs(table_name, clause, if_not_exists=if_not_exists),
                         parameters)

    def _create_table_from_generator(self, table_name, gen, _, if_exists):
        schema, table_name = _split_table_name(table_name)
        first = True
        with self._engine.begin() as conn:
            for df in gen:
                df.to_sql(table_name, conn,
                          schema=schema,
                          if_exists=if_exists if first else "append",
                          index=False, chunksize=1000)
                first = False
                # conn.commit()

    @dispatcher({sas.elements.ClauseElement: '_create_view_from_clause',
                 str: '_create_view_from_str'},
                ix=1)
    def create_view(self, table_name, sql, parameters, if_exists):
        ...

    def _create_view_from_str(self, view_name, sql, parameters, if_exists):
        return self._create_view_from_clause(view_name, sas.text(sql), parameters, if_exists)

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

    def query_chunked(self, sql, parameters, chunksize):
        with self._engine.connect() as conn:
            for chunk in pandas.read_sql(_wrap(sql), conn, chunksize=chunksize):
                yield chunk

    def query_group(self, sql, parameters, by):
        wrapped_sql = sas.select("*").select_from(AsSubquery(_wrap(sql))).order_by(*[sas.column(c) for c in by])

        tail = None
        for chunk in self.query_chunked(wrapped_sql, parameters, 1000):
            if tail is not None:
                chunk = pandas.concat([tail, chunk])
            groups =  [g for _, g in chunk.groupby(by)]
            tail = groups.pop()
            for group in groups:
                group = group.reset_index()
                yield group
        if tail is not None:
            yield tail
