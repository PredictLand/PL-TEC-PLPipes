from plpipes.config import cfg
import pathlib
import logging

_driver_class = {}
_registry = {}

def lookup(name=None):
    if name is None:
        name = "work"
    if name not in _registry:
        _registry[name] = _init_driver(name)
    return _registry[name]

def _init_driver(name):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    return _driver_class[drv_cfg.get("driver", "sqlite")](name, drv_cfg)

class _Driver:
    def __init__(self, name, drv_cfg, conn=None):
        self._name = name
        self._cfg = drv_cfg
        self._conn = conn

    def query(self, sql, parameters=()):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        import pandas
        return pandas.read_sql_query(sql, self._conn, parameters=parameters)

    def execute(self, sql, parameters=None, commit=True):
        logging.debug(f"database execute: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        self._conn.cursor().execute(sql, parameters)
        if commit:
            self._conn.commit()

    def execute_script(self, sql, commit=True):
        logging.debug(f"database execute_script code: {repr(sql)}")
        self._conn.executescript(sql)
        if commit:
            self._conn.commit()

    def commit(self):
        self._conn.commit()

    def read_table(self, table_name):
        return self.query(f"select * from {table_name}")

    def create_table_from_pandas(self, table_name, df, if_exists):
        df.to_sql(table_name, self._conn, if_exists=if_exists)

    def _drop_table_if_exists(self, table_name):
        drop_sql = f"drop table if exists {table_name}"
        logging.debug(f"database create table from sql drop code: {repr(drop_sql)}")
        self._conn.cursor().execute(drop_sql)

    def create_table_from_sql(self, table_name, sql, parameters, if_exists):
        if if_exists=="replace":
            self._drop_table_if_exists(table_name)
        create_sql = f"create table {table_name} as {sql}"
        logging.debug(f"database create table from sql code: {repr(create_sql)}, parameters: {parameters}")
        self._conn.cursor().execute(create_sql, parameters)

    def create_table_from_schema(self, table_name, schema, if_exists):
        create_sql = "create table"
        if if_exists=="replace":
            self._drop_table_if_exists(table_name, if_exists)
        elif if_exists=="ignore":
            create_sql += " if not exists"
        create_sql += f" {table_name} ({schema})"
        logging.debug(f"database create table from schema: {repr(create_sql)}")
        self._conn.cursor().execute(create_sql)

class _SQLiteMapAsPandas:
    def __init__(self):
        self.rows = []

    def step(self, *row):
        self.rows.append(row)

    def finalize(self):
        try:
            df = pandas.DataFrame(self.rows, columns=list(args))
            r = self.process_pandas(df)
            return json.dumps(r)
        except Exception as ex:
            logging.error(f"Exception caught: {ex}")
            raise ex

class _SQLiteDriver(_Driver):
    def __init__(self, name, drv_cfg):
        # if there is an entry for the given name in cfg["fs"] we use
        # that, otherwise we store the db file in the work directory:

        root_dir = pathlib.Path(cfg.get(f"fs.{name}", cfg["fs.work"]))

        fn = root_dir.joinpath(drv_cfg.setdefault("file", f"{name}.sqlite")).absolute()
        fn.parent.mkdir(exist_ok=True, parents=True)
        import sqlite3
        conn = sqlite3.connect(str(fn))
        super().__init__(name, drv_cfg, conn=conn)
        self._last_key = 0

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def create_table_from_query_group_and_map(self,
                                              name, sql, by,
                                              function, args, out,
                                              if_exists):
        n_out = len(out)

        class _C(_SQLiteMapAsPandas):
            def process_pandas(self, df):
                r = function(df)
                if len(r) != n_out:
                    raise ValueError(f"Wrong number of items returned by function, {len(r)} found, {n_out} expected")
                return r

        key = self._next_key()
        agg_name = f"_map_to_pandas_aggregate_XXX{key}"
        out_name = f"_aggregate_output_column_XXX{key}"

        extractors = [f"""json_extract({out_name}, "$[{ix}]") as {col_name}"""
                      for ix, col_name in enumerate(out)]

        full_sql = f"""
        create table {name} as
          select {','.join([*by, *extractors])}
          from (
            select {','.join([*by, ''])}
                   {agg_name}({', '.join(args)}) as {out_name}
            from (
              {sql}
            )
            group by {', '.join(by)}
         )"""

        logging.debug(f"SQL code: {repr(full_sql)}")

        if if_exists:
            self.execute(f"drop table if exists {name}")
        self._conn.create_aggregate(agg_name, len(args), _C)
        try:
            self.execute(full_sql)
        finally:
            self._conn.create_aggregate(agg_name, len(args), None)

    def query_group_and_map(self,
                            sql, by,
                            function, args):

        class _C(_MapAsPandas):
            def process_pandas(self, df):
                function(df)
                return 1

        key = self._next_key()
        agg_name = f"_map_to_pandas_aggregate_XXX{_last_key}"

        full_sql = f"""
select sum({agg_name}({', '.join(args)}))
from (
  {sql}
)
group by {', '.join(by)}
"""
        logging.debug(f"SQL code: {repr(full_sql)}")

        if exists:
            self.execute(f"drop table if exists {name}")

        self._conn.create_aggregate(agg_name, len(args), _C)
        try:
            self.query(full_sql)
        finally:
            self._conn.create_aggregate(agg_name, len(args), None)


class _ODBCDriver(_Driver):
    def __init__(self, name, drv_cfg):
        import pyodbc

        connection_string = f"driver={drv_cfg['driver']};Server={drv_cfg['server']};Database={drv_cfg['database']};UID={drv_cfg['user']};PWD={drv_cfg['pwd']}"
        conn = pyodbc.connect(connection_string)
        super().__init__(name, drv_cfg, conn=conn)

# Register drivers
_driver_class["sqlite"] = _SQLiteDriver
_driver_class["odbc"] = _ODBCDriver

def query(sql, *parameters, db=None):
    return lookup(db).query(sql, parameters)

def execute(sql, *parameters, db=None, commit=True):
    lookup(db).execute(sql, parameters)

def commit(db=None):
    lookup(db).commit()

def create_table(table_name, sql_or_df, *parameters, db=None, if_exists="replace"):
    dbh = lookup(db)
    if isinstance(sql_or_df, str):
        dbh.create_table_from_sql(table_name, sql_or_df, parameters, if_exists)
    else:
        if parameters:
            raise ValueError("Query parameters are not supported when creating a table from a dataframe")
        dbh.create_table_from_pandas(table_name, sql_or_df, if_exists)

def create_empty_table(table_name, schema, db=None, if_exists="ignore"):
    return lookup(db).create_table_from_schema(table_name, schema, if_exists=if_exists)

def read_table(table_name, db=None):
    return lookup(db).read_table(table_name)

def execute_script(sql_script, db=None, commit=True):
    lookup(db).execute_script(sql_script)

def download_table(from_table_name, to_table_name=None, from_db="input", to_db="work"):
    if to_table_name is None:
        to_table_name = from_table_name
    df = read_table(from_table_name, db=from_db)
    create_table(to_table_name, df, db=to_db)

def create_table_from_query_group_and_map(name,
                                          sql,
                                          by,
                                          function,
                                          args,
                                          out,
                                          db=None,
                                          if_exists="replace"):
    if isinstance(by, str):
        by = [by]
    if isinstance(args, str):
        args = [args]
    if isinstance(out, str):
        out = [out]
    lookup(db).create_table_from_query_group_and_map(name, sql, by,
                                                     function, args, out,
                                                     if_exists)

def query_group_and_map(sql,
                        by,
                        function,
                        args,
                        db=None,
                        if_exists="replace"):
    if isinstance(by, str):
        by = [by]
    if isinstance(args, str):
        args = [args]
    if isinstance(out, str):
        out = [out]
    lookup(db).query_group_and_map(sql, by,
                                   function, args,
                                   if_exists)

