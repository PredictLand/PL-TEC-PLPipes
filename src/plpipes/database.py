from plpipes.config import cfg
import pathlib
import logging
import sqlalchemy
import sqlalchemy.sql
import sqlalchemy.engine
import pandas
import polars
import pyarrow.lib

_driver_class = {}
_registry = {}

_create_table_methods = {
    pyarrow.lib.Table: '_create_table_from_arrow',
    polars.LazyFrame:  '_create_table_from_lazy_polars',
    polars.DataFrame:  '_create_table_from_polars',
    pandas.DataFrame:  '_create_table_from_pandas',
    str:               '_create_table_from_sql'
}

def lookup(name=None):
    if name is None:
        name = "work"
    if name not in _registry:
        _registry[name] = _init_driver(name)
    return _registry[name]

def _init_driver(name):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    return _driver_class[drv_cfg.get("driver", "duckdb")](name, drv_cfg)

class _Driver:
    def __init__(self, name, drv_cfg, url):
        self._name = name
        self._cfg = drv_cfg
        self._url = url
        self._engine = sqlalchemy.create_engine(url)
        self._last_key = 0

    def _next_key(self):
        self._last_key += 1
        return self._last_key

    def query(self, sql, parameters={}):
        logging.debug(f"database query code: {repr(sql)}, parameters: {str(parameters)[0:40]}")
        import pandas
        return pandas.read_sql_query(sql, self._engine, params=parameters)

    def execute(self, sql, parameters={}):
        logging.debug(f"database execute: {repr(sql)}, parameters: {str(parameters)[0:40]}")

        with self._engine.begin() as conn:
            conn.execute(sqlalchemy.sql.text(sql), **parameters)

    def execute_script(self, sql):
        logging.debug(f"database execute_script code: {repr(sql)}")
        conn = self._engine.raw_connection()
        try:
            conn.executescript(sql)
            conn.commit()
        finally:
            conn.close()

    def read_table(self, table_name):
        return self.query(f"select * from {table_name}")

    def _create_table_from_arrow(self, table_name, df, _, if_exists):
        _create_table_from_pandas(table_name, df.to_pandas(), None, if_exists)

    def _create_table_from_pandas(self, table_name, df, _, if_exists):
        df.to_sql(table_name, self._engine, if_exists=if_exists, index=False, chunksize=1000)

    def _create_table_from_polars(self, table_name, df, _, if_exists):
        self._create_table_from_pandas(table_name, df.to_pandas(), None, if_exists)

    def _create_table_from_lazy_polars(self, table_name, df, _, if_exists):
        self._create_table_from_polars(table_name, df.collect(), None, if_exists)

    def _drop_table_if_exists(self, table_name):
        self.execute(f"drop table if exists {table_name}")

    def _drop_view_if_exists(self, view_name):
        self.execute(f"drop view if exists {view_name}")

    def _create_table_from_sql(self, table_name, sql, parameters, if_exists):
        if if_exists=="replace":
            self._drop_table_if_exists(table_name)
        create_sql = f"create table {table_name} as {sql}"
        logging.debug(f"database create table from sql code: {repr(create_sql)}, parameters: {parameters}")
        self.execute(create_sql, parameters)

    def _create_view_from_sql(self, view_name, sql, parameters, if_exists):
        if if_exists=="replace":
            self._drop_view_if_exists(view_name)
        create_sql = f"create view {view_name} as {sql}"
        logging.debug(f"database create view from sql code: {repr(create_sql)}, parameters: {parameters}")
        self.execute(create_sql, parameters)

    def create_table_from_schema(self, table_name, schema, if_exists):
        create_sql = "create table"
        if if_exists=="replace":
            self._drop_table_if_exists(table_name, if_exists)
        elif if_exists=="ignore":
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

class _LocalFileDriver(_Driver):
    def __init__(self, name, drv_cfg, driver):
        # if there is an entry for the given name in cfg["fs"] we use
        # that, otherwise we store the db file in the work directory:
        root_dir = pathlib.Path(cfg.get(f"fs.{name}", cfg["fs.work"]))
        fn = root_dir.joinpath(drv_cfg.setdefault("file", f"{name}.{driver}")).absolute()
        fn.parent.mkdir(exist_ok=True, parents=True)

        url = f"{driver}:///{fn}"
        super().__init__(name, drv_cfg, url)
        self._fn = fn

class _SQLiteDriver(_LocalFileDriver):
    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg, "sqlite")

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

        with self.connection() as conn:
            if if_exists:
                self.execute(f"drop table if exists {name}")
            conn.connection.create_aggregate(agg_name, len(args), _C)
            try:
                conn.execute(full_sql)
            finally:
                conn.connection.create_aggregate(agg_name, len(args), None)

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

        with self.connection() as conn:
            conn.connection.create_aggregate(agg_name, len(args), _C)
            try:
                self.query(full_sql)
            finally:
                conn.connection.create_aggregate(agg_name, len(args), None)

class _ODBCDriver(_Driver):
    def __init__(self, name, drv_cfg):
        connection_string = f"driver={drv_cfg['odbc_driver']};Server={drv_cfg['server']};Database={drv_cfg['database']};UID={drv_cfg['user']};PWD={drv_cfg['pwd']}"
        url = sqlalchemy.engine.URL.create("mssql+pyodbc",
                                           query={"odbc_connect": connection_string})

        super().__init__(name, drv_cfg, url)

class _DuckDBDriver(_LocalFileDriver):
    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg, "duckdb")

    #def query(self, sql, parameters={}):
    #    with self.connection() as conn:
    #        out = conn.connection.query(sql)
    #        return polars.DataFrame(out.arrow()).lazy()

    def _create_table_from_polars(self, table_name, df, _, if_exists):
        you_dont_have_a_table_named_like_this_in_the_database_arrow = df.to_arrow()
        self._create_table_from_sql(table_name,
                                    "select * from you_dont_have_a_table_named_like_this_in_the_database_arrow",
                                    {}, if_exists)

# Register drivers
_driver_class["duckdb"] = _DuckDBDriver
_driver_class["sqlite"] = _SQLiteDriver
_driver_class["odbc"]   = _ODBCDriver

def query(sql, db=None, **parameters):
    return lookup(db).query(sql, parameters)

def execute(sql, db=None, **parameters):
    lookup(db).execute(sql, parameters)

def create_table(table_name, sql_or_df, db=None, if_exists="replace", **parameters):
    dbh = lookup(db)
    try:
        method_name = _create_table_methods[type(sql_or_df)]
    except:
        raise ValueError(f"Unsupported DataFrame type {type(sql_or_df)}")
    method = getattr(dbh, method_name)
    method(table_name, sql_or_df, parameters, if_exists)

def create_view(view_name, sql, db=None, if_exists="replace", **parameters):
    lookup(db).create_view_from_sql(view_name, sql, parameters, if_exists)

def create_empty_table(table_name, schema, db=None, if_exists="ignore"):
    return lookup(db).create_table_from_schema(table_name, schema, if_exists=if_exists)

def read_table(table_name, db=None):
    return lookup(db).read_table(table_name)

def execute_script(sql_script, db=None):
    lookup(db).execute_script(sql_script)

def download_table(from_table_name, to_table_name=None, from_db="input", to_db="work"):
    ## TODO: use an iterative approach, without dumping everything
    ## into a pandas dataframe.
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

def engine(db=None):
    return lookup(db).engine()

def connection(db=None):
    return lookup(db).connection()
