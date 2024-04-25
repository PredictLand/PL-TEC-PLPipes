from plpipes.database.driver.filedb import FileDBDriver

from plpipes.database.driver.transaction import Transaction
from plpipes.database.sqlext import Wrap

import sqlalchemy as sa
import sqlalchemy.sql as sas

import plpipes.plugin
extension_register = plpipes.plugin.Registry("db_backend_sqlite_extension", "plpipes.database.driver.sqlite.extension")

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

class SQLiteTransaction(Transaction):
    def create_function(self, name, nargs, pyfunc):
        self._driver._create_function(self, name, nargs, pyfunc)

class SQLiteDriver(FileDBDriver):

    _transaction_factory = SQLiteTransaction

    def __init__(self, name, drv_cfg):
        super().__init__(name, drv_cfg, "sqlite")

        self._extensions = []
        for extension_name in drv_cfg.get("extensions", []):
            extension_class = extension_register.lookup(extension_name)
            self._extensions.append(extension_class(self, extension_name, drv_cfg))

    def _create_function(self, txn, name, nargs, pyfunc):
        txn._conn.connection.create_function(name, nargs, pyfunc)

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

    def _list_tables_query(self):
        return sas.select(sas.column("name")).select_from(sas.table("sqlite_master"))
