import logging
import pathlib
import prql_python as prql

from plpipes.action.registry import register_class

from plpipes.action.driver.sql import _SqlTableCreator, _SqlRunner

class _PrqlTemplated:

    def _read_and_render_sql_template(self, fn):
        prql_code = super()._read_and_render_sql_template(fn)
        return prql.to_sql(prql_code)

class _PrqlTableCreator(_PrqlTemplated, _SqlTableCreator):
    def _source_fn(self):
        return self._cfg["files.table_prql"]

class _PrqlRunner(_PrqlTemplated, _SqlRunner):
    def _source_fn(self):
        return self._cfg["files.prql"]

register_class("prql_script", _PrqlRunner, "prql")
register_class("prql_table_creator", _PrqlTableCreator, "table_prql", "table.prql")

