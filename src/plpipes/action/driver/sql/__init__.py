import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

from plpipes.config import cfg

class _SqlTemplated(Action):
    def do_it(self):
        fn = self._source_fn()
        sql_code = self._read_and_render_sql_template(fn)
        self._run_sql(sql_code)

    def _read_and_render_sql_template(self, fn):
        engine = self._cfg.get("engine", "jinja2")

        with open(fn, "r") as f:
            src = f.read()

        if engine == "jinja2":
            from . import jinja2
            return jinja2.render_template(src, {'cfg': cfg, 'acfg': self._cfg})

        raise ValueError(f"Unsupported SQL template engine {engine}")

    def _short_name_to_table(self):
        name = self.short_name()
        return name.replace("-", "_")

class _SqlTableCreator(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.table_sql"]

    def _run_sql(self, sql_code):
        from plpipes.database import create_table
        create_table(self._short_name_to_table(), sql_code)

class _SqlViewCreator(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.view_sql"]

    def _run_sql(self, sql_code):
        from plpipes.database import create_view
        create_view(self._short_name_to_table(), sql_code)

class _SqlRunner(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.sql"]

    def _run_sql(self, sql_code):
        from plpipes.database import execute_script
        execute_script(sql_code)

register_class("sql_script", _SqlRunner, "sql")
register_class("sql_table_creator", _SqlTableCreator, "table_sql", "table.sql")
register_class("sql_view_creator", _SqlViewCreator, "view_sql", "view.sql")
