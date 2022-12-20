import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

def _read_and_render_sql_template(fn):



    env = jinja2.Environment()
    return env.from_string(src).render()

class _SqlTemplated(Action):
    def do_it(self):
        fn = self._source_fn()
        sql_code = _read_and_render_sql_template(fn)
        self._run_sql(sql_code)

    def _read_and_render_sql_template(self, fn):
        engine = self._cfg.get("engine", "jinja2")

        with open(fn, "r") as f:
            src = f.read()

        if engine == "jinja2":
            import plpipes.action.sql.jinja2
            return pipipes.action.sql.jinja2.render_template(src)

        raise ValueError(f"Unsupported SQL template engine {engine}")

class _SqlTableCreator(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.table_sql"]

    def _run_sql(self, sql_code):
        from plpipes.database import create_table
        create_table(self.short_name(), sql_code)

class _SqlRunner(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.sql"]

    def _run_sql(self, sql_code):
        from plpipes.database import execute_script
        execute_script(sql_code)

register_class("sql_script", _SqlRunner, "sql")
register_class("sql_table_creator", _SqlTableCreator, "table_sql", "table.sql")
