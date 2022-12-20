import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

import jinja2

def _read_and_render_sql_template(fn):

    with open(fn, "r") as f:
        src = f.read()

    env = jinja2.Environment()
    return env.from_string(src).render()

class _SqlTableCreator(Action):

    def do_it(self):
        path = pathlib.Path(self._cfg["files.table_sql"])
        table_name = path.stem.split(".")[0]

        sql = _read_and_render_sql_template(path)

        from plpipes.database import create_table
        create_table(table_name, sql_code)


class _SqlRunner(Action):

    def do_it(self):
        path = pathlib.Path(self._cfg["files.sql"])
        sql_code = _read_and_render_sql_template(path)
        from plpipes.database import execute_script
        execute_script(sql_code)

register_class("sql_script", _SqlRunner, "sql")
register_class("sql_table_creator", _SqlTableCreator, "table_sql", "table.sql")
