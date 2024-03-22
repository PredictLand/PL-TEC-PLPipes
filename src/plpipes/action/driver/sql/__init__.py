import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

from plpipes.config import cfg

class _SqlTemplated(Action):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        acfg, source = self._break_source_file()
        self._cfg.merge(acfg)
        self._source = source

    def _break_source_file(self):
        # Extract the YAML header from the source file.
        # This is a hacky state machine.

        fn = self._source_fn()

        with open(fn, "r") as f:
            in_yaml = False
            yaml = []

            lines = f.readlines()
            ix = 0
            while ix < len(lines):
                sl = lines[ix].rstrip()
                ix += 1
                if sl == '---':
                    yaml_start = ix
                    while ix < len(lines):
                        sl = lines[ix].rstrip()
                        if sl == '---':
                            import yaml
                            acfg = yaml.safe_load("".join(lines[yaml_start:ix]))
                            lines = "".join(lines[:yaml_start] +
                                            ["--- YAML header removed.\n"] * (ix - yaml_start) +
                                            lines[ix:])
                            return acfg, lines
                        ix += 1
                    else:
                        raise ValueError("YAML header never closed")

                elif sl != '':
                    break
            return {}, "".join(lines)

    def do_it(self):
        sql_code = self._render_source_template()
        self._run_sql(sql_code)

    def _render_source_template(self):
        engine = self._cfg.get("engine", "jinja2")

        if engine == "jinja2":
            from . import jinja2
            return jinja2.render_template(self._source, {'cfg': cfg, 'acfg': self._cfg, 'str': str})

        raise ValueError(f"Unsupported SQL template engine {engine}")

    def _short_name_to_table(self):
        name = self.short_name()
        return name.replace("-", "_")

class _SqlTableCreator(_SqlTemplated):
    def _source_fn(self):
        return self._cfg["files.table_sql"]

    def _run_sql(self, sql_code):
        import plpipes.database as db

        source_db = self._cfg.get("source_db", "work")
        target_db = self._cfg.get("target_db", "work")
        if source_db == target_db:
            db.create_table(self._short_name_to_table(), sql_code, db=source_db)
        else:
            iter = db.query(sql_code, db=source_db)
            db.create_table(self._short_name_to_table(), iter, db=target_db)

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
