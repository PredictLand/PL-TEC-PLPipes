"""
This module contains drivers for handling SQL-related actions within the plpipes framework.

The supported actions include executing raw SQL scripts, creating SQL tables from templates,
and creating SQL views from templates. Each action driver is responsible for loading the
appropriate SQL configuration, rendering templates, and executing the SQL commands
against the specified database.
"""

import logging
import pathlib

from plpipes.action.base import Action
from plpipes.action.registry import register_class

from plpipes.config import cfg

class _SqlTemplated(Action):
    """
    Base class for SQL templated actions.

    This class handles loading and rendering SQL templates, as well as managing
    the configuration and source file extraction.
    """
    def __init__(self, *args, **kwargs):
        """
        Initializes the _SqlTemplated action.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.
        """
        super().__init__(*args, **kwargs)
        acfg, source = self._break_source_file()
        self._cfg.merge(acfg)
        self._source = source

    def _break_source_file(self):
        """
        Extracts the YAML header from the source file.

        Returns:
            A tuple containing the extracted configuration and the remaining
            lines of the source file.

        Raises:
            ValueError: If the YAML header is not properly closed.
        """
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
        """
        Executes the SQL code rendered from the source template.
        """
        sql_code = self._render_source_template()
        self._run_sql(sql_code)

    def _render_source_template(self):
        """
        Renders the SQL source template using the specified template engine.

        Returns:
            The rendered SQL code.

        Raises:
            ValueError: If an unsupported SQL template engine is specified.
        """
        engine = self._cfg.get("engine", "jinja2")

        if engine == "jinja2":
            from . import jinja2
            return jinja2.render_template(self._source, {'cfg': cfg, 'acfg': self._cfg, 'str': str})

        raise ValueError(f"Unsupported SQL template engine {engine}")

    def _short_name_to_table(self):
        """
        Converts the short name to a valid table name.

        Returns:
            A string representing the table name.
        """
        name = self.short_name()
        return name.replace("-", "_")

class _SqlTableCreator(_SqlTemplated):
    """
    Action for creating SQL tables from a template.
    """
    def _source_fn(self):
        """
        Retrieves the source file name for the table SQL.

        Returns:
            The source file path for the table SQL.
        """
        return self._cfg["files.table_sql"]

    def _run_sql(self, sql_code):
        """
        Executes the SQL code to create a table in the specified database.

        Args:
            sql_code: The SQL code to be executed.
        """
        import plpipes.database as db

        source_db = self._cfg.get("source_db", "work")
        target_db = self._cfg.get("target_db", "work")
        if source_db == target_db:
            db.create_table(self._short_name_to_table(), sql_code, db=source_db)
        else:
            iter = db.query(sql_code, db=source_db)
            db.create_table(self._short_name_to_table(), iter, db=target_db)

class _SqlViewCreator(_SqlTemplated):
    """
    Action for creating SQL views from a template.
    """
    def _source_fn(self):
        """
        Retrieves the source file name for the view SQL.

        Returns:
            The source file path for the view SQL.
        """
        return self._cfg["files.view_sql"]

    def _run_sql(self, sql_code):
        """
        Executes the SQL code to create a view in the database.

        Args:
            sql_code: The SQL code to be executed.
        """
        from plpipes.database import create_view
        create_view(self._short_name_to_table(), sql_code)

class _SqlRunner(_SqlTemplated):
    """
    Action for executing raw SQL scripts.
    """
    def _source_fn(self):
        """
        Retrieves the source file name for the SQL script.

        Returns:
            The source file path for the SQL script.
        """
        return self._cfg["files.sql"]

    def _run_sql(self, sql_code):
        """
        Executes the SQL code from a script.

        Args:
            sql_code: The SQL code to be executed.
        """
        from plpipes.database import execute_script
        execute_script(sql_code)

register_class("sql_script", _SqlRunner, "sql")
register_class("sql_table_creator", _SqlTableCreator, "table_sql", "table.sql")
register_class("sql_view_creator", _SqlViewCreator, "view_sql", "view.sql")
