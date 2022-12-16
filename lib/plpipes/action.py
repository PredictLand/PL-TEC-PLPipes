import pathlib
import logging
import time
from plpipes import cfg

_cache = {}

suffix_aliases = { 'yaml': ['yml'],
                   'table_sql': ['table.sql'] }
suffixes = ['yaml', 'json', 'table_sql', 'py']

def _find_action_files(fn_start):
    files = {}
    if fn_start.is_dir():
        files["dir"] = str(fn_start)
    for suffix in suffixes:
        for alias in (suffix, *suffix_aliases.get(suffix, [])):

            print(f"fn_start: {fn_start}, suffix: {suffix}")

            fn = fn_start.with_suffix("."+alias)
            if fn.exists():
                files[suffix] = str(fn)
                break
    return files

def lookup(name):

    if name not in _cache:
        actions_dir = pathlib.Path(cfg["fs.actions"])
        files = _find_action_files((actions_dir / name.replace(".", "/")).absolute())

        path = "actions." + ".children.".join(name.split("."))
        acfg = cfg.cd(path)

        for ext in ("yaml", "json"):
            if ext in files:
                acfg.merge_file(files[ext], frame=-1)

        for k, v in files.items():
            acfg.setdefault(f"files.{k}", v)

        if "py" in files:
            acfg.setdefault("type", "python_script")
        elif "table_sql" in files:
            acfg.setdefault("type", "sql_table_creator")

        try:
            action_type = acfg["type"]
        except KeyError:
            raise ValueError(f"Action {name} has no type declared or action file not found")

        action_class = _class_lookup(action_type)

        _cache[name] = action_class(name, acfg)

    return _cache[name]

def run(name):
    lookup(name).run()

_class_registry = {}

def _class_lookup(type):
    if type in _class_registry:
        return _class_registry[type]
    raise ValueError(f"Unsupported action type {type}")

def register_class(type, action_class):
    _class_registry[type] = action_class

class Action:
    def __init__(self, name, action_cfg):
        self._name = name
        self._cfg = action_cfg

    def name(self):
        return self._name

    def _do_it(self, indent):
        self.do_it()

    def do_it(self):
        ...

    def run(self, indent=0):
        name = self.name()
        logging.info(f"{' '*indent}Action {name} started")
        start = time.time()
        self._do_it(indent=indent)
        lapse = int(10*(time.time() - start) + 0.5)/10.0
        logging.info(f"{' '*indent}Action {name} done ({lapse}s)")

    def __str__(self):
        return f"<Action {self._name}>"

class _SqlTableCreator(Action):

    def do_it(self):

        path = pathlib.Path(self._cfg["files.table_sql"])
        with open(path, "r") as f:
            sql = f.read()
        table_name = path.stem.split(".")[0]
        from plpipes.database import create_table
        create_table(table_name, sql)

register_class("sql_table_creator", _SqlTableCreator)


class _PythonRunner(Action):

    def __init__(self, name, acfg):
        super().__init__(name, acfg)
        self._new_action = None

    def replace_action(self, new_action_class):
        self._new_action_class = new_action_class

    def _do_it(self, indent):

        if self._new_action_class is None:

            path = pathlib.Path(self._cfg["files.py"])
            with open(path, "r") as f:
                code = f.read()
            exec(code, {}, {'replace_action': self.replace_action})

            if self._new_action_class is None:
                return

            self._new_action = self._new_action_class(self._name, self._cfg)

        self._new_action._do_it(indent)

register_class("python_script", _PythonRunner)
