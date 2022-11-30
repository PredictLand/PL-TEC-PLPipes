import cfg
import pathlib
import plpipes.config
_cache = {}

suffix_aliases = { 'yaml': ['yml'] }
suffixes = ['yaml', 'json', 'table.sql', 'py']

def _find_action_files(fn_start):
    files = {}
    if fn_start.is_dir():
        files["dir"] = str(fn_start)
    for suffix in suffixes:
        for alias in (suffix, *suffix_aliases.get(suffix, [])):
            fn = fn_start.with_suffix(alias)
            if fn.exists():
                files[suffix] = str(fn)
                break
    return files

def lookup(name):

    if name not in _cache:
        actions_dir = pathlib.Path(cfg.fs.actions)
        files = _find_action_files((actions_dir / name.replace(".", "/")).absolute())
        acfg_prefix = "actions." + ".children.".join(name.split("."))
        acfg_low = { 'files': files }
        if "py" in files:
            acfg["type"] = "python"
        elif "sql" in files:
            acfg["type"] = "sql"

        acfg = plpipes.config.ConfigStack()
        acfg.push(acfg_low, prefix=acfg_prefix)
        acfg.push(cfg._to_tree())
        except KeyError: pass
        for ext in ("yaml", "json"):
            if ext in files:
                acfg.push_file(files[ext], prefix=acfg_prefix)
        try: acfg.push(cfg._to_tree)
        except KeyError: pass

        try:
            acfg_branch = acfg._browse(acfg_prefix)
            action_type = acfg_branch.type
        except IndexError:
            raise ValueError(f"Action {name} has no type declared or action file not found")

        action_class = _class_lookup(action_type)

        _cache[name] = action_class(name, acfg_branch)

    return _cache[name]

_class_registry = {}

def _class_lookup(type):
    if type in _class_registry:
        return _class_registry(type)
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

        path = pathlib.Path(self._cfg.files.sql)
        with open(path, "r") as f:
            sql = f.read()
        table_name = path.stem
        from plpipes.database import create_table
        create_table(table_name, sql)


class _PythonRunner(Action):

    def __init__(self, name, acfg):
        super().__init__(name, acfg)
        self._new_action = None

    def replace_action(self, new_action_class):
        self._new_action_class = new_action_class

    def _do_it(self, indent):

        if self._new_action_class is None:

            path = pathlib.Path(self._cfg.files.py)
            with open(path, "r") as f:
                code = f.read()
            exec(code, {}, {'replace_action': self.replace_action})

            if self._new_action_class is None:
                return

            self._new_action = self._new_action_class(self._name, self._cfg)

        self._new_action._do_it(indent)
