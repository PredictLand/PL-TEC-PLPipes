import logging
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg
import subprocess
from pathlib import Path
import yaml
from contextlib import contextmanager
import os
import shutil
import json

def _read_yaml_header(fn):
    with open(fn, "r") as f:
        in_yaml = False
        yaml = []

        for line in f.readlines():
            line = line.rstrip()
            if in_yaml:
                if line == "---":
                    logging.debug(f"YAML header read from Quarto file at {fn}")
                    return "\n".join(*yaml, "")
                else:
                    yaml.append(line)
            else:
                if line == '---':
                    in_yaml = True
                elif line != '':
                    return None
        else:
            if in_yaml:
                raise Exception("YAML header never closed")
        return None

def _patch_qmd(source, dest, config):
    patch = """```{python}
#| echo: false
import plpipes.action.driver.quarto
plpipes.action.driver.quarto._init_plpipes(""" + repr(str(config.absolute())) + """)
```
"""
    with open(source, "r") as src_f:
        with open(dest, "w") as dest_f:
            state = "start"
            for line in src_f.readlines():
                if state != "copy":
                    stripped = line.rstrip()
                    if state == "start":
                        if stripped == "---":
                            state = "in_yaml"
                        elif stripped != "":
                            dest_f.write(patch)
                            state = "copy"
                    elif state == "in_yaml":
                        if stripped == "---":
                            dest_f.write(patch)
                            state = "copy"
                    else:
                        raise Exception("Internal error, unexpected state")
                dest_f.write(line)

def _init_plpipes(config):
    import plpipes.init
    plpipes.init.init(config_files=[config])

@contextmanager
def _cd(newdir):
    prevdir = os.getcwd()
    os.chdir(os.path.expanduser(newdir))
    try:
        yield
    finally:
        os.chdir(prevdir)

class _QuartoRunner(Action):

    def __init__(self, name, action_cfg):
        self._path = Path(action_cfg["files.qmd"])
        header = _read_yaml_header(self._path)
        if header is not None:
            action_cfg.merge(yaml.load(header), key="quarto")
            action_cfg.copydefaults(action_cfg.cd("quarto.plpipes"),
                                    ["dest.dir", "dest.key", "dest.file", "dest.format"])
        super().__init__(name, action_cfg)

    def do_it(self):
        qmd_path = Path(self._path).absolute()
        dest_key = self._cfg.setdefault("dest.key", "work")
        dest_dir = self._cfg.setdefault("dest.dir", "")
        dest_format = self._cfg.setdefault("dest.format", "html")
        dest_file = self._cfg.setdefault("dest.file", str(qmd_path.with_suffix("." + dest_format).name))

        dest_file_path = Path(cfg[f"fs.{dest_key}"]) / dest_dir / dest_file
        dest_dir_path = dest_file_path.parent
        dest_dir_path.mkdir(parents=True, exist_ok=True)

        env = os.environ.copy()
        env['PLPIPES_ROOT_DIR'] = str(Path(cfg["fs.root"]).absolute())

        with _cd(dest_dir_path):
            dest_work_path = Path("quarto")
            dest_work_path.mkdir(parents=True, exist_ok=True)
            qmd_copy_path = dest_work_path / qmd_path.name
            dest_cfg_path = (dest_work_path / qmd_path.name).with_suffix(".json")

            # Dump the configuration into a temporary file
            with open(dest_cfg_path, "w") as f:
                json.dump(cfg.to_tree(), f)

            _patch_qmd(qmd_path, dest=qmd_copy_path, config=dest_cfg_path)
            #shutil.copyfile(qmd_path, qmd_copy_path)

            subprocess.run(["quarto", "render", qmd_copy_path,
                            "--output", dest_file_path.name,
                            "--to", dest_format,
                            "--no-execute-daemon"],
                           env=env)

register_class("quarto", _QuartoRunner, "qmd")
