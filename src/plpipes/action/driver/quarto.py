import logging
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg
import subprocess
from pathlib import Path
import yaml
from contextlib import contextmanager
import os
import tempfile
import json
import shutil

def _read_yaml_header(fn):
    with open(fn, "r") as f:
        in_yaml = False
        yaml = []

        for line in f.readlines():
            line = line.rstrip()
            if in_yaml:
                if line == "---":
                    logging.debug(f"YAML header read from Quarto file at {fn}")
                    return "\n".join([*yaml, ""])
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
                            state = "after_yaml"
                    elif state == "after_yaml":
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
        self._path = Path(action_cfg["files.qmd"]).absolute()
        header = _read_yaml_header(self._path)
        if header is not None:
            action_cfg.merge(yaml.load(header, Loader=yaml.SafeLoader), key="quarto")
        dcfg = action_cfg.cd("dest")
        scfg = action_cfg.cd("quarto.plpipes.dest")
        dcfg.copydefaults(scfg,
                          key='work',
                          dir='',
                          format="html")
        default_file = self._path.with_suffix("." + dcfg["format"]).name
        dcfg.copydefaults(scfg, file=default_file)
        super().__init__(name, action_cfg)

    def do_it(self):
        logging.debug(f"Action config: {self._cfg.to_tree()}")
        key_dir = Path(cfg["fs." + self._cfg['dest.key']])
        target_path = key_dir / self._cfg['dest.dir'] / self._cfg['dest.file']
        stem = target_path.stem

        with tempfile.TemporaryDirectory() as workdir:
            workdir = Path(workdir)
            temp_target_path = workdir / target_path.name
            temp_cfg_path = workdir / "config.json"
            with open(temp_cfg_path, "w") as f:
                json.dump(cfg.to_tree(), f)

            temp_qmd_path = workdir / f"{stem}.qmd"
            _patch_qmd(self._path, temp_qmd_path, temp_cfg_path)

            env = os.environ.copy()
            env['PLPIPES_ROOT_DIR'] = str(Path(cfg["fs.root"]).absolute())

            with _cd(workdir):
                cmd = ["quarto", "render", temp_qmd_path.name,
                       "--output", temp_target_path.name,
                       "--to", self._cfg["dest.format"],
                       "--no-execute-daemon"]

                logging.debug(f"Running quarto: {cmd}, cwd: {os.getcwd()}")
                proc = subprocess.run(cmd, env=env)
                if proc.returncode != 0:
                    raise Exception("Quarto command failed, RC: {proc.returncode}")

                logging.debug("Moving Quarto output files to final destination")
                target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(temp_target_path,
                             target_path)
                aux_dir_path = Path(f"{stem}_files")
                if aux_dir_path.is_dir():
                    shutil.copytree(aux_dir_path,
                                    target_path.parent / aux_dir_path,
                                    dirs_exist_ok=True)

        logging.info(f"Quarto document available at {target_path}")

register_class("quarto", _QuartoRunner, "qmd")
