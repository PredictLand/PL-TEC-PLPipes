import logging
from plpipes.action.base import Action
from plpipes.action.registry import register_class
from plpipes.config import cfg
import plpipes.filesystem as fs
import subprocess
from pathlib import Path
import yaml
from contextlib import contextmanager
import os
import tempfile
import json
import shutil
import datetime
import dateparser

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
                          section='work',
                          dir='',
                          format="html",
                          append_date=False,
                          insert_date=False,
                          date=None)
        default_file = self._path.with_suffix("." + dcfg["format"]).name
        dcfg.copydefaults(scfg, file=default_file)
        super().__init__(name, action_cfg)

    def do_it(self):
        logging.debug(f"Action config: {self._cfg.to_tree()}")
        rel_path = Path(self._cfg['dest.dir']) / self._cfg['dest.file']

        if self._cfg['dest.insert_date'] or self._cfg['dest.append_date']:
            date_raw = self._cfg['dest.date']
            if date_raw is None:
                date = datetime.datetime.strptime(cfg['run.as_of_date_normalized'], "%Y%m%dT%H%M%SZ0")
            else:
                date = dateparser.parse(date_raw)
                if date is None:
                    raise ValueError(f"Unable to parse date {date_raw}")

            if self._cfg['dest.insert_date']:
                rel_path = date.strftime(str(rel_path))
                logging.debug(f"Inserting date into target filename ({date} --> {rel_path})")
            else:
                rel_path_parent = rel_path.parent
                rel_path_stem = rel_path.stem
                rel_path_suffix = rel_path.suffix
                rel_path = rel_path_parent / (rel_path_stem + '-' + date.strftime("%Y%m%dT%H%M%SZ0") + rel_path_suffix)
                logging.debug(f"Appending date into target filename ({rel_path})")

        target_path = fs.path(rel_path, section=self._cfg['dest.section'])
        stem = target_path.stem

        with tempfile.TemporaryDirectory() as workdir:
            # workdir = "/home/salva/tmp/report"
            workdir = Path(workdir)
            temp_target_path = workdir / target_path.name
            temp_cfg_path = workdir / "config.json"
            with open(temp_cfg_path, "w") as f:
                json.dump(cfg.to_tree(), f)

            temp_qmd_path = workdir / f"{stem}.qmd"
            _patch_qmd(self._path, temp_qmd_path, temp_cfg_path)

            env = os.environ.copy()
            env['PLPIPES_ROOT_DIR'] = str(fs.path(".", section="root").absolute())

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
