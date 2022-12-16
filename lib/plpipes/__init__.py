import plpipes.config
import logging
import sys
import os

cfg_stack = plpipes.config.ConfigStack()
cfg = cfg_stack.root()

_config0 = { 'db': { 'instance': { 'work': {},
                                   'input': {},
                                   'output': {} }},
             'env': os.environ.get("PLPIPES_ENV", "dev"),
             'logging': { 'level': os.environ.get("PLPIPES_LOGLEVEL", "info") } }

def init(config={}, config_files=[]):
    from pathlib import Path

    # frame  0: command line arguments
    # frame -1: custom configuration files
    # frame -2: standard configuration files

    cfg.merge(_config0, frame=-2)

    for k, v in config.items():
        cfg.merge(v, key=k, frame=0)

    prog = Path(sys.argv[0])
    default_stem = str(prog.stem)
    default_log_level = "INFO"

    logging.getLogger().setLevel(cfg["logging.level"].upper())

    for fn in config_files:
        cfg.merge_file(fn, frame=-1)
        logging.getLogger().setLevel(cfg["logging.level"].upper())

    for dir_key in (False, True):
        for stem_key in (False, True):
            for secrets_part in ("", "-secrets"):
                for env_key in (False, True):
                    for ext in ("json", "yaml"):
                        # The following values can be changed as
                        # config files are read, so they are
                        # recalculated every time:

                        env         = cfg.get('env', 'dev')
                        stem        = cfg.get('fs.stem', default_stem)
                        root_dir    = Path(cfg.get('fs.root'   , prog.parent.parent.absolute()))
                        config_dir  = Path(cfg.get('fs.config' , root_dir / "config"))
                        default_dir = Path(cfg.get('fs.default', root_dir / "default"))

                        env_part  = f"-{env}"  if env_key  else ""
                        stem_part = stem       if stem_key else "common"
                        dir       = config_dir if dir_key  else default_dir
                        path      = dir / f"{stem_part}{secrets_part}{env_part}.{ext}"
                        if path.exists():
                            cfg.merge_file(path, frame=-2)
                            logging.getLogger().setLevel(cfg.logging.level.upper())
                        else:
                            logging.debug(f"Configuration file {path} not found")

    cfg.squash_frames()

    # calculate configuration for file system paths and set it

    root_dir = Path(cfg.setdefault('fs.root' , prog.parent.parent.absolute()))
    cfg.setdefault('fs.bin'    , root_dir   / "bin")
    cfg.setdefault('fs.lib'    , root_dir   / "lib")
    cfg.setdefault('fs.config' , root_dir   / "config")
    cfg.setdefault('fs.default', config_dir / "default")
    cfg.setdefault('fs.input'  , root_dir   / "input")
    cfg.setdefault('fs.output' , root_dir   / "output")
    cfg.setdefault('fs.work'   , root_dir   / "work")
    cfg.setdefault('fs.actions', root_dir   / "actions")
    cfg.setdefault('fs.stem'   , default_stem)

    return True

def run_action(name):
    import plpipes.action
    plpipes.action.run(name)
