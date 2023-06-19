from plpipes.config import cfg

import sys
import os
import logging
import pathlib
import datetime
import dateparser

_config0 = {'db': {'instance': {'work': {},
                                'input': {},
                                'output': {}}},
            'env': os.environ.get("PLPIPES_ENV", "dev"),
            'run': {'as_of_date': 'now'},
            'logging': {'level': os.environ.get("PLPIPES_LOGLEVEL", "info")}}

def init(*configs, config_files=[]):
    from pathlib import Path

    # frame 0: command line arguments
    # frame 1: custom configuration files
    # frame 2: standard configuration files

    cfg.merge(_config0, frame=2)

    for config in configs:
        for k, v in config.items():
            cfg.merge(v, key=k, frame=0)

    prog = Path(sys.argv[0])
    default_stem = str(prog.stem)
    default_log_level = "INFO"

    logging.getLogger().setLevel(cfg["logging.level"].upper())

    for fn in config_files:
        cfg.merge_file(fn, frame=1)
        logging.getLogger().setLevel(cfg["logging.level"].upper())

    global_cfg_dir = Path.home() / ".config/plpipes"
    for suffix in ("", "-secrets"):
        for ext in ("json", "yml", "yaml"):
            path = global_cfg_dir / f"plpipes{suffix}.{ext}"
            if path.exists():
                cfg.merge_file(path, frame=2)
                logging.getLogger().setLevel(cfg["logging.level"].upper())
            else:
                logging.debug(f"Configuration file {path} not found")

    for dir_key in (False, True):
        for stem_key in (False, True):
            for secrets_part in ("", "-secrets"):
                for env_key in (False, True):
                    for ext in ("json", "yml", "yaml"):
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
                            cfg.merge_file(path, frame=2)
                            logging.getLogger().setLevel(cfg["logging.level"].upper())
                        else:
                            logging.debug(f"Configuration file {path} not found")

    cfg.squash_frames()

    cfg.setdefault('fs.stem', default_stem)

    # calculate configuration for file system paths and set it
    root_dir = Path(cfg.setdefault('fs.root', prog.parent.parent.absolute()))
    for e in ('bin', 'lib', 'config', 'default',
              'input', 'output', 'work', 'actions'):
        cfg.setdefault("fs." + e, root_dir / e)

    as_of_date = dateparser.parse(cfg['run.as_of_date'])
    as_of_date = as_of_date.astimezone(datetime.timezone.utc)
    cfg['run.as_of_date_normalized'] = as_of_date.strftime("%Y%m%dT%H%M%SZ0")

    _filelog_setup()

    logging.debug(f"Configuration: {repr(cfg.to_tree())}")

    return True


def _filelog_setup():
    if cfg.get("logging.log_to_file", True):
        dir = cfg.get("logging.log_dir", "logs")
        dir = pathlib.Path(cfg["fs.root"]) / dir
        dir.mkdir(exist_ok=True, parents=True)
        ts = datetime.datetime.utcnow().strftime('%y%m%dT%H%M%SZ')
        name = f"log-{ts}.txt"

        last = dir / "last-log.txt"
        try:
            if last.exists():
                last.unlink()
            (dir / "last-log.txt").symlink_to(name)
        except:
            logging.warn(f"Unable to create link for {last}", exc_info=True)

        logger = logging.getLogger()

        fh = logging.FileHandler(str(dir / name))
        fh.setLevel(logger.getEffectiveLevel())
        try:
            formatter = logger.handlers[0].formatter
            fh.setFormatter(formatter)
        except:
            logging.warn("Can't set format for file logger", exc_info=True)

        logger.addHandler(fh)
