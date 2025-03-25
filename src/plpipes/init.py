from plpipes.config import cfg

import sys
import os
import logging
import pathlib
import datetime
import friendlydateparser

_initialized = False

_config0 = {'db': {'instance': {'work': {},
                                'input': {},
                                'output': {}}},
            'env': os.environ.get("PLPIPES_ENV",
                                  "dev"),
            'run': {'as_of_date': 'now'},
            'logging': {'level': os.environ.get("PLPIPES_LOGLEVEL",
                                                "INFO"),
                        'level_file': os.environ.get("PLPIPES_LOGLEVEL_FILE",
                                                     "INFO")}}

def init(*configs, config_files=[]):
    """Initializes the PLPipes configuration.

    This function merges provided configurations, command line arguments, and environment variables
    to set up the application. It also initializes logging and file system paths.

    Args:
        *configs: Additional configuration dictionaries.
        config_files: List of custom configuration files to be merged.

    Returns:
        bool: True if initialization is successful.
    """
    from pathlib import Path

    global _initialized
    if _initialized:
        logging.warning("Reinitialicing PLPipes")

    # frame 0: command line arguments
    # frame 1: custom configuration files
    # frame 2: standard configuration files

    cfg.merge(_config0, frame=2)

    for config in configs:
        for k, v in config.items():
            cfg.merge(v, key=k, frame=0)

    prog = Path(sys.argv[0])
    default_stem = str(prog.stem)

    # update cfg to get the log levels (normal and file)
    for fn in config_files:
        cfg.merge_file(fn, frame=1)

    list_configuration_files_not_found = []

    global_cfg_dir = Path.home() / ".config/plpipes"
    for suffix in ("", "-secrets"):
        for ext in ("json", "yml", "yaml"):
            path = global_cfg_dir / f"plpipes{suffix}.{ext}"
            if path.exists():
                cfg.merge_file(path, frame=2)
            else:
                list_configuration_files_not_found.append(path)

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
                        else:
                            list_configuration_files_not_found.append(path)

    # set the root log level as NOTSET, which is the deepest level; it's like
    # this because all the other handlers, even if they have ther own levels,
    # will not log anything if the root level is higher than their level
    logging.getLogger().setLevel("NOTSET")

    cfg.squash_frames()

    cfg.setdefault('fs.stem', default_stem)

    # calculate configuration for file system paths and set it
    root_dir = Path(cfg.setdefault('fs.root', prog.parent.parent.absolute()))
    for e in ('bin', 'lib', 'config', 'default',
              'input', 'output', 'work', 'actions',
              'resources'):
        cfg.setdefault("fs." + e, root_dir / e)

    init_run_as_of_date()

    _log_setup()

    logging.debug(f"List of configuration files not found: {list_configuration_files_not_found}")

    logging.debug(f"Configuration: {repr(cfg.to_tree())}")

    _initialized = True

    return True

def init_run_as_of_date():
    """Initializes the 'as_of_date' configuration entry.

    It sets the 'run.as_of_date_normalized' entry in the configuration
    based on the specified or default 'run.as_of_date'.

    Returns:
        None
    """
    date = cfg.setdefault('run.as_of_date', 'now')
    as_of_date = friendlydateparser.parse_datetime(date)
    as_of_date = as_of_date.astimezone(datetime.timezone.utc)
    cfg['run.as_of_date_normalized'] = as_of_date.strftime("%Y%m%dT%H%M%SZ0")

def _log_setup():
    """This function sets up the logging system. It is called by init().

    Args:
        None.
    Returns:
        None
    """

    # get the logger
    logger = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s:%(levelname)s:%(name)s:%(message)s")

    # if logging.log_to_file is True, then we set up a file handler
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
            import platform
            if platform.system() != 'Windows':
                logging.warn(f"Unable to create link for {last}", exc_info=True)

        fh = logging.FileHandler(str(dir / name))
        fh.setLevel(cfg["logging.level_file"].upper())
        fh.setFormatter(formatter)

    # we set up a console handler
    ch = logging.StreamHandler()
    ch.setLevel(cfg["logging.level"].upper())

    if ch.stream.isatty():
        import colorlog
        ch.setFormatter(colorlog.ColoredFormatter("%(log_color)s%(asctime)s:%(levelname)s:%(name)s:%(reset)s%(white)s%(message)s",
                                                  log_colors={'DEBUG': 'cyan',
                                                              'INFO': 'green',
                                                              'WARNING': 'yellow',
                                                              'ERROR': 'red',
                                                              'CRITICAL': 'red,bg_white'}))
    else:
        ch.setFormatter(formatter)

    # we force the handlers as ch and fh
    if cfg.get("logging.log_to_file", True):
        logger.handlers = [ch, fh]
    else:
        logger.handlers = [ch]

    # we do not allow propagations to other handlers
    logger.propagate = False
