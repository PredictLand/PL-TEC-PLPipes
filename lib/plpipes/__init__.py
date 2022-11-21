import plpipes.config
import logging
import sys
import os

cfg_stack = plpipes.config.ConfigStack()
cfg = cfg_stack.root()

def init(config_files=[], config_extra={}, env=None):

    from pathlib import Path

    prog = Path(sys.argv[0])

    cfg_stack.push({'fs': { 'root': str(prog.parent.parent),
                            'stem': str(prog.stem) },
                    'env': os.environ.get("PLPIPES_ENV", "dev"),
                    'logging': { 'level': os.environ.get("PLPIPES_LOGLEVEL", "info") } })

    cfg_stack.push(config_extra)
    logging.getLogger().setLevel(cfg.logging.level.upper())

    for fn in config_files:
        cfg_stack.push_file(fn)
    cfg_stack.push(config_extra)
    cfg_stack.squash()

    logging.getLogger().setLevel(cfg.logging.level.upper())

    stem = cfg.fs.stem
    env = cfg.env
    root_dir = Path(cfg.fs.root).absolute()
    bin_dir = Path(cfg.fs.get("bin", root_dir / "bin"))
    lib_dir = Path(cfg.fs.get("lib", root_dir / "lib"))
    config_dir = Path(cfg.fs.get("config", root_dir / "config"))
    config_default_dir = Path(cfg.fs.get("config_default", config_dir / "default"))
    input_dir = Path(cfg.fs.get("input", root_dir / "input"))
    output_dir = Path(cfg.fs.get("output", root_dir / "output"))
    work_dir = Path(cfg.fs.get("work", root_dir / "work"))

    # dynamic fs config
    cfg_stack.push({ 'fs': { "root": str(root_dir),
                             "config": str(config_dir),
                             "config_default": str(config_default_dir),
                             "bin": str(bin_dir),
                             "lib": str(lib_dir),
                             "input": str(input_dir),
                             "output": str(output_dir),
                             "work": str(work_dir) }})

    for dir in (config_default_dir, config_dir):
        for stem_part in ("common", stem):
            for secrets_part in ("", "-secrets"):
                for env_part in ("", f"-{env}"):
                    for ext in ("json", "yaml"):
                        path = dir / f"{stem_part}{secrets_part}{env_part}.{ext}"
                        if path.exists():
                            cfg_stack.push_file(path)
                        else:
                            logging.debug(f"Configuration file {path} not found")

    # reload custom configuration on top
    for fn in config_files:
        cfg_stack.push_file(fn)
    cfg_stack.push(config_extra)
    cfg_stack.squash()

    logging.getLogger().setLevel(cfg.logging.level.upper())


    print(cfg)
