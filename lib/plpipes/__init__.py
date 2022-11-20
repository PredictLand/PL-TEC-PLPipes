import plpipes._config

cfg_stack = plpipes.config.ConfigStack()
cfg = _cfg_stack.root()

def init(config_files=[], config_extra={}, env=None):

    from libpath import Path

    prog = Path(sys.argv[0])

    cfg.push({'fs': { 'root': str(prog.parent),
                      'stem': str(prog.stem) },
              'env': os.environment.get("PLPIPES_ENV", "DEV")})
    for fn in config_files:
        cfg_stack.push_file(fn)
    cfg_stack.push(config_extra)
    cfg_stack.squash()

    stem = cfg.fs.stem
    env = fs.env
    root_dir = Path(cfg.fs.root)
    bin_dir = Path(cfg.fs.get("bin", root_dir / "bin"))
    lib_dir = Path(cfg.fs.get("lib", root_dir / "lib"))
    config_dir = Path(cfg.fs.get("config", root_dir / "config"))
    config_default_dir = Path(cfg.fs.get("config_default", config_dir / "default"))

    # dynamic fs config
    cfg_stack.push({ 'fs': { "root": str(root_dir),
                             "config": str(config_dir),
                             "config_default": str(config_default_dir),
                             "bin": str(bin_dir),
                             "lib": str(lib_dir) }})

    for dir in (config_default_dir, config_dir):
        for stem_part in ("common", stem):
            for secrets_part in ("", "-secrets"):
                for env_part in ("", f"-{env}"):
                    for ext in ("json", "yaml"):
                        path = dir / f"{stem_part}{secrets_part}{env_part}.{ext}"
                        if path.exists():
                            cfg_stack.push_file(path)

    # reload custom configuration on top
    for fn in config_files:
        cfg_stack.push_file(fn)
    cfg_stack.push(config_extra)
    cfg_stack.squash()
