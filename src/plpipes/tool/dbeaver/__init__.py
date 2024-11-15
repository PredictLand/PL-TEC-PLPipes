
import sys
from plpipes.config import cfg
import plpipes.filesystem as fs
import plpipes.plugin
import logging
import findapp
import subprocess
import os

_conarg_class_registry = plpipes.plugin.Registry("dbeaver_conarg_backend", "plpipes.tool.dbeaver.conarg.driver")

def _conarg_lookup(name, drv_cfg):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    con_name = drv_cfg.get("driver", "sqlite")
    con_class = _conarg_class_registry.lookup(con_name)
    return con_class(name, drv_cfg)

def run_cmd_detached(command):
    kwargs = {
        #'stdout'=subprocess.DEVNULL,
        #'stderr'=subprocess.DEVNULL,
        #'stdin'=subprocess.DEVNULL
    }
    if os.name == 'posix':
        kwargs['start_new_session'] = True
    elif os.name == 'nt':
        kwargs['creationflags'] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS

    return subprocess.Popen(command, **kwargs)

def run_dbeaver(permanent=False, connect=False, instances=None):
    try:
        dbeaver_path = findapp.findapp(cfg.get("fs.command.dbeaver", "dbeaver-ce"),
                                       app_name="dbeaver",
                                       by_os={'windows': {'binary_name': cfg.get("fs.command.dbeaver-cli", 'dbeaver-cli')}})
    except FileNotFoundError:
        logging.error("DBeaver not found, skipping")
        return

    instances_cfg = cfg.cd("db.instance")
    if instances:
        force_active = True
    else:
        instances = instances_cfg.keys()
        force_active = False

    conargs = []
    for instance in instances:
        try:
            dbc = _conarg_lookup(instance,instances_cfg.cd("instance"))
            if force_active or (instance == "work") or dbc.active():
                args = dbc.conargs()
                args['folder'] = cfg['fs.project']
                args['create'] = True
                if permanent:
                    args['save'] = True
                if connect:
                    args['connect'] = True
                conargs.append(args)
        except ModuleNotFoundError:
            logging.warning(f"Unable to initialize DBeaver connection to DB {instance}, config extractor for {instance_cfg.get('driver')} not found")
        except:
            logging.exception(f"Unable to initialize DBeaver connection to DB {instance}")

    if not conargs:
        logging.error("No active DBeaver connections found or initialized, skipping DBeaver launch")
        return

    cmd = [dbeaver_path]
    for args in conargs:
        cmd.append("-con"),
        cmd.append("|".join([f"{k}={v}" for k, v in args.items()]))

    logging.info(f"Running command {cmd}")
    run_cmd_detached(cmd)
