
import sys
from plpipes.runner import simple_init
from plpipes.config import cfg
import plpipes.filesystem as fs
import plpipes.plugin
import logging
import findapp
import subprocess
import os

_con_class_registry = plpipes.plugin.Registry("dbeaver_con_backend", "plpipes.tool.dbeaver.con.driver")

def _con_lookup(name, drv_cfg):
    drv_cfg = cfg.cd(f"db.instance.{name}")
    con_name = drv_cfg.get("driver", "sqlite")
    con_class = _con_class_registry.lookup(con_name)
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

def run(argv):
    opts = simple_init(argv)

    try:
        dbeaver_path = findapp.findapp(cfg.get("fs.command.dbeaver", "dbeaver-ce"),
                                       by_os={'windows': {'binary_name': cfg.get("fs.command.dbeaver-cli", 'dbeaver-cli')}})
    except FileNotFoundError:
        logging.error("DBeaver not found, skipping")
        return

    con_args = []
    instances_cfg = cfg.cd("db.instance")
    for instance in instances_cfg.keys():
        instance_cfg = instances_cfg.cd(instance)
        try:
            dbc = _con_lookup(instance, instance_cfg)
            if (dbc.name == "work") or dbc.active():
                args = dbc.con_args()
                args['folder'] = cfg['fs.project']
                con_args.append(args)
        except ModuleNotFoundError:
            logging.warning(f"Unable to initialize DBeaver connection to DB {instance}, config extractor for {instance_cfg.get('driver')} not found")
        except:
            logging.exception(f"Unable to initialize DBeaver connection to DB {instance}")

    cmd = [dbeaver_path]
    for args in con_args:
        cmd.append("-con"),
        cmd.append("|".join([f"{k}={v}" for k, v in args.items()]))

    logging.info(f"Running command {cmd}")
    run_cmd_detached(cmd)
