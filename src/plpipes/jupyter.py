"""
plpipes.jupyter - a Jupyter extension module for initializing and managing the PLPipes framework.
"""

from IPython.core.magic import Magics, magics_class, line_magic, needs_local_scope

import sys
import os
import logging
import pathlib

import plpipes.init
import plpipes.config
import plpipes.database

@magics_class
class PLPipesMagics(Magics):
    """
    A class to define Jupyter magic commands for the PLPipes framework.
    """

    def __init__(self, shell):
        """
        Initializes the PLPipesMagics class.

        Parameters:
            shell: The IPython shell instance.
        """
        super().__init__(shell)
        self.initialized = False

    @needs_local_scope
    @line_magic
    def plpipes(self, line, local_ns):
        """
        Magic function to initialize the PLPipes framework based on the given configuration.

        Parameters:
            line (str): The line of input provided to the magic command.
            local_ns (dict): The local namespace to inject configurations and database objects.
        """
        # import sql.connection

        if self.initialized:
            logging.warn("PLPipes framework already initialized. You will have to restart the kernel if you want to load a new configuration")
        else:
            if "PLPIPES_ROOT_DIR" in os.environ:
                root_dir = pathlib.Path(os.environ["PLPIPES_ROOT_DIR"]).resolve(strict=True)
            else:
                root_dir = pathlib.Path(os.getcwd()).resolve(strict=True)
                while not (root_dir/"config").is_dir():
                    old_root_dir = root_dir
                    root_dir = root_dir.parent
                    if old_root_dir == root_dir:
                        raise RuntimeError(f"PLPipes project root dir not found (cwd: {os.getcwd()}")

            stem = line.strip()
            if stem == "":
                stem = "jupyter"
            logging.info("Initializing PLPipes framework")
            plpipes.init.init({"fs.stem": stem, "fs.root": str(root_dir)})

            # class MyConnection(sql.connection.Connection):
            #     @classmethod
            #     def set(cls, descriptor, *args, **argkw):
            #         # Introduce @@name shortcut for referring to PLPipes databases
            #         if isinstance(descriptor, str) and descriptor.startswith("@@"):
            #             dbname = descriptor[2:]
            #             descriptor = str(plpipes.database.engine(dbname).url)
            #         return super().set(descriptor, *args, **argkw)
            # sql.connection.Connection = MyConnection

            sys.path.append(plpipes.config.cfg["fs.lib"])

            self.initialized = True

        local_ns["cfg"] = plpipes.config.cfg
        local_ns["db"] = plpipes.database
        local_ns["create_table"] = plpipes.database.create_table
        local_ns["query"] = plpipes.database.query

        for dir in ("root", "input", "work", "output"):
            local_ns[f"{dir}_dir"] = pathlib.Path(plpipes.config.cfg[f"fs.{dir}"])

        # sql.connection.Connection.set("@@work", False)


def load_ipython_extension(ipython):
    """
    Loads the PLPipes magic extension in the IPython environment.

    Parameters:
        ipython: The IPython instance to load the extension into.
    """
    global former_connection_class
    ipython.register_magics(PLPipesMagics)
    # ipython.extension_manager.load_extension("sql")
