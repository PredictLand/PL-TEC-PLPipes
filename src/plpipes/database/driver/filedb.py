import pathlib
from plpipes.config import cfg
from plpipes.database.driver.sqlalchemy import SQLAlchemyDriver

class FileDBDriver(SQLAlchemyDriver):
    def __init__(self, name, drv_cfg, driver):
        # if there is an entry for the given name in cfg["fs"] we use
        # that, otherwise we store the db file in the work directory:
        root_dir = pathlib.Path(cfg.get(f"fs.{name}", cfg["fs.work"]))
        fn = root_dir.joinpath(drv_cfg.setdefault("file", f"{name}.{driver}")).absolute()
        fn.parent.mkdir(exist_ok=True, parents=True)

        url = f"{driver}:///{fn}"
        super().__init__(name, drv_cfg, url)
        self._fn = fn
