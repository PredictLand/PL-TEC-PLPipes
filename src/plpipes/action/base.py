import logging
import time

class Action:
    def __init__(self, name, action_cfg):
        self._name = name
        self._cfg = action_cfg

    def name(self):
        return self._name

    def short_name(self):
        return self._name.split(".")[-1]

    def _do_it(self, indent):
        self.do_it()

    def do_it(self):
        ...

    def run(self, indent=0):
        name = self.name()
        logging.info(f"{' '*indent}Action {name} started")
        start = time.time()
        self._do_it(indent=indent)
        lapse = int(10*(time.time() - start) + 0.5)/10.0
        logging.info(f"{' '*indent}Action {name} done ({lapse}s)")

    def __str__(self):
        return f"<Action {self._name}>"

