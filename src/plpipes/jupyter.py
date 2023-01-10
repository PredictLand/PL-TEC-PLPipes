
from IPython.core.magic import Magics, magics_class, line_cell_magic

@magics_class
class MyMagics(Magics):
    @line_cell_magic
    def lcmagic(self, line, cell=None):
        "Magic that works both as %lcmagic and as %%lcmagic"
        if cell is None:
            cell = ""
        if line is None:
            line = ""
        lines = [x
                 for x in l.split("\n")
                 for l in (cell, line)
                 if l is not None]
        return repr(lines)

def load_ipython_extension(ipython):
    """
    Any module file that define a function named `load_ipython_extension`
    can be loaded via `%load_ext module.path` or be configured to be
    autoloaded by IPython at startup time.
    """

    ipython.register_magics(MyMagics)
