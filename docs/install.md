# Installing `plpipes`

The Python module `plpipes` can be installed in two ways:

## Installing a packed version

This is the recommended method for installing the module if you simply want to use it without contributing to its development. 

You can install `plpipes` directly from the Python Package Index (PyPI) using pip:

```bash
pip install plpipes
```

This will ensure you have the latest stable version.

## Installing from git

1. Clone the repository outside of your project directory and switch to the `develop` branch:

    ```bash
    git clone git@github.com:PredictLand/PL-TEC-PLPipes.git
    cd PL-TEC-PLPipes
    git checkout develop
    ```

2. Add the `src` subdirectory to the Python search path:

    ```bash
    # Linux and/or bash:
    export PYTHONPATH=path/to/.../PL-TEC-PLPipes/src
    # Windows
    set PYTHONPATH=C:\path\to\...\PL-TEC-PLPipes\src
    ```

3. Check that it works:

    ```bash
    python -m plpipes -c "print('ok')"
    ```

Alternatively, you can modify your project's main script to append the `src` directory to the module search path so that you don't need to set `PYTHONPATH` manually each time you start a new session.

For example:

```python
from pathlib import Path
import sys
sys.path.append(str(Path.cwd().parent.parent.parent / "PL-TEC-PLPipes/src"))

from plpipes.runner import main
main()
```

Or you can also set `PYTHONPATH` from your shell startup script (`~/.profile`) or in the Windows registry.

## Packing `plpipes`

If you would like to create a wheel file for `plpipes`, you can do so using [flit](https://flit.pypa.io/en/stable/), which can be installed with pip:

```bash
pip install flit
```

To generate a Python wheel file for `plpipes`, run the following command from inside the `plpipes` root directory:

```bash
flit build
```

The generated wheel file will be placed in the `dist` directory. This file is a standard (pure) Python package that can be installed on any operating system as demonstrated above.
