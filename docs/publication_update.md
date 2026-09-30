# Publication and update of the package in pypi

This code is published as a Python package in pypi (https://pypi.org/project/plpipes). There are several ways to publish or update the package. We here used flit (https://flit.pypa.io/en/stable/). In case you want to update the version, follow these steps:

- First, you will need to install "flit". Therefore, run `pip install flit` (not in the environment of plpipes, but in your device).
- Update the version of the package in the file "pyproject.toml", in "[project]" > "version":

```toml
[project]
name = "plpipes"
authors = [{name = "PredictLand", email = "info@predictland.com"}]
readme = "README.md"
license = {file = "LICENSE"}
classifiers = ["License :: OSI Approved :: MIT License"]
version = "0.4"
description = "PredictLand Data Science Framwork"
```

Typically, increase the first number if you break backwards compatibility and, otherwise, increase the second.

- Create an account in pypi.
- Then, from your terminal, run:

```powershell
flit build
flit login pypi
flit publish
```