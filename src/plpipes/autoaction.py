"""
plpipes.autoaction

This module is a "magic" module that enables Python actions to be executed as if they were standard scripts. It should be imported at the beginning of any action script, as it performs necessary initialization and variable injection.

When imported, this module checks if the plpipes environment is initialized. If it is not, it sets up the environment and injects variables into the calling code's global namespace. This includes variables defined by PLPipes for actions, making them readily available for the action script without requiring explicit definitions.

Usage:
- Import this module at the very start of your action script to ensure proper initialization and variable accessibility.
- The module will automatically handle the necessary setup and variable injection.

Example:

```python
import plpipes.autoaction
# Your action script logic follows...
```

Exceptions:
- If the module is not the first import in the script, an exception will be raised indicating that the plpipes.autoaction must be the first sentence in the action script.

"""

import plpipes.init

if not plpipes.init._initialized:
    from plpipes.runner import simple_init
    from plpipes.action.driver.simple import _action_namespace_setup
    import inspect
    import logging
    import sys

    argv = sys.argv
    cmd = argv[0] if argv else ''
    # Hack for VSCode/ipykernel not setting the correct command line arguments
    if cmd.endswith('ipykernel_launcher.py'):
        argv = ['bin/run.py']

    simple_init(argv)

    def get_main_frame():
        stack = inspect.stack()
        for frame_info in stack:
            if frame_info.function == '<module>':
                if frame_info.frame.f_globals['__name__'] == '__main__':
                    return frame_info.frame

        raise Exception("Unable to locate main frame for injecting globals!")

    main = get_main_frame()
    for k, v in main.f_globals.items():
        if k.startswith("__"):
            continue
        logging.warning(f"Importing plpipes.autoaction should be the first sentence in the action script. Found `{k}` already declared.")

    logging.info(f"Injecting variables into action namespace.")

    for k, v in _action_namespace_setup().items():
        main.f_globals[k] = v
