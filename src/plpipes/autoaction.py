
import plpipes.init

if not plpipes.init._initialized:
    from plpipes.runner import simple_init
    from plpipes.action.driver.simple import _action_namespace_setup
    import inspect
    import logging
    import sys

    argv = sys.argv
    cmd = argv.get(0, '')
    # Hack for VSCode/ipykernel not setting the correct command line arguments
    if cmd.endswith('ipykernel_launcher.py'):
        argv = ['bin/run.py']

    simple_init(argv)

    def get_main_frame():
        stack = inspect.stack()
        for frame_info in stack:
            # logging.info(f"Frame info: {frame_info.frame.f_globals['__name__']}")
            if frame_info.function == '<module>':
                if frame_info.frame.f_globals['__name__'] == '__main__':
                    return frame_info.frame

        raise Exception("Unable to localte main frame for injecting globals!")

    main = get_main_frame()
    for k, v in main.f_globals.items():
        if k.startswith("__"):
            continue
        raise Exception(f"Importing plpipes.autoaction must be the first sentence in the action script. Found `{k}` already declared.")



    logging.info(f"Injecting variables into action namespace.")

    for k, v in _action_namespace_setup().items():
        main.f_globals[k] = v
