import functools
import logging

def _class_cmp(a, b):
    return -1 if issubclass(b, a) and a is not b else 0

class TypeDict:
    def __init__(self, seed):
        self.seed = {**seed}
        self.cache = {}
        self.lazy_register_cb = None

    def __getitem__(self, obj):
        try:
            return self.cache[type(obj)]
        except KeyError:
            # Traverse seed dictionary from the most specific class to the lest.
            for lazy_register in (False, True):
                if lazy_register:
                    logging.info(f"lazy register for {type(obj)}")
                    if not self.__lazy_register(obj):
                        continue

                classes = sorted(self.seed, key=functools.cmp_to_key(_class_cmp), reverse=True)
                logging.debug(f"sorted classes: {classes}")
                for klass in classes:
                    if isinstance(obj, klass):
                        v = self.seed[klass]
                        self.cache[type(obj)] = v
                        return v

        raise KeyError(f"Unable to find value for type {type(obj)} or any of its parent classes")

    def __lazy_register(self, obj):
        if self.lazy_register_cb is not None:
            try:
                t = type(obj)
                m = getattr(t, "__module__", "")
                n = getattr(t, "__name__", "")
                return self.lazy_register_cb(self, f"{m}.{n}")
            except Exception as e:
                logging.warn("lazy load failed with exception", exc_info=e)
        return False

    def register(self, type, method):
        self.seed[type] = method
        self.cache = {}

def dispatcher(seed, ix=0):

    def mk_wrapper(method):

        td = TypeDict(seed)

        def wrapper(inner_self, *args, **kwargs):
            logging.debug(f"method {method} called!")
            try:
                name_or_ref = td[args[ix]]
            except IndexError:
                raise IndexError(f"Not enough arguments. Dispatcher looks at argument #{ix} but only {len(args)} where given")
            if isinstance(name_or_ref, str):
                target_method = getattr(inner_self, name_or_ref)
                return target_method(*args, **kwargs)
            else:
                return name_or_ref(*args, **kwargs)

        def copy():
            return mk_wrapper(method)

        functools.update_wrapper(wrapper, method)
        wrapper.copy = copy
        wrapper.td = td

        return wrapper

    return mk_wrapper
