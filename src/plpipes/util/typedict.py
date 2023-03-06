import logging
import functools

class TypeDict:
    def __init__(self, seed):
        self.seed = { **seed }
        self.cache = {}

    def __getitem__(self, obj):
        try:
            return self.cache[type(obj)]
        except KeyError as ex:
            for klass, value in self.seed.items():
                if isinstance(obj, klass):
                    self.cache[type(obj)] = value
                    return value
        raise KeyError(f"Unable to find value for type {type(obj)} or any of its parent classes")

def dispatcher(seed, ix=0):
    td = TypeDict(seed)
    def dispatcher(method):
        @functools.wraps(method)
        def wrapped_method(self, *args, **kwargs):
            name = td[args[ix]]
            target_method = getattr(self, name)
            return target_method(*args, **kwargs)
        return wrapped_method
    return dispatcher
