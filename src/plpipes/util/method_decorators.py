
def optional_abstract(method):
    def wrapper(*args, **kwargs):
        raise NotImplementedError(f"Optional method {method.__name__} is not implemented by the class {args[0].__class__.__name__}")
    return wrapper
