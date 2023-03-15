class set_context_var:

    def __init__(self, var, value):
        self.var = var
        self.value = value
        self.token = None

    def __enter__(self):
        self.token = self.var.set(self.value)

    def __exit__(self, *_):
        if self.token is not None:
            self.var.reset(self.token)
