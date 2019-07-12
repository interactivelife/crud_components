

class ExtensionConfiguration:
    def __init__(self, extension_cls, args, kwargs):
        self.extension_cls = extension_cls
        self.args = args
        self.kwargs = kwargs

    def instantiate(self, session, instance):
        return self.extension_cls(session, instance, *self.args, **self.kwargs)
