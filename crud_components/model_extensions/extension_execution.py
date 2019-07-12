

class ExtensionPropertyExecution:
    __abstract__ = False
    MODEL_EXECUTION = '__model__'

    def __init__(self, func, stage, **info):
        self.info = info  # TODO should we change the signature to allow info to be a colinfo object?
        self.name = info.get('name', self.MODEL_EXECUTION)
        self.func = func
        self.stage = stage
        self.extension_cls = None
        self.priority = info.get('priority', 0)
        self.with_parent_visitor = info.get('with_parent_visitor', False)
        self.overrides = info.get('overrides', False)

    def assign(self, extension_cls):
        if self.extension_cls is not None:
            raise TypeError('This property is already configured with another extension')
        self.extension_cls = extension_cls
        return self

    def clone(self):
        cls = self.__class__
        return cls(func=self.func, stage=self.stage, **self.info)

    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)

    def __repr__(self):
        return 'ExtensionPropertyExecution({!r}, stage={!r} info={!r})'.format(self.name, self.stage, self.info)
