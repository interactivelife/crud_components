

class ExtensionProperty(property):
    is_attribute = False
    is_property = False
    __abstract__ = False

    def __init__(self, fget=None, fset=None, fdel=None, doc=None, **info):
        self.info = info
        self.name = fget.__name__
        self.extension_cls = None
        self.doc = doc
        super(ExtensionProperty, self).__init__(fget, fset, fdel, doc)

    def assign(self, extension_cls):
        if self.extension_cls is not None:
            raise TypeError('This property is already configured with another extension')
        self.extension_cls = extension_cls
        self.__abstract__ = False
        return self

    def clone(self):
        assert self.__abstract__ and self.extension_cls is None
        cls = self.__class__
        return cls(fget=self.fget, fset=self.fset, fdel=self.fdel, doc=self.doc, **self.info)

    def setter(self, fset):
        r = super(ExtensionProperty, self).setter(fset)
        r.info = self.info
        r.name = self.name
        return r

    def __repr__(self):
        return 'ExtensionProperty({!r}, info={!r})'.format(self.name, self.info)
