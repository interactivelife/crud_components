__all__ = ('Extension', 'ExtensionProperty', 'ExtensionPropertyExecution', 'Stage',
           'extension_property', 'extension_post_flush', 'extension_pre_flush', 'extension_pre_set',
           'extension_pre_versioning', 'extension_pre_flush_delete',
)

import logging
import abc
from enum import Enum

logger = logging.getLogger(__name__)


class Stage(Enum):
    PRE_FLUSH = 'pre_flush'
    POST_FLUSH = 'post_flush'
    PRE_SET = 'pre_set'
    PRE_VERSIONING = 'pre_versioning'
    PRE_FLUSH_DELETE = 'pre_flush_delete'


class ExtensionMeta(abc.ABCMeta):
    def __new__(cls, name, bases, dikt):
        abstract = dikt.get('__abstract__', False)

        try:
            model_cls = dikt['__model__']
        except KeyError as ex:
            if not abstract:
                raise Exception('Misconfigured model extension') from ex
            model_cls = None

        # assert issubclass(model_cls, BaseModel)
        extensions = getattr(model_cls, '__extensions__', None) if model_cls is not None else None
        if not extensions:
            extensions = []

        dikt['__properties__'] = props = {}
        dikt['__executions__'] = execs = {}

        instance = super(ExtensionMeta, cls).__new__(cls, name, bases, dikt)

        extensions.append(instance)
        if model_cls is not None:
            model_cls.__extensions__ = extensions

        for b in bases:
            for k, v in getattr(b, '__properties__', dict()).items():
                assert isinstance(v, ExtensionProperty)
                props[k] = cls.link_to_extension(v, instance, abstract)

            for k, v in getattr(b, '__executions__', dict()).items():
                assert isinstance(v, ExtensionPropertyExecution)
                execs[k] = cls.link_to_extension(v, instance, abstract)

        for k, v in dikt.items():
            if isinstance(v, ExtensionProperty):
                if k in props:
                    logger.warning('Extension property being overwritten')
                props[k] = cls.link_to_extension(v, instance, abstract)
            elif isinstance(v, ExtensionPropertyExecution):
                if k in execs:
                    logger.warning('Extension property being overwritten')
                execs[k] = cls.link_to_extension(v, instance, abstract)

        return instance

    @staticmethod
    def link_to_extension(prop, instance, abstract):
        if abstract:
            prop.__abstract__ = True
            return prop
        else:
            if prop.__abstract__:
                prop = prop.clone()
            return prop.assign(instance)


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


class Extension(metaclass=ExtensionMeta):
    __abstract__ = True
    __model__ = None
    __implicit__ = False
    __properties__ = {}
    __executions__ = {}

    def __init__(self, session, instance):
        self.session = session
        self.instance = instance

    @classmethod
    def with_arguments(cls, *args, **kwargs):
        return ExtensionConfiguration(cls, args, kwargs)

    @classmethod
    def sorted_executions(cls, stage, field_name):
        for v in sorted(cls.__executions__.values(), key=lambda x: x.priority):
            if v.name == field_name and v.stage == stage:
                yield v

    def expose(self, instance, field, **kw):
        assert instance is self.instance
        return getattr(self, field.internal_name)

    def update(self, instance, field, value):
        assert instance is self.instance
        # before = getattr(self, field.internal_name)
        setattr(self, field.internal_name, value)
        # after = getattr(self, field.internal_name)
        # return value, 0 if before == after else 1
        return value, 1  # TODO change detection for property extensions?

    def model_execute(self, parent_visitor, stage, instance, value=None):
        assert instance is self.instance
        for execution_function in self.sorted_executions(stage, ExtensionPropertyExecution.MODEL_EXECUTION):
            if execution_function.with_parent_visitor:
                execution_function(self, value=value, parent_visitor=parent_visitor)
            else:
                execution_function(self, value=value)

    def execute(self, parent_visitor, stage, instance, field, value, overriden=None):
        assert instance is self.instance
        for execution_function in self.sorted_executions(stage, field.internal_name):
            kwargs = dict()
            if execution_function.with_parent_visitor:
                kwargs['parent_visitor'] = parent_visitor
            if execution_function.overrides:
                kwargs['overriden'] = overriden
            retval = execution_function(self, value=value, **kwargs)
            if retval is None or not execution_function.overrides:
                yield None, False
            else:
                yield retval, True


class ExtensionConfiguration:
    def __init__(self, extension_cls, args, kwargs):
        self.extension_cls = extension_cls
        self.args = args
        self.kwargs = kwargs

    def instantiate(self, session, instance):
        return self.extension_cls(session, instance, *self.args, **self.kwargs)


def extension_property(_f=None, **info):
    if _f is not None:
        return ExtensionProperty(fget=_f, **info)

    return lambda f: ExtensionProperty(fget=f, **info)


def extension_pre_set(_f=None, **info):
    info.setdefault('stage', Stage.PRE_SET)
    if _f is not None:
        return ExtensionPropertyExecution(func=_f, **info)

    return lambda f: ExtensionPropertyExecution(func=f, **info)


def extension_pre_flush(_f=None, **info):
    info.setdefault('stage', Stage.PRE_FLUSH)
    if _f is not None:
        return ExtensionPropertyExecution(func=_f, **info)

    return lambda f: ExtensionPropertyExecution(func=f, **info)


def extension_post_flush(_f=None, **info):
    info.setdefault('stage', Stage.POST_FLUSH)
    if _f is not None:
        return ExtensionPropertyExecution(func=_f, **info)

    return lambda f: ExtensionPropertyExecution(func=f, **info)


def extension_pre_versioning(_f=None, **info):
    info.setdefault('stage', Stage.PRE_VERSIONING)
    if _f is not None:
        return ExtensionPropertyExecution(func=_f, **info)

    return lambda f: ExtensionPropertyExecution(func=f, **info)

def extension_pre_flush_delete(_f=None, **info):
    info.setdefault('stage', Stage.PRE_FLUSH_DELETE)
    if _f is not None:
        return ExtensionPropertyExecution(func=_f, **info)

    return lambda f: ExtensionPropertyExecution(func=f, **info)
