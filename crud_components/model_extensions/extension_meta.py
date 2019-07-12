import abc
import logging
from .extension_property import ExtensionProperty
from .extension_execution import ExtensionPropertyExecution

logger = logging.getLogger(__name__)


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
