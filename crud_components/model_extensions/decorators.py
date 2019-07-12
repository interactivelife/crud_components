from .execution_stage import Stage
from .extension_property import ExtensionProperty
from .extension_execution import ExtensionPropertyExecution


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
