from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import generic_repr


@generic_repr
class AbstractBaseModel(declarative_base()):
    __abstract__ = True
    __extensions__ = frozenset()  # This is automatically replaced by a list in the Extension metaclass
    __extension_instance_map__ = None  # This is automatically replaced by a dict in the Extension metaclass
