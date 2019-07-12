from ..mixins import UidMixin, IdMixin, id_with_sequence
from .base_model import BaseModel


class BaseModelWithUid(UidMixin, IdMixin, BaseModel):
    __abstract__ = True


class BaseModelWithId(IdMixin, BaseModel):
    __abstract__ = True


def base_model_with_uid_sequence(seq):
    class BaseModelWithUidSequence(UidMixin, id_with_sequence(seq), BaseModel):
        __abstract__ = True
    return BaseModelWithUidSequence
