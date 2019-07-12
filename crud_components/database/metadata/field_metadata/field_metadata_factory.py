from collections import namedtuple
from ..field_info import FieldInfo


def field_metadata_class_factory(name, field_info_class):
    field_info_keys = field_info_class.get_info_keys()
    _Metadata = namedtuple(name, field_info_keys)
    _Metadata.__new__.__defaults__ = (None,) * len(field_info_keys)

    class FieldMetadata(_Metadata):
        def __new__(cls, *args, **kwargs):
            # extras = tuple(kwargs.pop('extras', dict()).items())
            extras = kwargs.pop('extras', dict())
            self = super(FieldMetadata, cls).__new__(cls, *args, **kwargs)
            self.extras = extras
            return self

        def _asdict(self):
            dikt = super(FieldMetadata, self)._asdict()
            dikt.update(self.extras)
            return dikt

        def _copy_with(self, **kwargs):
            dikt = super(FieldMetadata, self)._asdict()
            extras = self.extras.copy()
            for k, v in kwargs.items():
                if k in dikt:
                    dikt[k] = v
                else:
                    extras[k] = v
            dikt['extras'] = extras
            return FieldMetadata(**dikt)

        def __repr__(self):
            return '{}(extras={!r})'.format(super(FieldMetadata, self).__repr__(), self.extras)
        
    return FieldMetadata
