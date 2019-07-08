from ...exceptions import ModelValidationError
from crud_components.utils.validators.uid import uid_str, parse_uid, Uid


class UidMixin:
    #: This needs to be set in the subclasses directly
    UID_PREFIX = 'XXX'

    @property
    def uid(self):
        version = 0
        return uid_str(prefix=self.UID_PREFIX, serial_id=self.id, version=version)

    @uid.setter
    def uid(self, value):
        uid = parse_uid(value)
        if uid.prefix != self.UID_PREFIX:
            raise ModelValidationError("Invalid UID {!r}; expected prefix {!r}".format(value, self.UID_PREFIX))
        self.id = uid.serial_id
        if uid.version != 0:
            raise ModelValidationError("Unexpected versioned UID")

    @classmethod
    def find(cls, identifier):
        if isinstance(identifier, Uid):
            uid = identifier
        else:
            uid = parse_uid(identifier)
        if uid.prefix != cls.UID_PREFIX:
            raise ModelValidationError("Invalid UID {!r}; expected prefix {!r}".format(identifier, cls.UID_PREFIX))
        pkey_value = uid.serial_id if uid is not None else None
        if pkey_value is None:
            return None
        return cls.query.get(pkey_value)
