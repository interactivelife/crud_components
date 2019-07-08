__all__ = ('Uid', 'UidValidator', 'UidValueError', 'parse_uid', 'uid_str')

import re
from collections import namedtuple

from flask import current_app
from hashids import Hashids
from jsonschema import draft4_format_checker


Uid = namedtuple('Uid', 'prefix,serial_id,version')


class UidValueError(ValueError):
    pass


class UidValidator:
    """
    Client facing UIDs have the following format:

        ([A-Z]{3})([VN])_($code)(?:_($version))?
        $code = hashids.encode(serial_id)
        $version = int if 'V' else 0

    The prefix identifies the type of object.
    The version flag identifies whether the UID identifies a specific version (V) or not (N).
    The code is the encoded (using "hashids") serial ID of the object.
    The version is an optional positive number, non-zero if the version flag is V.

    """
    PREFIX_ANY = object()
    PREFIX_VALID = object()
    UID_REGEX = re.compile('^([A-Z]{3})([VN])_([a-zA-Z0-9]+)(?:_([0-9]+))?$')

    # This should be populated by services using this package
    VALID_PREFIXES = {}

    CANARY2 = 0xA3

    @classmethod
    def add_prefixes(cls, **prefixes):
        cls.VALID_PREFIXES.update(prefixes)

    def __init__(self, salt=None, prefix=PREFIX_VALID, versioned=None):
        self.prefix = prefix
        self.versioned = versioned
        if salt is not None:
            self.hashids = Hashids(salt=salt)

    def decode(self, val, versioned=None):
        if val is None or val == '':
            return
        try:
            match = self.UID_REGEX.match(val)
        except TypeError:
            raise UidValueError('Not a string')
        if not match:
            raise UidValueError('No regexp match')
        prefix, versioned_flag, hashed_id, version_str = match.groups()
        is_versioned = (versioned_flag == 'V')
        if self.prefix is self.PREFIX_ANY:
            pass
        elif self.prefix is self.PREFIX_VALID:
            if prefix not in self.VALID_PREFIXES:
                raise UidValueError('Invalid prefix')
        else:
            if self.prefix != prefix:
                raise UidValueError('Expected prefix {!r}, got {!r}'.format(self.prefix, prefix))

        try:
            version = int(version_str)
        except (TypeError, ValueError):
            version = 0
        if is_versioned and not version:
            # zero, empty string, or missing
            raise UidValueError('Version flag and value mismatch')

        versioned = self.versioned if versioned is None else versioned
        if versioned is None:
            pass
        elif (versioned and not is_versioned) or (not versioned and is_versioned):
            raise UidValueError('Expected {}versioned element'.format('' if versioned else 'un-'))

        try:
            canary1, serial_id, canary2 = self.hashids.decode(hashed_id)
        except ValueError as ex:
            raise UidValueError('The canaries are missing') from ex
        else:
            expected_canary1 = self.VALID_PREFIXES.get(prefix, 0x00)
            if canary1 != expected_canary1 or canary2 != self.CANARY2:
                raise UidValueError('The canaries are fuming')

        return Uid(prefix=prefix, serial_id=serial_id, version=version)

    def encode(self, uid, valid=True, versioned=None):
        if uid is None:
            return
        if uid.serial_id is None:
            return
        if valid and uid.prefix not in self.VALID_PREFIXES:
            raise UidValueError('Invalid prefix {!r}'.format(uid.prefix))

        if versioned is None:
            versioned = bool(uid.version)
        elif (versioned and not uid.version) or (not versioned and uid.version):
            raise UidValueError('Expected {}versioned uid'.format('' if versioned else 'un-'))

        canary1 = self.VALID_PREFIXES.get(uid.prefix, 0x00)

        hashed_id = self.hashids.encode(canary1, uid.serial_id, self.CANARY2)
        if hashed_id == '':
            raise UidValueError('Invalid serial id {!r}'.format(uid.serial_id))

        if versioned:
            return '{prefix}{vflag}_{hashed_id}_{version}'.format(prefix=uid.prefix, vflag='V', hashed_id=hashed_id,
                                                                  version=uid.version)
        else:
            return '{prefix}{vflag}_{hashed_id}'.format(prefix=uid.prefix, vflag='N', hashed_id=hashed_id)

    def __call__(self, val):
        uid = self.decode(val)
        return True

    @property
    def draft4_format(self):
        mapping = {
            (self.PREFIX_ANY, None): 'uid-ANY',
            (self.PREFIX_ANY, True): 'uid-v-ANY',
            (self.PREFIX_ANY, False): 'uid-n-ANY',
            (self.PREFIX_VALID, None): 'uid',
            (self.PREFIX_VALID, True): 'uid-v',
            (self.PREFIX_VALID, False): 'uid-n',
        }
        if (self.prefix, self.versioned) in mapping:
            return mapping[self.prefix, self.versioned]
        elif self.versioned is None:
            return 'uid-{}'.format(self.prefix)
        else:
            return 'uid-{}-{}'.format('v' if self.versioned else 'n', self.prefix)

    def register(self):
        return draft4_format_checker.checks(self.draft4_format, raises=UidValueError)(self)

    @classmethod
    def init_app(cls, app):
        salt = app.config['UID_SALT']
        validator = cls(prefix=cls.PREFIX_ANY, versioned=None, salt=salt).register()
        cls(prefix=cls.PREFIX_ANY, versioned=True, salt=salt).register()
        cls(prefix=cls.PREFIX_ANY, versioned=False, salt=salt).register()

        cls(prefix=cls.PREFIX_VALID, versioned=None, salt=salt).register()
        cls(prefix=cls.PREFIX_VALID, versioned=True, salt=salt).register()
        cls(prefix=cls.PREFIX_VALID, versioned=False, salt=salt).register()

        for prefix in cls.VALID_PREFIXES:
            for versioned in (None, True, False):
                cls(prefix=prefix, versioned=versioned, salt=salt).register()

        app.extensions['uid_validator'] = validator


def parse_uid(val, version_id=None, prefix=None, versioned=None):
    salt = current_app.config['UID_SALT']
    prefix = UidValidator.PREFIX_VALID if prefix is None else prefix
    validator = UidValidator(prefix=prefix, versioned=versioned, salt=salt)
    uid = validator.decode(val)
    if version_id:
        prefix, serial_id, version = uid
        assert not version
        return Uid(prefix, serial_id, version_id)
    return uid


def uid_str(valid=True, versioned=None, **uid):
    validator = current_app.extensions['uid_validator']
    return validator.encode(Uid(**uid), versioned=versioned)
