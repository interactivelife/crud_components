import sqlalchemy as sa
from sqlalchemy.ext.declarative import declared_attr
from crud_components.validators.uid import uid_str, parse_uid


class IdMixinWithSequence:
    pass


def id_with_sequence(sequence):
    inherits = IdMixinWithSequence if sequence else object

    class IdWithSequence(inherits):
        # pylint: disable=no-self-argument,method-hidden
        @declared_attr.cascading
        def id(cls):
            info = {
                'ordering': 0,
                'exposed_name': 'id',
                'display_name': 'ID',
                'type': 'integer',
                'orderable': False,
                'searchable': False,
                'nullable': False,
                'editable': False,
                'localizable': False,
                'unique': True,
                'visible': False,
                'generated': True,
                'implicit': True,
                'summary': True,
                'quick_search': False,
            }

            from .uid_mixin import UidMixin
            if issubclass(cls, UidMixin):  # Ugly, but with declared_attr.cascading we cannot override "id" in subclasses
                # This works but it interferes with references: it will change exposed_as to e.g. organization.uid
                # info['exposed_as'] = 'uid'
                info['exposed_as'] = lambda s, f: uid_str(prefix=f.extras['uid_prefix'], serial_id=getattr(s, f.internal_name), version=None)
                info['unexposed_as'] = lambda s, f, v: parse_uid(v, prefix=f.extras['uid_prefix']).serial_id if v else None
                info['exposed_name'] = 'uid'
                info['type'] = 'uid'
                info['uid_prefix'] = getattr(cls, 'UID_PREFIX', None)

            for base in cls.__mro__[1:-1]:
                if getattr(base, '__table__', None) is not None:
                    col_type = sa.ForeignKey(base.id)
                    col_kwargs = dict()
                    break
            else:
                col_type = sa.Integer
                col_kwargs = dict(autoincrement=True)
            col_args = getattr(cls, '__id_args__', tuple())
            col_kwargs.update(getattr(cls, '__id_kwargs__', dict()))

            if sequence:
                col = sa.Column(col_type, *col_args, sequence, primary_key=True, info=info, **col_kwargs)
            else:
                col = sa.Column(col_type, *col_args, primary_key=True, info=info, **col_kwargs)
            # This makes sure it's created first because TranslatableMixin assumes the order of the primary keys
            col._creation_order = -1
            return col

        @classmethod
        def find(cls, identifier):
            if identifier is None:
                return None
            pkey_value = int(identifier)
            return cls.query.get(pkey_value)
    return IdWithSequence


class IdMixin(id_with_sequence(None)):
    pass
