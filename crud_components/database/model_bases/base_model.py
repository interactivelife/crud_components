from sqlalchemy import orm, inspect
import sqlalchemy as sa
from .abstract_base_model import AbstractBaseModel
from ...exceptions import SkipExtension, ModelValidationError


class BaseModel(AbstractBaseModel):
    __abstract__ = True
    crud_metadata = None

    __STATES = ('expired', 'persistent', 'detached', 'modified', 'deleted', 'was_deleted', 'pending', 'transient')

    @classmethod
    def create(cls):
        return cls()

    @classmethod
    def find(cls, identifier):
        # Overridden in IdMixin, UidMixin and VersionedMixin
        return cls.query.get(identifier)

    def update_from_dict(self, visitor, iter_whitelist, with_extensions=None):
        """
        for model specific write traversal
        :param visitor:
        :param iter_whitelist:
        :param with_extensions:
        :return:
        """
        for key, field, value, only_sub_field_names in iter_whitelist:
            if field is None:
                yield 0
                continue
            unexposed_value, additional_changes = visitor.visit_field(self, field, value)
            yield additional_changes

    def as_dict(self, visitor, field_name_pairs, with_extensions=None):
        """
        for model specific read traversal
        :param visitor:
        :param field_name_pairs:
        :param with_extensions:
        :return:
        """
        dikt = dict()
        for field, sub_field_names in field_name_pairs:
            visitor.visit_field(dikt, self, field, field_names=sub_field_names)
        return dikt

    def extension_instance(self, extension_cls, session=None, with_extensions=None):
        """
        todo
        :param extension_cls:
        :param session:
        :param with_extensions:
        :return:
        """
        assert extension_cls in self.__extensions__
        if not self.__extension_instance_map__:
            self.__extension_instance_map__ = {}
        if extension_cls in self.__extension_instance_map__:
            return self.__extension_instance_map__[extension_cls]

        if session is None:
            session = orm.object_session(self)
            assert session is not None, 'Cannot get extension instance on detached model instance'

        for extension_conf in with_extensions or tuple():
            if extension_cls is extension_conf.extension_cls:
                extension_instance = extension_conf.instantiate(session, self)
                break
        else:
            if extension_cls.__implicit__:
                extension_instance = extension_cls(session, self)
            else:
                raise SkipExtension()
        self.__extension_instance_map__[extension_cls] = extension_instance
        return extension_instance

    _alias_map = {}
    @classmethod
    def alias_for_relationship(cls, name):
        """
        Alias for a generic table in a join statement
        :param name:
        :return:
        """
        relation = getattr(cls, name)
        entity = sa.inspect(relation).mapper.entity
        try:
            alias = cls._alias_map[name]
        except KeyError:
            cls._alias_map[name] = alias = orm.aliased(
                entity,
                name='{}_{}_internal'.format(cls.__name__, entity.__name__).lower()
            )
        return alias

    @classmethod
    def field_by_column(cls, col):
        """
        Gets field metadata of a column
        :param col:
        :return:
        """
        internal_name = col.name  # TODO this only works for columns, not all attributes
        return cls.crud_metadata.find_field_by_internal_name(internal_name)

    @classmethod
    def column_by_field(cls, field, multiple=False, as_tuple=False, aliases=None):
        """
        Gets column of field metadata
        :param field:
        :param multiple:
        :param as_tuple:
        :param aliases:
        :return:
        """
        if as_tuple:
            return getattr(cls, field.internal_name),
        else:
            return getattr(cls, field.internal_name)

    @classmethod
    def column_order_by_field(cls, field, direction, modifier=None, value=None, aliases=None):
        """
        Returns an "orderBy" sqla construct given the following ordering details.
        :param field:
        :param direction:
        :param modifier:
        :param value:
        :param aliases:
        :return:
        """
        col = cls.column_by_field(field, multiple=True, aliases=aliases)
        direction = direction.lower() if direction else ''
        if direction not in ('asc', 'desc'):
            raise ValueError("Expected sort order to be one of 'asc' or 'desc'")

        needed_joins = field.needed_joins
        for join in needed_joins:
            aliases.add_pending_join(join)

        if modifier is None:
            return getattr(col, direction)(), None
        try:
            order_by_func, extra_fields_func = col.info['order_modifiers'][modifier]
        except KeyError as ex:
            raise ValueError('Did not expect modifier {!r} for field {!r}'.format(modifier, field)) from ex
        try:
            order_by = order_by_func(cls, value)
            extra_fields = extra_fields_func(cls, value) if extra_fields_func else None
        except (KeyError, ValueError, TypeError) as ex:
            raise ValueError('Wrong value {!r} for modifier {!r} of field {!r}'.format(value, modifier, field)) from ex
        return getattr(order_by, direction)(), extra_fields

    def assert_uniqueness(self):
        """
        optimistic check for uniqueness.
        Works 99% of the time, fails when a race condition occurs (i.e. a model is inserted while we are doing the checking)
        The actual checking happens at the DB level when the transaction is being persisted in the DB.

        This check is used to display a nice message for the user.
        :return:
        """
        # TODO handle uniqueness in translatable fields and use fields metadata
        mapper = inspect(self).mapper
        for name, col in mapper.columns.items():
            if not col.unique:
                continue
            value = getattr(self, name)
            if value is None and col.nullable:
                continue
            match = self.query.filter(col == value).limit(1).one_or_none()
            if match is None:
                # No conlict found
                continue

            if match.id == self.id:
                # The only match is the instance itself
                # (self.id would be None if it's a newly created instance)
                continue

            # There's a conflict with another instance
            raise ModelValidationError("Field {} is not unique".format(name))

    def __repr__(self):
        """
        A generic repr method, with state information about loaded/expired attributes/relationships and session state.

        See also `sqlalchemy_utils.generic_repr`.

        :return: a string representation of the instance's data and its state in the session
        """
        state = inspect(self)
        state_dict = state.dict
        mapper = state.mapper
        col_keys = mapper.columns.keys()
        identity = state.identity if state.identity is None or len(state.identity) > 1 else state.identity[0]
        return '{class_name}(identity={identity}, state=({state}), {values}{unloaded}{expired})'.format(
            class_name=type(self).__name__,
            identity=identity,
            values=', '.join('{}={!r}'.format(k, state_dict[k]) for k in col_keys if k in state_dict),
            unloaded=', unloaded={!r}'.format(state.unloaded) if state.unloaded else '',
            expired=', expired={!r}'.format(state.expired_attributes) if state.expired_attributes else '',
            state=' '.join(k for k in self.__STATES if getattr(state, k))
        )
