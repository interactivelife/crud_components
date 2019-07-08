__all__ = (
    'UserFilters', 'UserFilterItem', 'UserFilterConnector', 'UserOrder',
    'AliasesCollection', 'make_search_queries',
)

import itertools
import logging
from collections import namedtuple

from sqlalchemy_utils import escape_like
from sqlalchemy import orm
import sqlalchemy as sa

from .helpers import parse_field_names
from crud_components.utils.validators import parse_uid, ga_point_from_dict
from ..exceptions import MetadataValidationProblem

logger = logging.getLogger(__name__)


OPERATOR_FILTER_MAP = {
    'eq': lambda f, v: f == v,
    'neq': lambda f, v: f != v,
    'gt': lambda f, v: f > v,
    'gte': lambda f, v: f >= v,
    'lt': lambda f, v: f < v,
    'lte': lambda f, v: f <= v,
    'contains': lambda f, v: f.ilike('%{}%'.format(escape_like(v))),
    'contains_cs': lambda f, v: f.like('%{}%'.format(escape_like(v))),
    'eq_cs': lambda f, v: f.ilike('{}'.format(escape_like(v))),
    'neq_cs': lambda f, v: ~f.ilike('{}'.format(escape_like(v))),
    'near': lambda f, v: f.ST_DWithin(ga_point_from_dict(v), v.get('radius', '20000')),
    'in': lambda f, v: sa.or_(*(f.any(vv) for vv in v)),  # field: array; value: array
    'all': lambda f, v: sa.and_(*(f.any(vv) for vv in v)),  # field: array; value: array
    'any': lambda f, v: f.any(v),  # field: array; value: string
    'array_contains': lambda f, v: f.contains(sa.cast(v, sa.ARRAY(sa.Unicode))),  # field: array; value: array
}

UserFilterItem = namedtuple('UserFilterItem', 'field,operator,value,case_sensitive')
UserFilterConnector = namedtuple('UserFilterConnector', 'operand,items')
UserOrder = namedtuple('UserOrder', 'field,direction,modifier,value')


def _transform_value(field, value):
    if field.type == 'integer':
        return int(value)
    elif field.type == 'uid':
        prefix = field.extras.get('uid_prefix')
        if prefix is None:
            logger.warning("Using a UID field in a filter without specifying uid_prefix")
        uid = parse_uid(value, prefix=prefix)
        return uid.serial_id if uid is not None else None
    else:
        return value


class UserFilters:
    def __init__(self, model_cls, custom_filter, filter=None, term="", include=None, exclude=None, order=None):
        self.model_cls = model_cls
        self.term = term
        self.include = tuple() if not include else tuple(include)
        self.exclude = tuple() if not exclude else tuple(exclude)
        self.custom_filter = custom_filter or tuple()
        self.orders = tuple(self._parse_orders(order))
        self.tree = tuple(self._parse_filters(filter))

    def _parse_filters(self, dikt):
        if dikt is None:
            return
        for name, criterion in sorted(dikt.items()):
            # If we use equal, it will not support cases such as `(A and B) or (X and Y)`
            # (i.e. more than one clause of the same type)
            # So we use startswith to allow the user to specify `_or: {_and_1: {A, B}, _and_2: {X, Y}}`
            if name.startswith('_or'):
                yield UserFilterConnector('or', tuple(self._parse_filters(criterion)))
                continue
            elif name.startswith('_and'):
                yield UserFilterConnector('and', tuple(self._parse_filters(criterion)))
                continue
            try:
                field = self.model_cls.crud_metadata.find_field_by_exposed_name(name)
            except AttributeError:
                logger.exception("Expected field name in filter, got %r", name)
                raise MetadataValidationProblem(
                    title='Invalid filter fields',
                    detail="Expected field name in filter, got {!r}".format(name)
                )
            yield UserFilterItem(field=field, operator=criterion['op'], value=criterion['value'],
                                  case_sensitive=criterion.get('case_sensitive', False))

    def _parse_orders(self, orders):
        if not orders:
            return
        try:
            for order in orders:
                name = order["field"]
                direction = order["order"]
                if direction not in ('asc', 'desc'):
                    logger.debug("Expected asc/desc in order direction, got %r", direction, exc_info=True)
                    raise MetadataValidationProblem(
                        title='Invalid order direction',
                        detail='Expected "asc" or "desc" in order, got {!r}'.format(direction),
                    )
                try:
                    field = self.model_cls.crud_metadata.find_field_by_exposed_name(name)
                except AttributeError:
                    logger.debug("Expected field name in order, got %r", name, exc_info=True)
                    raise MetadataValidationProblem(
                        title='Invalid filter fields',
                        detail="Expected field name in filter, got {!r}".format(name)
                    )
                yield UserOrder(
                    field=field, direction=direction,
                    modifier=order.get('modifier'), value=order.get('value')
                )
        except KeyError as ex:
            # wrong order item slipped through the validation
            logger.debug("Invalid order values", exc_info=True)
            raise MetadataValidationProblem(
                title="Invalid order values",
                detail="Invalid field name or order value or data type in request",
            ) from ex

    def _item_condition(self, filter_item, aliases):
        field, op, value, case_sensitive = filter_item
        col = self.model_cls.column_by_field(field, multiple=True, aliases=aliases)

        needed_joins = field.needed_joins
        for join in needed_joins:
            aliases.add_pending_join(join)

        if case_sensitive:
            func_names = (op + '_cs', op)
        else:
            func_names = (op,)
        for fname in func_names:
            operator_function = OPERATOR_FILTER_MAP.get(fname)
            if operator_function is not None:
                break
        else:
            assert False, 'We should not get here'
        try:
            value = _transform_value(field, value)
        except (AttributeError, KeyError, TypeError, ValueError) as ex:
            logger.debug("Failed to transform input in filter value", exc_info=True)
            raise MetadataValidationProblem(
                title="Invalid filter values",
                detail="Invalid filter value for field {}".format(field.exposed_name),
            ) from ex
        return operator_function(col, value)

    def _condition(self, item, aliases):
        if isinstance(item, UserFilterItem):
            return self._item_condition(item, aliases)
        else:
            operand, items = item
            operand_func = sa.and_ if operand == 'and' else sa.or_
            return operand_func(*(self._condition(it, aliases) for it in items))

    def iter_criteria(self, aliases):
        for cond in self.custom_filter:
            yield self._condition(cond, aliases), True

        # if issubclass(self.model_cls, WeakVersionableMixin):
        #     deleted_field = self.model_cls.crud_metadata.find_field_by_internal_name('deleted')
        #     filter_item = UserFilterItem(field=deleted_field, operator='eq', value=False, case_sensitive=False)
        #     yield self._condition(filter_item, aliases), True

        if self.term:
            term_conditions = UserFilterConnector('or', tuple(
                UserFilterItem(field=field, operator=operator, value=self.term, case_sensitive=False)
                for field, operator in self.model_cls.crud_metadata.quick_search_fields.items()
            ))
            yield self._condition(UserFilterConnector('and', (
                term_conditions,
                UserFilterConnector('and', self.tree)
            )), aliases), True
        else:
            yield from ((self._condition(c, aliases), True) for c in self.tree)

        if self.include or self.exclude:
            uid_field = self.model_cls.crud_metadata.find_field_by_exposed_name('uid')
            for uid in self.include:
                yield self._condition(UserFilterItem(uid_field, 'neq', uid, False), aliases), False
            for uid in self.exclude:
                yield self._condition(UserFilterItem(uid_field, 'neq', uid, False), aliases), True

    def get_order(self, aliases):
        if self.orders:
            try:
                order_by_args, extra_columns = zip(*(
                    self.model_cls.column_order_by_field(
                        field=order.field, direction=order.direction,
                        modifier=order.modifier, value=order.value,
                        aliases=aliases,
                    )
                    for order in self.orders
                ))
            except ValueError as ex:
                # wrong data passed to modifier or its value
                logger.debug("Invalid order values", exc_info=True)
                raise MetadataValidationProblem(
                    title="Invalid order values",
                    detail="Invalid order modifier/value or data type in request",
                ) from ex
            else:
                extra_columns = tuple(itertools.chain(*(f for f in extra_columns if f is not None)))
                order_by_args = list(order_by_args)
            # TODO name of "id" field can be customized?
            if all('id' != order.field.internal_name for order in self.orders):
                order_by_args.append(self.model_cls.id.asc())
        else:
            # TODO default order can be customized?
            order_by_args = (self.model_cls.id.asc(),)
            extra_columns = tuple()
        has_extra_query = bool(self.include)
        return order_by_args, extra_columns, has_extra_query

    def iter_extra_query_criteria(self, aliases):
        uid_field = self.model_cls.crud_metadata.find_field_by_exposed_name('uid')
        yield self._condition(UserFilterConnector('or', (
            UserFilterItem(uid_field, 'eq', uid, False)
            for uid in self.include
        )), aliases)

    def __hashable(self):
        return (
            self.model_cls.__name__, self.custom_filter,
            self.term, self.tree, self.include, self.exclude, self.orders,
        )

    def __hash__(self):
        return hash(self.__hashable())

    def __repr__(self):
        return repr(self.__hashable())


class AliasesCollection:
    def __init__(self, model_cls):
        self.model_cls = model_cls
        self.alias_list = []
        self.pending_joins = []

    def _generate_alias(self, rel_cls):
        name = '{}_{}'.format(self.model_cls.__name__, rel_cls.__name__)
        for alias in self.alias_list:
            if sa.inspect(alias).name == name:
                return alias
        return orm.aliased(rel_cls, name=name)

    def _add_alias_join(self, query, relation, alias):
        # TODO do not add duplicate joins
        # return query.outerjoin(alias, relation)
        return query.outerjoin(alias, relation).options(orm.contains_eager(relation, alias=alias))

    def append(self, alias, relation=None):
        if isinstance(alias, orm.util.AliasedClass):
            pass
        elif alias is None:
            alias = self.model_cls.alias_for_relationship(relation)
        elif isinstance(alias, str):
            alias = self.model_cls.alias_for_key(alias)
        else:
            alias = self._generate_alias(alias)
        self.alias_list.append(alias)
        return alias

    def append_and_join(self, query, relation, alias):
        alias = self.append(alias, relation)
        return self._add_alias_join(query, relation, alias)

    def add_pending_join(self, join):
        self.pending_joins.append(join)

    def apply_pending_joins(self, query):
        for join in self.pending_joins:
            relation, alias = join
            query = self.append_and_join(query, relation, alias)
        self.pending_joins.clear()
        return query

    def __iter__(self):
        return iter(self.alias_list)


def make_search_queries(model_cls, filters, count, offset, field_names=None, with_extra_columns=False):
    query = model_cls.query

    aliases = AliasesCollection(model_cls)

    additional_names, field_name_pairs = parse_field_names(model_cls.crud_metadata, field_names)
    for field, _ in field_name_pairs:
        for join in field.needed_joins:
            aliases.add_pending_join(join)

    extra_query = query
    total_query = query
    for criterion, apply_to_total in filters.iter_criteria(aliases):
        query = aliases.apply_pending_joins(query)
        query = query.filter(criterion)
        if apply_to_total:
            total_query = query.filter(criterion)

    order_by_args, extra_columns, has_extra_query = filters.get_order(aliases)
    query = aliases.apply_pending_joins(query)

    if has_extra_query:
        for criterion in filters.iter_extra_query_criteria(aliases):
            extra_query = aliases.apply_pending_joins(extra_query)
            extra_query = extra_query.filter(criterion)

    if extra_columns and with_extra_columns:
        query = query.add_columns(*extra_columns)
        extra_query = extra_query.add_columns(*extra_columns)

    # Setting it initially will fail when using joinedload
    # See https://stackoverflow.com/a/39553869/1043456
    total_query = total_query.with_entities(sa.func.count(model_cls.id))

    query = query.order_by(*order_by_args)
    query = query.limit(count)
    query = query.offset(offset)

    if has_extra_query:
        query = extra_query.union_all(query)

    return query, total_query, extra_columns and with_extra_columns
