__all__ = ('sorted_properties', 'get_sorted_models', 'iter_properties', 'parse_field_names')

import logging
import itertools
from collections import defaultdict

from sqlalchemy.inspection import inspect
from sqlalchemy.ext.hybrid import HYBRID_PROPERTY
from sqlalchemy.ext.associationproxy import ASSOCIATION_PROXY
from sqlalchemy.sql import util as sql_util

logger = logging.getLogger(__name__)


def _default_ordering_accessor(c, default):
    return c.info.get('ordering', default)


def sorted_properties(columns, accessor=_default_ordering_accessor):
    """follows deceleration order by default but can be overriden bby specifying "ordering" in colinfo """
    max_ordering = len(columns)
    default_ordering = max_ordering // 2
    # stable sort, so the default ordering of sqla will be respected
    return sorted(
        columns,
        key=lambda c: min(accessor(c, default_ordering), max_ordering - 1) % max_ordering
    )


def iter_properties(mapper):
    return itertools.chain(
        (  # All columns and relationships
            (p, p.key, p.info, p.class_attribute.info)
            for p in mapper.attrs
        ),
        (  # All hybrid properties
            (p, p.__name__, p.info)
            for p in mapper.all_orm_descriptors
            if p.is_attribute and p.extension_type is HYBRID_PROPERTY
        ),
        (  # All association proxies
            (p, p.info.get('exposed_name', p.key), p.info)  # TODO the internal name cannot be determined here!
            for p in mapper.all_orm_descriptors
            if p.extension_type is ASSOCIATION_PROXY
        )
    )


def get_sorted_models(decl_base):
    """
    Returns an iterator over all declarative classes registered to the given declarative base
    Similar to `sqlalchemy_utils.functions.orm.get_class_by_table` and `Mapper._sorted_tables`

    Uses the ordered list of tables (ordered by sqla using topological sort) to infer model order

    :param base:
    :return:
    """
    table_to_mapper = {}
    for k, v in decl_base._decl_class_registry.items():
        if k == '_sa_module_registry':
            # Internal orm declarative stuff
            continue
        v = inspect(v)  # Get the mapper
        for mapper in v.base_mapper.self_and_descendants:  # Taken from Mapper._sorted_tables
            for table in mapper.tables:
                table_to_mapper.setdefault(table, mapper)

    extra_dependencies = []
    for table, mapper in table_to_mapper.items():
        super_ = mapper.inherits
        if super_:
            extra_dependencies.extend([
                (super_table, table)
                for super_table in super_.tables
            ])

    sorted_tables = sql_util.sort_tables(table_to_mapper, extra_dependencies=extra_dependencies)

    for table in sorted_tables:
        try:
            mapper = table_to_mapper[table]
        except KeyError:
            logging.warning('Table not mapped to %r: %r', decl_base.__name__, table)
        else:
            yield mapper.entity


def parse_field_names(crud_metadata, field_names, exclude=None, include=None):
    """
    Validates dot notation access from api calls and parses to tuples.
    ex: widget.name -> (widget, name)
    :param crud_metadata:
    :param field_names:
    :param exclude:
    :param include:
    :return:
    """
    exclude = tuple() if not exclude else exclude
    include = tuple() if not include else include
    if field_names:
        field_dotted_names = defaultdict(set)
        for name in field_names:
            parts = name.split('.', 1)
            if len(parts) == 1:
                field_dotted_names[parts[0]] = set()
            elif field_dotted_names[parts[0]] is not None:
                field_dotted_names[parts[0]].add(parts[1])
    else:
        field_dotted_names = {}

    pop_excluded = [(f, field_dotted_names.pop(f.exposed_name, None))
        for f in crud_metadata.fields.values()
        if f.internal_name in exclude]

    field_name_pairs = [
        (f, field_dotted_names.pop(f.exposed_name, None))
        for f in crud_metadata.fields.values()
        if f.exposed and f.readable and (f.implicit or f.exposed_name in field_dotted_names or f.internal_name in include) and f.internal_name not in exclude
    ]
    return field_dotted_names, field_name_pairs
