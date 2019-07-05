import logging
from sqlalchemy import orm
from sqlalchemy.ext.associationproxy import AssociationProxy, ASSOCIATION_PROXY
from sqlalchemy.ext.hybrid import HYBRID_PROPERTY

from crud_components.extension import ExtensionProperty
from ..field_metadata import FieldMetadata
from .abstract_metadata_builder import AbstractMetadataBuilder

logger = logging.getLogger(__name__)


class FieldMetadataBuilder(AbstractMetadataBuilder):

    @property
    def metadata_cls(self):
        return FieldMetadata

    def _process_info(self, attr_key, attr, info):
        extras = {}

        orderable = None
        searchable = None
        nullable = None
        unique = None
        attr_type = None
        ftype = None

        exposed = info.get('exposed', True)
        summary = info.get('summary', False)
        quick_search = info.get('quick_search', False)

        default_type = 'object' if not exposed else None

        if summary:
            extras['summary'] = summary

        exposed_as = info.get('exposed_as')
        unexposed_as = info.get('unexposed_as')
        exposed_name = info.get('exposed_name', self.expose_name(attr_key))
        display_name = info.get('display_name', self.beautify_name(exposed_name))
        alias_name = info.get('alias_name', "")

        generated = bool(info.get('generated', False))
        editable = bool(info.get('editable', not generated))
        readable = bool(info.get('readable', True))
        visible = bool(info.get('visible', not attr_key.startswith('_')))
        implicit = bool(info.get('implicit', True))

        needed_joins = info.get('needed_joins', tuple())

        assert not editable or (bool(exposed_as) == bool(
            unexposed_as)), 'If editable, you need to specify neither or both exposed_as and unexposed_as'

        if attr.is_property and isinstance(attr, orm.ColumnProperty):
            attr_type = 'column'
            f = attr.class_attribute
            ftype = info.get('type', self.TYPE_MAP.get(type(f.type), default_type))
            if ftype is None:
                raise ValueError("Did not find type of field {}.{}".format(self.mapper.entity.__name__, attr_key))
            nullable = bool(info.get('nullable', f.nullable))
            unique = bool(info.get('unique', f.unique))
            orderable = bool(info.get('orderable', ftype in self.ORDERABLE_TYPES))
            searchable = bool(info.get('searchable', ftype in self.SEARCHABLE_TYPES))
            if ftype == 'enum':
                extras['enum'] = [
                    {'value': v, 'displayName': v.replace('_', ' ').title()}  # TODO Localize enum display names
                    for v in f.type.enums
                ]
        elif attr.is_attribute and attr.extension_type is HYBRID_PROPERTY:
            attr_type = 'hybrid_property'
            ftype = info.get('type', default_type)
            if ftype is None:
                raise ValueError(
                    "Cannot infer type of hybrid property {}.{}".format(self.mapper.entity.__name__, attr_key))
            elif ftype == 'enum':
                raise ValueError("Cannot use enum type for hybrid property yet")
            nullable = bool(info.get('nullable', True))
            unique = bool(info.get('unique', False))
            editable = bool(info.get('editable', attr.fset is not None))
            generated = bool(info.get('generated', attr.fset is None))
            has_expr = attr.fget is not attr.expr
            orderable = bool(info.get('orderable', has_expr and ftype in self.ORDERABLE_TYPES))
            searchable = bool(info.get('searchable', has_expr and ftype in self.SEARCHABLE_TYPES))
        elif isinstance(attr, ExtensionProperty):
            attr_type = 'extension'
            extras['extension'] = attr.extension_cls
            ftype = info.get('type', default_type)
            if ftype is None:
                raise ValueError("Cannot infer type of extension property {}->{}.{}".format(
                    self.mapper.entity.__name__, attr.extension_cls.__name__, attr_key))
            elif ftype == 'enum':
                raise ValueError("Cannot use enum type for hybrid property yet")
            implicit = bool(info.get('implicit', False))
            nullable = bool(info.get('nullable', True))
            unique = bool(info.get('unique', False))
            editable = bool(info.get('editable', attr.fset is not None))
            orderable = bool(info.get('orderable', False))
            searchable = bool(info.get('searchable', False))
            assert not unique and not orderable and not searchable, 'Not supported yet'
        elif isinstance(attr, orm.CompositeProperty):
            attr_type = 'composite_property'
            ftype = info.get('type', default_type) or 'object'
            composite_class = attr.composite_class
            composite_class_name = composite_class.__name__
            extras['composite'] = {
                'class': composite_class_name,
                'columns': [c.name for c in attr.columns],
            }
            orderable = False
            searchable = False
            nullable = False
            unique = False
        elif isinstance(attr, AssociationProxy) and attr.extension_type is ASSOCIATION_PROXY:
            attr_type = 'association_proxy'
            if exposed:
                raise TypeError('Association proxy objects are kind of problematic')
            else:
                logger.debug('Cannot handle association proxy; unexposed, so ignoring field {}.{}'.format(
                    self.mapper.entity.__name__, attr_key))

        if ftype == 'tags':
            assert info['tags_kind'], "Couldn't get kind of tags field"
            extras['tags'] = dict()
            extras['tags']["kind"] = info['tags_kind']

        if ftype == 'uid' and 'uid_prefix' in info:
            extras['uid_prefix'] = info['uid_prefix']

        if 'purpose' in info:
            extras['purpose'] = info['purpose']

        return dict(
            internal_name=attr_key,
            quick_search = quick_search,
            exposed_name=exposed_name,
            exposed=exposed,
            exposed_as=exposed_as,
            unexposed_as=unexposed_as,
            display_name=display_name,
            alias_name=alias_name,
            type=ftype,
            orderable=orderable,
            searchable=searchable,
            editable=editable,
            nullable=nullable,
            unique=unique,
            visible=visible,
            generated=generated,
            implicit=implicit,
            needed_joins=needed_joins,
            readable=readable,
            attr_type=attr_type,
            extras=extras,
        )
