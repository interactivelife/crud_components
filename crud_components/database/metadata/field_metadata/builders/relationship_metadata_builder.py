from sqlalchemy import orm
from sqlalchemy.ext.hybrid import HYBRID_PROPERTY
from sqlalchemy.orm.base import MANYTOONE, ONETOMANY

from crud_components.database.model_bases.abstract_base_model import AbstractBaseModel
from .field_metadata_builder import FieldMetadataBuilder
from ...field_info import RelationshipInfo


class RelationshipMetadataBuilder(FieldMetadataBuilder):

    def _process_info(self, attr_key, attr, info):
        info = super()._process_info(attr_key, attr, info)
        extras = {}

        assert attr.is_property and isinstance(attr, orm.RelationshipProperty)
        attr_type = 'relationship'
        ftype = 'reference'

        # basic attributes
        reference_creatable = bool(info.get('reference_creatable', False))
        reference_editable = bool(info.get('reference_editable', False))

        orderable = False
        searchable = False
        # TODO handle multiple foreign keys and unique constraints?
        nullable = any(c.nullable for c in attr.local_columns)
        unique = any(c.unique for c in attr.local_columns)

        # relationship details from SQLAlchemy mapper
        reference_to_cls = attr.mapper.entity
        reference_kind = self.RELATIONSHIP_TYPE_MAPPING[attr.direction, attr.uselist]
        reference_direction = attr.direction.name

        # relationship verification
        ownership_relation = attr.direction is MANYTOONE or attr.direction is ONETOMANY
        reference_verify_relationship = info.get('reference_verify_relationship', ownership_relation)
        reference_relationship_role = info.get('reference_relationship_role', None)
        if reference_relationship_role is None and reference_verify_relationship:
            if attr.direction is MANYTOONE:
                reference_relationship_role = RelationshipInfo.RelationshipRole.CHILD
            else:
                reference_relationship_role = RelationshipInfo.RelationshipRole.PARENT
        reference_backref = info.get('reference_backref', attr.back_populates)

        if attr.is_attribute and attr.extension_type is HYBRID_PROPERTY and info[type] == 'reference':
            assert reference_kind is not None, 'Hybrid property {}.{} of type reference without reference_kind'.format(
                self.mapper.entity.__name__, attr_key)
            assert reference_to_cls is not None, 'Hybrid property {}.{} of type reference without reference_to'.format(
                self.mapper.entity.__name__, attr_key)

        if info['type'] == 'reference':
            assert isinstance(reference_to_cls, type) and issubclass(reference_to_cls, AbstractBaseModel), \
                'reference_to={!r} not a model class'.format(reference_to_cls)
            assert isinstance(reference_relationship_role, (type(None), RelationshipInfo.RelationshipRole))
            assert reference_kind in ('single', 'multiple')
            assert reference_direction in (None, 'MANYTOONE', 'ONETOMANY', 'MANYTOMANY')
            if reference_direction is None:
                reference_direction = 'MANYTOONE' if reference_direction == 'single' else 'multiple'
            reference_to_name = reference_to_cls.__name__
            extras['reference'] = {
                'to': reference_to_name,
                'kind': reference_kind,
                'direction': reference_direction,
            }

        relationship_info = dict(
            type=ftype,
            orderable=orderable,
            searchable=searchable,
            nullable=nullable,
            unique=unique,
            reference_kind=reference_kind,
            reference_to=reference_to_cls,
            reference_creatable=reference_creatable,
            reference_editable=reference_editable,
            reference_verify_relationship=reference_verify_relationship,
            reference_relationship_role=reference_relationship_role,
            reference_backref=reference_backref,
            attr_type=attr_type,
        )

        info.update(relationship_info)
        info['extras'].update(extras)
        return info
