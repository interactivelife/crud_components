from sqlalchemy import orm
from .field_metadata_builder import FieldMetadataBuilder
from .relationship_metadata_builder import RelationshipMetadataBuilder


class MetadataBuilderFactory:

    def get_builder(self, attr):
        if isinstance(attr, orm.RelationshipProperty):
            return RelationshipMetadataBuilder()
        else:
            return FieldMetadataBuilder
