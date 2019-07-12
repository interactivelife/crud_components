from sqlalchemy import orm
from .field_metadata_builder import FieldMetadataBuilder
from .relationship_metadata_builder import RelationshipMetadataBuilder


class MetadataBuilderFactory:

    def get_builder(self, model_name, attr):
        if isinstance(attr, orm.RelationshipProperty):
            return RelationshipMetadataBuilder(model_name)
        else:
            return FieldMetadataBuilder(model_name)
