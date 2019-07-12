from .field_metadata_factory import field_metadata_class_factory
from ..field_info import FieldInfo, RelationshipInfo

# conversion to named tuples is used to enforce immutability
FieldMetadata = field_metadata_class_factory('FieldMetadata', FieldInfo)
RelationshipMetadata = field_metadata_class_factory('RelationshipMetadata', RelationshipInfo)
