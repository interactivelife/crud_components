from collections import namedtuple
from ..field_info import FieldInfo, RelationshipInfo


# conversion to named tuples is used to enforce immutability
FieldMetadata = namedtuple('FieldMetadata', FieldInfo.get_info_keys())
RelationshipMetadata = namedtuple('RelationshipMetadata', RelationshipInfo.get_info_keys())

