from enum import Enum
from .field_info import FieldInfo


class RelationshipInfo(FieldInfo):

    class RelationshipRole(Enum):
        CHILD = 0
        PARENT = 1

    def purpose(self, purpose: str):
        """
        Should be used only on relationship columns, determines the purpose of the relationship.
        """
        self['purpose'] = purpose
        return self

    def reference_creatable(self, creatable=True):
        """
        If true, a referenced model will be created if no id is specified in the JSON of the referenced value.
        """
        self['reference_creatable'] = creatable
        return self

    def reference_editable(self, editable=True):
        """
        If true, a referenced model will be updated when an id is specified in the request body.

        Example:
            Models:
            A(id, name, b) B(id, name)
            (b is a reference to B)

            DB:
            A(1, 'a1')
            B(1, 'b1')

            Update request:
            Update A:
                {
                    id = 1,
                    name = 'new_name'
                    b = {
                        id=1,
                        name = 'new_name_for_b'
                    }
                }

            If the column "b" in a has reference_editable, then in the above request b(id=1) will be assigned to a(id=1)
            and also has its name updated. Else b(id=1) will only be assigned to a(id=1) and all other extra fields
            will be ignored.

        """
        self['reference_editable'] = editable
        return self

    def reference_verify_relationship(self, verify_relationship=True):
        """
        If true, make sure the referenced model is still linked to the same "parent"
        """
        self['reference_verify_relationship'] = verify_relationship
        return self

    def reference_relationship_role(self, relationship_role):
        """
        Either "parent" or "child" (see RelationshipRole enum)
        """
        self['reference_relationship_role'] = relationship_role
        return self

    def reference_backref(self, reference_backref: str):
        """
        The name of the field of the model on the other side of the relationship that references this model.
        Used for relationship verification.

        Example:
             Models:
             A(id, b)
             B(id, a)

             The reference_backref of A.b is B.a
        """
        self['reference_backref'] = reference_backref
        return self

    def reference_to(self, reference_to: str):
        """
        Name of the model that is being referenced (usually filled by default from sqla metadata)
        """
        self['reference_to'] = reference_to
        return self

    def reference_kind(self, reference_kind):
        """
        Either "single" or "multiple" (usually filled by default from sqla metadata)
        """
        self['reference_kind'] = reference_kind
        return self
