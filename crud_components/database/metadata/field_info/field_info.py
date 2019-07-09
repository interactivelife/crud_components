from enum import Enum
from typing import Tuple


class FieldInfo(dict):

    NOTSET = object()

    @classmethod
    def get_info_keys(cls):
        return [key for key, val in cls.__dict__ if callable(val) and key not in ('__init__', 'get_info_keys')]

    def exposed(self, exposed=True):
        """
        Determines whether this field is shown in the client facing response
        """
        self['exposed'] = exposed
        return self

    def type(self, type: str):
        """
        The type of the field
        """
        self['type'] = type
        return self

    def exposed_name(self, name: str):
        """
        The name of the field as it appears in the client facing response
        """
        self['exposed_name'] = name
        return self

    def exposed_as(self, mapping_func: callable):
        """
        A function that determines how this field is displayed in the client facing response
        """
        self['exposed_as'] = mapping_func
        return self

    def unexposed_as(self, mapping_func: callable):
        """
        A function that maps a client facing field value to its internal representation.
        It is supposed to be the inverse function of exposed_as
        """
        self['unexposed_as'] = mapping_func
        return self

    def searchable(self, searchable=True):
        """
        Determines whether a model can be searched by this field
        """
        self['searchable'] = searchable
        return self

    def order_modifiers(self, modifiers: Tuple[callable]):
        """
        A tuple containing functions that return a number used for ordering
        """
        self['order_modifiers'] = modifiers
        return self

    def quick_search(self, quick_search='contains'):
        """
        Quick Search: Instead of searching for a pattern in a specific field, clients can search on multiple fields.
        This is quick search, it is called quick search because it is quicker to use from a users perspective.

        The fields that are valid to be used in the quick search are marked by calling this method.

        Example:
              Model: User(first_name, last_name, address)
              Quick search fields: (first_name, last_name)
              If the user wants to search for a user having 'ib' in first or last name he can do
              quick_search('ib') instead of search('ib' in u.first_name or 'ib' in u.last_name)

        :param quick_search (str) determines the operator for the search function. Currently only contains.
        """
        self['quick_search'] = quick_search
        return self

    def summary(self, summary=True):
        """
        If true, include this field when summary is requested in APIs (in addition to id, text, subtext)
        """
        self['summary'] = summary
        return self

    def editable(self, editable=True):
        """
        If true, the user is allowed to change the value once the model is created.
        """
        self['editable'] = editable
        return self

    def implicit(self, implicit=True):
        """
        If true, the field is included in the JSON even if not explicitly requested in API call
        """
        self['implicit'] = implicit
        return self

    def generated(self, generated=True):
        """
        If true, the user cannot specify a custom value (e.g. id autoincrement, create_at date, etc.)
        """
        self['generated'] = generated
        return self

    def needed_joins(self, needed_joins=True):
        """
        List of (relationship_name, alias_name) that need to be joined to read the value of the field
        """
        self['needed_joins'] = needed_joins
        return self

    def visible(self, visible=True):
        """
        If true, indicates to the frontend that this field should be shown to the user.

        Notice the difference between expose and visible:
        exposed: value should be sent to frontend
        visible: value should be shown to user on the frontend
        Buy definition, a visible field is an exposed field.
        """
        self['visible'] = visible
        return self

    def display_name(self, display_name: str):
        """
        Indicates the name the frontend should use to show to the user (by default based on title case of internal name)
        """
        self['display_name'] = display_name
        return self

    def ordering(self, ordering: int):
        """
        Overrides the default ordering of columns (which is based on order of declaration) in the db table
        """
        self['ordering'] = ordering
        return self

    def nullable(self, nullable=True):
        """
        Indicates whether a field can be nullable.
        """
        self['nullable'] = nullable
        return self

    def readable(self, readable=True):
        """
        Indicates whether a field is readable by a user (ex: passwords shouldn't be readable)
        """
        self['readable'] = readable
        return self

    def orderable(self, orderable=True):
        """
        Indicates whether this field can be used to sort model instances based on its values
        """
        self['orderable'] = orderable
        return self

    def uid_prefix(self, uid_prefix: str):
        self['uid_prefix'] = uid_prefix
        return self

    def attr_type(self, attr_type: str):
        """
        SQLA attribute type: column, hybrid prop, etc..
        """
        self['attr_type'] = attr_type
        return self

    def tags_kind(self, tags_kind: str):
        """
        Tag kind of the tag associated with the tile or whatever
        """
        self['tags_kind'] = tags_kind
        return self

    def extras(self, extras):
        """
        Optional dict containing some info
        """
        self['extras'] = extras
        return self
