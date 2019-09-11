import logging
import geoalchemy2 as ga
from colour import Color
from sqlalchemy_utils.functions import getdotattr
from geoalchemy2.shape import to_shape
from crud_components.exceptions import ModelValidationError
from crud_components.utils import Jsonifiable
from ...database import BaseModel, SummaryMixin, parse_field_names
from ...model_extensions import SkipExtension

logger = logging.getLogger(__name__)


class ModelReadVisitor:

    def __init__(self, session, with_extensions=None):
        self.session = session
        self.with_extensions = with_extensions
        self.include_map = {}
        self._visited = set()

    def visit_summary(self, instance):
        if instance is None:
            return None
        elif isinstance(instance, SummaryMixin):
            assert isinstance(instance, BaseModel)
            dikt = dict(
                text=instance.summary_text,
                subtext=instance.summary_subtext,
            )
            for field in instance.crud_metadata.summary_fields:
                name = field.extras['summary']
                if name is True:
                    name = None
                self.visit_field(dikt, instance, field, name=name)
            return dikt
        else:
            return self.visit_model(instance, summary=False)

    def visit_model(self, instance, field_names=None, summary=False, exclude=None, include=None):
        # include_map is not emptied after visiting the object.
        # Same visitor would save the include_map for more than 1 object.
        include = set(include or []).union(self.include_map.get(type(instance), ([], tuple()))[1] or [])
        if include or exclude:
            _ , include_field_name_pairs = parse_field_names(instance.crud_metadata, include)
            _ , exclude_field_name_pairs = parse_field_names(instance.crud_metadata, exclude)
            field_name_pairs = [x for x in include_field_name_pairs if x not in exclude_field_name_pairs]
            for f, sub_field_names in field_name_pairs:
                if f.reference_kind:
                    self.include_map[f.reference_to] = (f, sub_field_names)

        assert instance is None or isinstance(instance, BaseModel), 'Invalid instance {!r}'.format(type(instance))
        if summary and field_names:
            raise ValueError("Did not expect fields array in summary response")

        if summary:
            return self.visit_summary(instance)

        if instance is None:
            return None

        include = set(include or []).union(self.include_map.get(type(instance), ([], tuple()))[1] or [])
        additional_names, field_name_pairs = parse_field_names(instance.crud_metadata, field_names, exclude=exclude, include=include)
        dikt = self.visit_model_fields(instance, field_name_pairs, additional_names)

        if additional_names:
            # Still not empty
            raise ValueError("Unexpected field names: {}".format(', '.join(map(repr, additional_names.keys()))))
        return dikt

    def visit_model_fields(self, instance, field_name_pairs, additional_names=None):
        return instance.as_dict(self, field_name_pairs, with_extensions=self.with_extensions)

    def visit_field(self, dikt, instance, field, field_names=None, name=None):
        exposed_name = name or field.exposed_name
        extension = field.extras.get('extension')
        if extension is not None:
            try:
                extension_instance = instance.extension_instance(extension, self.session, with_extensions=self.with_extensions)
                expose = extension_instance.expose
            except SkipExtension:
                return
        elif field.exposed_as is None:
            expose = lambda s, f, **kw: getdotattr(s, f.internal_name)
        elif isinstance(field.exposed_as, str):
            expose = lambda s, f, **kw: getdotattr(s, f.exposed_as)
        elif callable(field.exposed_as):
            expose = field.exposed_as
        else:
            raise TypeError('Field exposed_as is expected to be a string or a function')
        exposed_value = expose(instance, field)
        if field.type == 'reference':
            if instance in self._visited:
                return 
            else:
                self._visited.add(instance)
        dikt[exposed_name] = self.visit_value(instance, field, exposed_value, field_names=field_names)

    def visit_value(self, instance, field, value, field_names):
        if field.type == 'reference':
            return self.visit_reference(instance, field, value, field_names)
        elif isinstance(value, ga.WKBElement):
            point = to_shape(value)
            return dict(longitude=str(point.x), latitude=str(point.y))
        elif isinstance(value, Jsonifiable):
            return value.as_jsonable_dict()
        elif isinstance(value, Color):
            return value.hex_l
        return value

    def visit_reference(self, instance, field, value, field_names):
        summary = field_names and '_summary' in field_names
        if summary and len(field_names) == 1:  # It's only X._summary that was matched
            field_names = None
        if field.reference_kind == 'single':
            return self.visit_model(value, field_names=field_names, summary=summary)
        elif field.reference_kind == 'multiple':
            assert value is not None
            return [
                self.visit_model(v, field_names=field_names, summary=summary)
                for v in value
            ]
        else:
            raise ModelValidationError("Something is very wrong")
