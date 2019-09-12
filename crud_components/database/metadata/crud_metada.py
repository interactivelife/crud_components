import re
import logging
import sqlalchemy as sa
import sqlalchemy_utils as sau
import geoalchemy2 as ga
from collections import OrderedDict, defaultdict
from sqlalchemy import orm
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm.base import ONETOMANY, MANYTOONE, MANYTOMANY
from sqlalchemy_utils import IntRangeType
from sqlalchemy.ext.hybrid import HYBRID_PROPERTY
from ..helpers import *
from ...model_extensions import *

logger = logging.getLogger(__name__)


class CrudMetadata:
    
    # indicates which fields to include in the response
    FIELD_METADATA_KEYS = {
        'internal_name': False,  # Do not include
        'exposed': False,  # Do not include
        'exposed_name': 'name',  # Change name
        'exposed_as': False,  # Do not include
        'unexposed_as': False,  # Do not include
        'reference': None,  # Only include if not none
        'tags': None,  # Only include if not none
        'enum': None,  # Only include if not none
        'summary': False,
        'implicit': False,
        'reference_kind': False,
        'reference_to': False,
        'reference_editable': False,
        'reference_creatable': False,
        'reference_verify_relationship': False,
        'reference_relationship_role': False,
        'reference_backref': False,
        'extension': False,
        'display_name': 'displayName',
        'alias_name': 'alias',
        'needed_joins': False,
        'executions': False,
        'attr_type': False,
    }
    MISSING = object()

    def __init__(self, cls, metadata_builder_factory):
        self.mapper = sa.inspect(cls)
        self.fields = OrderedDict()
        self.public = True
        self.name = cls.__name__
        self.cls = cls
        self.metadata_builder_factory = metadata_builder_factory

        # TODO how do we determine this?
        self.default_count = 10
        self.default_order = []
        self.editable = True
        self.creatable = True
        self.deletable = True
        self.pagination = True

        # For the searchable mixin
        self.quick_search_fields = dict()
        # For the summary mixin
        self.summary_fields = set()
        # For the model-level executions
        self.model_executions = tuple()

        # fields to be ignored
        self._ignore_fields = set()

    def build(self):
        self.configure_fields()

    def find_field_by_exposed_name(self, name):
        for f in self.fields.values():
            if f.exposed_name == name:
                return f
        else:
            raise AttributeError("Field {!r} not found".format(name))

    def find_field_by_internal_name(self, name):
        for f in self.fields.values():
            if f.internal_name == name:
                return f
        else:
            raise AttributeError("Field {!r} not found".format(name))

    def as_dict(self, field_names):
        assert self.public, 'Calling as_dict on an internal model is pointless'
        fields = (f for f in self.fields.values() if self.include_field(f, field_names))
        fields = self.reorder_fields(fields, field_names)
        return dict(
            fields=[self.translate_keys(f, self.overrides_for_field(f, field_names)) for f in fields],
            defaultOrder=self.default_order,
            defaultCount=self.default_count,
            editable=self.editable and any(f.editable for f in fields),
            creatable=self.creatable,
            deletable=self.deletable,
            pagination=self.pagination,
        )

    @classmethod
    def translate_keys(cls, field, overrides):
        data = (
            (cls.FIELD_METADATA_KEYS.get(k, cls.MISSING), k, overrides.get(k, v))
            for k, v in field._asdict().items()
        )
        return {
            kk if k is cls.MISSING or not k else k: v
            for k, kk, v in data
            if k is not False and not (k is None and v is None)
        }

    @classmethod
    def include_field(cls, field, field_names):
        if not field.exposed:
            return False
        if not (field.implicit or field.exposed_name in field_names):
            return False
        return True

    @classmethod
    def overrides_for_field(cls, field, field_names):
        if field.exposed_name in field_names:
            return {'visible': True}
        return {}

    @classmethod
    def reorder_fields(cls, fields, field_names):
        if not field_names:
            return fields

        def sort_key(f, field_names=field_names):
            try:
                return field_names.index(f.exposed_name)
            except ValueError:
                return len(field_names) + 1
        return sorted(fields, key=sort_key)

    def beautify_name(self, value):
        value = re.sub(r'([A-Z])', r' \1', value)
        return value.replace('_', ' ').title().strip()  # TODO Localize field names

    def expose_name(self, *values):
        # We have to use camelCase to keep it compatible with the client codegen
        # See https://github.com/swagger-api/swagger-codegen/issues/6530
        # And https://github.com/swagger-api/swagger-codegen/issues/4774
        camel_case = ''.join(x.capitalize() or '_' for word in values for x in word.split('_'))
        return camel_case[0].lower() + camel_case[1:]
        # return '_'.join(values)

    def expose_remote_name(self, attr_key, local_col, remote_col):
        exposed_name = remote_col.info.get('exposed_name', remote_col.key)
        # return self.expose_name(attr_key, exposed_name)
        modified_exposed_name = re.sub(remote_col.key + '$', exposed_name, local_col.key)
        return self.expose_name(modified_exposed_name)

    def configure_fields(self):
        props = iter_properties(self.mapper)
        
        # todo small hack
        props = filter(lambda prop: not (prop[0].extension_type is HYBRID_PROPERTY and prop[1] == 'attribute_getter'), props)
        
        def _ordering_accessor(element, default):
            _, _, *info_dicts = element
            for info_dict in info_dicts:
                try:
                    return info_dict['ordering']
                except KeyError:
                    pass
            else:
                return default
        props = sorted_properties(list(props), _ordering_accessor)

        ordering_hints = {}
        prop_keys = [p[1] for p in props]

        quick_search_field_names = {}
        summary_field_names = set()

        # First round, some properties (relationships mainly) may affect other properties
        for attr, attr_key, *infos in props:
            if attr_key in self._ignore_fields or attr_key[0] == '_':
                # Fields strictly for internal use
                continue
            info = {}
            for info_dict in reversed(infos):
                info.update(info_dict)

            quick_search = info.get('quick_search', None)
            if quick_search:
                quick_search_field_names[attr_key] = quick_search
            
            if info.get('summary', False):
                summary_field_names.add(attr_key)     

            if isinstance(attr, orm.RelationshipProperty):
                if len(attr.local_remote_pairs) > len(attr.local_columns):
                    # These have a secondary join, may be single or multiple
                    # Many-to-many relationships
                    logger.debug('Relationships with secondary joins need manual work for {}.{}'.format(self.mapper.entity.__name__, attr_key))
                    continue
                ordering_hints[attr_key] = max(prop_keys.index(c.key) for c in attr.local_columns) * 2 + 1
                for c in attr.local_columns:
                    c.info.setdefault('visible', False)
                for lcol, rcol in attr.local_remote_pairs:
                    exposed_as = rcol.info.get('exposed_as')
                    if exposed_as:
                        if isinstance(exposed_as, str):
                            exposed_as = '{}.{}'.format(attr_key, exposed_as)
                        lcol.info.setdefault('exposed_as', exposed_as)

                    unexposed_as = rcol.info.get('unexposed_as')
                    if unexposed_as:
                        lcol.info.setdefault('unexposed_as', unexposed_as)

                    exposed_name = rcol.info.get('exposed_name')
                    if exposed_name is not None:
                        lcol.info.setdefault('exposed_name', self.expose_remote_name(attr_key, lcol, rcol))

                    display_name = rcol.info.get('display_name')
                    if display_name is not None:
                        exposed_name = info.get('exposed_name', self.expose_name(attr_key))
                        modified_display_name = '{} {}'.format(
                            info.get('display_name', self.beautify_name(exposed_name)),  # TODO Localize ref names
                            display_name
                        )
                        lcol.info.setdefault('display_name', modified_display_name)

                    # Hide the foreign key columns if they're just an id
                    # If not (e.g. id and version id) send them both
                    lcol.info.setdefault('implicit', len(attr.local_remote_pairs) > 1)
                    copy_keys = ('type', 'orderable', 'searchable', 'localizable', 'visible', 'uid_prefix')
                    for k, v in rcol.info.items():
                        if k not in copy_keys:
                            continue
                        lcol.info.setdefault(k, v)

        # Second, put the relationships after all their foreign keys
        props = sorted(props, key=lambda c: ordering_hints.get(c[1], prop_keys.index(c[1]) * 2))

        # Third, add the extension properties
        execution_mapping = defaultdict(set)
        for extension in self.mapper.entity.__extensions__:
            for prop in extension.__properties__.values():
                props.append([prop, prop.name, prop.info])
            for execution in extension.__executions__.values():
                execution_mapping[execution.name, execution.stage].add(extension)

        self.after_info_pre_processing(quick_search_field_names, summary_field_names, props)

        # Fourth, render the crud metadata according to the modified info dictionaries
        # if self.name == 'Page':
        #     import ipdb;
        #     ipdb.set_trace()
        for attr, attr_key, *infos in props:
            assert attr_key is not None
            if attr_key in self._ignore_fields or attr_key[0] == '_':
                # Fields strictly for internal use
                continue
            info = {}
            for info_dict in reversed(infos):
                info.update(info_dict)

            if 'extras' not in info:
                info['extras'] = {}
            info['extras']['executions'] = execs = dict()
            for stage in Stage:
                try:
                    stage_execs = execution_mapping.pop((attr_key, stage))
                except KeyError:
                    execs[stage] = tuple()
                else:
                    execs[stage] = tuple(stage_execs)

            self.before_metadata_build(attr_key, attr, info)
            builder = self.metadata_builder_factory.get_builder(self.mapper.entity.__name__, attr, info)
            metadata = builder.build(attr_key, attr, info)

            assert attr_key not in self.fields, 'Duplicate field name?'
            self.fields[attr_key] = metadata

        self.model_executions = execs = dict()
        for stage in Stage:
            try:
                stage_execs = execution_mapping.pop((ExtensionPropertyExecution.MODEL_EXECUTION, stage))
            except KeyError:
                execs[stage] = tuple()
            else:
                assert stage != Stage.PRE_SET, 'Cannot use the PRE_SET stage for model-level executions'
                execs[stage] = tuple(stage_execs)

        assert not execution_mapping, 'Unused executions were defined: {!r}'.format(execution_mapping.keys())

        self.quick_search_fields = {self.fields[k]: operator for k, operator in quick_search_field_names.items()}
        self.summary_fields = frozenset(self.fields[k] for k in summary_field_names)

    def after_info_pre_processing(self, quick_search_field_names, summary_field_names, props):
        pass

    def before_metadata_build(self, attr_key, attr, info):
        pass

    def __repr__(self):
        return '{}({})'.format(type(self).__name__, self.name)
