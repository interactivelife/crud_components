import logging
from collections import defaultdict, OrderedDict

import geoalchemy2 as ga
from colour import Color
from sqlalchemy.ext.orderinglist import OrderingList
from sqlalchemy.orm.attributes import InstrumentedAttribute
from sqlalchemy_utils.functions import getdotattr
from geoalchemy2.shape import to_shape
from crud_components.exceptions import ModelValidationError, ExecutePostFlush
from crud_components.utils import Jsonifiable, parse_uid
from crud_components.database import BaseModel, SummaryMixin, parse_field_names, RelationshipInfo
from crud_components.extension import Stage
from crud_components.exceptions import SkipExtension

logger = logging.getLogger(__name__)


class ModelWriteVisitor:

    def __init__(self, parent_visitor=None, session=None, with_whitelist_args=None, with_extensions=None):
        assert parent_visitor is not None or session is not None
        assert parent_visitor is None or session is None or parent_visitor.session is session
        self.session = session if session is not None else parent_visitor.session
        assert self.session is not None
        self.with_extensions = with_extensions
        self.with_whitelist_args = with_whitelist_args or dict()
        self._post_flush_field_visits = []
        self._executions = defaultdict(list)
        self._model_executions = defaultdict(list)
        self._nested_visitors = []

    @staticmethod
    def whitelist(crud_metadata, user_dikt, creating, only_field_names=None,
                  ignore_generated=True, ignore_extra=True, ignore_uneditable=True, keep_extra=False):
        safe_dikt = user_dikt.copy()
        whitelist = OrderedDict(
            (f.exposed_name, f)
            for f in crud_metadata.fields.values()
            if f.exposed and (creating or f.editable)
        )

        if only_field_names:
            field_dotted_names = defaultdict(set)
            for name in only_field_names:
                first, *last = name.split('.', 1)
                if not last:
                    field_dotted_names[first] = set()
                elif field_dotted_names[first] is not None:
                    field_dotted_names[first].add(last[0])
            field_name_pairs = OrderedDict(
                (f.exposed_name, (f, field_dotted_names.pop(f.exposed_name)))
                for f in crud_metadata.fields.values()
                if f.exposed and f.exposed_name in field_dotted_names
                )
            for k in set(whitelist.keys()).difference(field_name_pairs.keys()):
                del whitelist[k]
        else:
            field_dotted_names = None
            field_name_pairs = OrderedDict(
                (f.exposed_name, (f, None))
                for f in whitelist.values()
            )

        assert not field_dotted_names, 'I think this should always be empty here, it is not user input'

        user_keys = set(user_dikt.keys())
        remove_keys = set()
        keep_keys = set()
        if ignore_generated:
            remove_keys.update(f.exposed_name for f in crud_metadata.fields.values() if f.generated)
        if ignore_uneditable and not creating:
            remove_keys.update(f.exposed_name for f in crud_metadata.fields.values() if not f.editable)
        if ignore_extra:
            remove_keys.update(user_keys.difference(whitelist.keys()))
        elif keep_extra:
            keep_keys.update(user_keys.difference(whitelist.keys()))

        forbidden = user_keys.difference(remove_keys).difference(keep_keys).difference(whitelist.keys())
        if forbidden:
            # TODO suggest correction of field name or give more details why a field is not available
            raise ModelValidationError("Cannot specify field{} in {!r}: {}".format(
                's' if len(forbidden) > 1 else '',
                crud_metadata,
                ', '.join(forbidden)
            ))

        if remove_keys:
            logger.debug("Ignoring keys %r in %r", remove_keys, crud_metadata)
        if keep_keys:
            logger.debug("Keeping extra keys %r in %r", keep_keys, crud_metadata)
        for k in remove_keys.intersection(user_keys):
            del safe_dikt[k]

        for field_exposed_name, (field, field_names) in field_name_pairs.items():
            if field_exposed_name in safe_dikt:
                yield field_exposed_name, field, safe_dikt.pop(field_exposed_name), field_names

        for k in sorted(safe_dikt.keys()):
            yield k, None, safe_dikt[k], None

    @property
    def check_for_duplicates(self):
        return True

    def queue_post_flush_field_visit(self, instance, field, value):
        self._post_flush_field_visits.append((instance, field, value))

    def queue_field_execution(self, instance, field, value):
        executions = field.extras.get('executions')
        if executions:
            execution_stages = (
                ex_stage
                for ex_stage, extensions in executions.items()
                if extensions and ex_stage in (Stage.PRE_FLUSH, Stage.POST_FLUSH)
            )
            for stage in execution_stages:
                self._executions[stage].append((instance, field, value))

    def queue_model_execution(self, instance, value):
        execution_stages = (
            ex_stage
            for ex_stage, extensions in instance.crud_metadata.model_executions.items()
            if extensions and ex_stage in (Stage.PRE_FLUSH, Stage.POST_FLUSH, Stage.PRE_VERSIONING, Stage.PRE_FLUSH_DELETE)
        )
        for stage in execution_stages:
            self._model_executions[stage].append((instance, value))

    def _run_executions(self, stage, instance, field, value):
        executions = field.extras.get('executions', None)
        overriden, override = None, False
        if executions is not None:
            for extension in executions[stage]:
                try:
                    extension_instance = instance.extension_instance(extension, self.session, self.with_extensions)
                    steps = extension_instance.execute(self, stage, instance, field, value, overriden=overriden)
                    for step_overriden, step_override in steps:
                        override = override or step_override
                        overriden = step_overriden if override else overriden
                except SkipExtension:
                    continue
        return overriden, override

    def _run_model_executions(self, stage, instance, value):
        for extension in instance.crud_metadata.model_executions[stage]:
            try:
                extension_instance = instance.extension_instance(extension, self.session, self.with_extensions)
                extension_instance.model_execute(self, stage, instance, value)
            except SkipExtension:
                continue

    @classmethod
    def _handle_execution_queue(cls, queue, method, stage):
        for instance, *args in queue[stage]:
            logger.debug('Running %s executions for %r', stage, instance)
            method(stage, instance, *args)
        queue[stage].clear()

    def add_child(self, child_visitor):
        self._nested_visitors.append(child_visitor)

    def post_flush(self):
        changes = 0
        for instance, field, value in self._post_flush_field_visits:
            _, change = self.visit_field(instance, field, value, _post_flush=True)
            assert change is not None
            changes += change
        self._post_flush_field_visits.clear()

        self._handle_execution_queue(self._executions, self._run_executions, Stage.POST_FLUSH)
        self._handle_execution_queue(self._model_executions, self._run_model_executions, Stage.POST_FLUSH)

        return changes

    def pre_flush(self):
        self._handle_execution_queue(self._executions, self._run_executions, Stage.PRE_FLUSH)
        self._handle_execution_queue(self._model_executions, self._run_model_executions, Stage.PRE_FLUSH)

    def pre_flush_delete(self):
        self._handle_execution_queue(self._executions, self._run_executions, Stage.PRE_FLUSH_DELETE)
        self._handle_execution_queue(self._model_executions, self._run_model_executions, Stage.PRE_FLUSH_DELETE)

    def clear(self):
        self._post_flush_field_visits.clear()
        self._executions.clear()
        self._model_executions.clear()
        self._nested_visitors.clear()

    def visit_model(self, instance, dikt, creating, only_field_names=None):
        iter_whitelist = list(self.whitelist(
            instance.crud_metadata, dikt, creating,
            only_field_names, **self.with_whitelist_args
        ))

        changes = 0
        for change in self.visit_model_fields(instance, iter_whitelist):
            assert change is not None
            changes += change
        self.session.add(instance)

        self.queue_model_execution(instance, dikt)

        return instance, changes

    def visit_model_fields(self, instance, iter_whitelist):
        return instance.update_from_dict(self, iter_whitelist, with_extensions=self.with_extensions)

    def visit_field(self, instance, field, value, _post_flush=False):
        assert '.' not in field.internal_name, 'We do not handle that case yet'

        try:
            extension = field.extras.get('extension')
            extension_instance = None
            if extension is not None:
                try:
                    extension_instance = instance.extension_instance(extension, self.session, self.with_extensions)
                except SkipExtension:
                    return value, 0
                assert extension_instance is not None

            unexposed_value, changed = self.visit_value(instance, field, value)

            overriden_unexposed_value, override = self._run_executions(Stage.PRE_SET, instance, field, value)
            if override:
                unexposed_value = overriden_unexposed_value

            if extension_instance is None:
                attribute_name = field.exposed_as if isinstance(field.exposed_as, str) else field.internal_name

                # assert hasattr(instance, attribute_name)
                setattr(instance, attribute_name, unexposed_value)

                # force reorder of the children if the collection_class is ordering_list
                attribute_value = getattr(instance, attribute_name)
                if isinstance(attribute_value, OrderingList):
                    attribute_value.reorder()

            else:
                unexposed_value, changed = extension_instance.update(instance, field, unexposed_value)

            if changed is None:
                changed = 0  # TODO determine if it changed

            self.queue_field_execution(instance, field, value)

            return unexposed_value, changed
        except ExecutePostFlush:
            logger.debug("Need to handle field %r=%r in instance %r post flush", field, value, instance)
            assert not _post_flush, 'Got an ExecutePostFlush exception in a post_flush call'
            self.queue_post_flush_field_visit(instance, field, value)
            return value, 0

    def visit_value(self, instance, field, value):
        if field.type == 'reference':
            rv, changes = self.visit_reference(instance, field, value)
            self.verify_relationship(instance, rv, field)
            return rv, changes
        elif callable(field.unexposed_as):
            return field.unexposed_as(instance, field, value), None
        else:
            return value, None

    def visit_reference(self, instance, field, value):
        if field.reference_kind == 'single':
            return self.visit_reference_single(instance, field, value)
        else:
            assert field.reference_kind == 'multiple'
            value_list, changes_list = self.visit_reference_multiple(instance, field, value)

            # check for duplicates
            if self.check_for_duplicates:
                uids = set()
                for orm_object in value_list:
                    if not orm_object.uid:
                        continue
                    if orm_object.uid in uids:
                        raise ModelValidationError('Adding 2 objects with the same UID')
                    uids.add(orm_object.uid)

            return value_list, changes_list

    def visit_reference_single(self, instance, field, value):
        assert value is None or isinstance(value, dict), 'Expected a dictionary'
        instance_cls = type(instance)
        attr = getattr(instance_cls, field.internal_name)
        assert isinstance(attr, InstrumentedAttribute)
        field_cls = attr.property.mapper.class_

        old_instance = getattr(instance, field.internal_name)
        assert old_instance is None or isinstance(old_instance, field_cls)
        if value is None:
            # We are clearing a reference
            changes = 0 if old_instance is None else 1
            return None, changes

        return self._reference_instance(field, field_cls, value, old_instance)

    def visit_reference_multiple(self, instance, field, value):
        field_cls = field.reference_to
        if value:
            value_list, changes_list = zip(*(
                self._reference_instance(field, field_cls, v, None)
                for v in value
                if v is not None
            ))
        else:
            value_list, changes_list = tuple(), tuple()
        return list(value_list), sum(changes_list)

    def verify_relationship(self, current_side, other_side, field):
        """
        Verifies that you aren't replacing on side of a relationship, between 2 models, by an another model
        :param current_side: The current model
        :param other_side: The model you are trying to set at the other side of the relationship
        :param field: The metadata of the field in the current_side model that links it to the other_side model
        :raises ModelValidationError when you are changing one end of the relationship
        """
        if not field.reference_verify_relationship or field.reference_backref is None:
            return

        # Make sure we're not getting None or a list of models
        assert isinstance(current_side, BaseModel)

        # determine relationship roles
        curr_to_other_rel = field.reference_relationship_role

        if curr_to_other_rel is RelationshipInfo.RelationshipRole.PARENT:
            # Being a PARENT, assumes the other side is a list of children
            # This will probably fail in ONETOONE cases (i.e. by explicitly setting uselist=False)
            # It will also fail if we are using a different kind of collection class
            assert isinstance(other_side, (list, tuple))
            other_field = field.reference_to.crud_metadata.find_field_by_internal_name(field.reference_backref)
            assert other_field.reference_relationship_role is RelationshipInfo.RelationshipRole.CHILD
            for child in other_side:
                # Reverse the relationship direction and delegate to the other part of this function
                self.verify_relationship(child, current_side, other_field)
        elif curr_to_other_rel is RelationshipInfo.RelationshipRole.CHILD:
            # When reaching here, we should not have a list, it must have been unrolled in the PARENT side
            assert isinstance(other_side, (BaseModel, type(None)))
            # get the original parent at the other side
            original_parent = getattr(current_side, field.internal_name)

            # check the two models at the other side are the same
            if not (original_parent is None or original_parent is other_side):
                raise ModelValidationError('Changing model of relationship is illegal: {}.{} = other {}'.format(
                    type(current_side).__name__, field.internal_name, type(other_side).__name__
                ))
        else:
            raise NotImplementedError('Unexpected value for relationship_role {!r}'.format(curr_to_other_rel))

    def _reference_instance(self, field, field_cls, value, old_instance):
        assert value is not None
        # We have a value for this field
        uid_str = value.pop('uid', None)
        uid = parse_uid(uid_str)
        new_instance, creating = None, False

        if uid is None:
            # We are trying to create a new object
            if not field.reference_creatable:
                raise ModelValidationError("Cannot {} create inline from {}".format(field_cls, self))
            new_instance, creating = field_cls.create(), True
        elif old_instance is None or uid.serial_id != old_instance.id:
            # We specified a uid (probably among other things)
            match = field_cls.find(uid)
            if match is None:
                raise ModelValidationError("Invalid uid specified")
            new_instance, creating = match, False
        else:
            # uid is not None, old instance is not None and uid == old uid
            # We specified a uid and it is the same as the old instance
            new_instance, creating = old_instance, False

        # Here, we must have a new instance
        assert new_instance is not None, "New instance should not be None: cls={} old={} new={} parent_field={}".format(
            field_cls, old_instance, new_instance, field
        )

        visited_instance, changes = self.visit_model(new_instance, value, creating)
        if changes and not field.reference_editable and not creating:
            # TODO Instead of changes, we can inspect and check the instance state
            raise ModelValidationError("Cannot edit inline")
        return visited_instance, changes
