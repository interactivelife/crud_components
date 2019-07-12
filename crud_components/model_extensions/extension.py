from .extension_meta import ExtensionMeta
from .extension_config import ExtensionConfiguration
from .extension_execution import ExtensionPropertyExecution


class Extension(metaclass=ExtensionMeta):
    __abstract__ = True
    __model__ = None
    __implicit__ = False
    __properties__ = {}
    __executions__ = {}

    def __init__(self, session, instance):
        self.session = session
        self.instance = instance

    @classmethod
    def with_arguments(cls, *args, **kwargs):
        return ExtensionConfiguration(cls, args, kwargs)

    @classmethod
    def sorted_executions(cls, stage, field_name):
        for v in sorted(cls.__executions__.values(), key=lambda x: x.priority):
            if v.name == field_name and v.stage == stage:
                yield v

    def expose(self, instance, field, **kw):
        assert instance is self.instance
        return getattr(self, field.internal_name)

    def update(self, instance, field, value):
        assert instance is self.instance
        # before = getattr(self, field.internal_name)
        setattr(self, field.internal_name, value)
        # after = getattr(self, field.internal_name)
        # return value, 0 if before == after else 1
        return value, 1  # TODO change detection for property extensions?

    def model_execute(self, parent_visitor, stage, instance, value=None):
        assert instance is self.instance
        for execution_function in self.sorted_executions(stage, ExtensionPropertyExecution.MODEL_EXECUTION):
            if execution_function.with_parent_visitor:
                execution_function(self, value=value, parent_visitor=parent_visitor)
            else:
                execution_function(self, value=value)

    def execute(self, parent_visitor, stage, instance, field, value, overriden=None):
        assert instance is self.instance
        for execution_function in self.sorted_executions(stage, field.internal_name):
            kwargs = dict()
            if execution_function.with_parent_visitor:
                kwargs['parent_visitor'] = parent_visitor
            if execution_function.overrides:
                kwargs['overriden'] = overriden
            retval = execution_function(self, value=value, **kwargs)
            if retval is None or not execution_function.overrides:
                yield None, False
            else:
                yield retval, True
