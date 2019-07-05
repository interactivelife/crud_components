from connexion import ProblemException


class ModelValidationError(ProblemException):
    def __init__(self, message):
        super(ModelValidationError, self).__init__(title="Model validation error", detail=message)


class MetadataValidationProblem(ProblemException):
    pass


class SkipExtension(Exception):
    """
    Raised when requesting a field from an extension that is not explicitly configured.
    """
    pass


class ExecutePostFlush(Exception):
    """
    Raised when creating a model and setting a field requires the flush (e.g. it needs the auto increment id)
    """
    pass
