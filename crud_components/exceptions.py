from connexion import ProblemException


class ModelValidationError(ProblemException):
    def __init__(self, message):
        super(ModelValidationError, self).__init__(title="Model validation error", detail=message)


class MetadataValidationProblem(ProblemException):
    pass

