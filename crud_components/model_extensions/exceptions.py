

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
