import abc


class CrudHook(abc.ABC):
    
    """
    Params:
        Pre-Hooks:
            body: The body (as a dict) of the request
        Post-Hooks:
            body: The body (as a dict) of the request
            model: The model instance after performing the requested operation
    """

    def on_success(self):
        pass

    def on_failure(self, exception):
        pass

    def pre_create(self, body):
        pass

    def post_create(self, body, model_dict):
        pass
    
    def pre_update(self, body, model_uid):
        pass
    
    def post_update(self, body, model_dict):
        pass
    
    def pre_delete(self, model_dict):
        pass
    
    def post_delete(self, model_dict):
        pass
