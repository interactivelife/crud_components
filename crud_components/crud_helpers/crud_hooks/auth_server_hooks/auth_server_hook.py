import abc

from medicall.integration.auth_server.auth_server_utils import auth_utils
from ..crud_hook import CrudHook


class AuthServerHook(CrudHook, abc.ABC):

    def __init__(self):
        self.auth_utils = auth_utils

    @property
    @abc.abstractmethod
    def auth_suffix(self):
        raise NotImplementedError()

    def post_create(self, body, model_dict):
        self._notify_auth_server(model_dict)
        super().post_create(body, model_dict)

    def post_update(self, body, model_dict):
        self._notify_auth_server(model_dict)
        super().post_update(body, model_dict)

    def _notify_auth_server(self, model_dict):
        self.auth_utils.notify_auth_server(model_dict, self.auth_suffix)