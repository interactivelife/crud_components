import abc
from medicall.models import User
from ..crud_hook import CrudHook
from .uses_kc import uses_kc
from medicall.integration.kc import kc_user_utils, KeyCloakIntegrationException


class UserKcHook(CrudHook, abc.ABC):

    @property
    @abc.abstractmethod
    def content_user(self):
        raise NotImplementedError()

    def post_create(self, body, model_dict):
        self._notify_kc_server(model_dict['uid'])
        super().post_create(body, model_dict)

    def post_update(self, body, model_dict):
        self._notify_kc_server(model_dict['uid'])
        super().post_update(body, model_dict)

    def pre_delete(self, model_dict):
        self._notify_kc_server_delete(model_dict['uid'])
        super().pre_delete(model_dict)

    @uses_kc
    def _notify_kc_server(self, user_uid):
        user_dict, role_names = self._get_required_user_info(user_uid)
        try:
            kc_user_utils.notify_keycloak_server(user_dict, role_names, self.content_user)
        except Exception as e:
            self._raise_ex(e)

    @uses_kc
    def _notify_kc_server_delete(self, user_uid):
        user_dict, role_names = self._get_required_user_info(user_uid)
        try:
            kc_user_utils.notify_keycloak_server_delete(user_dict, role_names, self.content_user)
        except Exception as e:
            self._raise_ex(e)

    def _get_required_user_info(self, user_uid):
        user = User.find(user_uid)
        user_dict = {
            'uid': user.uid,
            'fullname': user.name,
            'email': user.email,
            'phone_number': user.phone_number
        }
        role_names = [
            r.name for r in user.app_studio_roles + user.org_studio_roles + user.content_roles
        ]
        return user_dict, role_names

    def _raise_ex(self, e):
        raise KeyCloakIntegrationException(status_code=500, message='Error updating Keycloak server') from e


class ContentUserKcHook(UserKcHook):

    @property
    def content_user(self):
        return True


class StudioUserKcHook(UserKcHook):

    @property
    def content_user(self):
        return False
