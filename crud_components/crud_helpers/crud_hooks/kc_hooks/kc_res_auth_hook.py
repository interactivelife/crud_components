from medicall.integration.kc.mixins import KeyCloakMixin
from ..crud_hook import CrudHook
from .uses_kc import uses_kc
from medicall.integration.kc.models.kc_resource import KcResource


class KcResAuthHook(CrudHook):

    def __init__(self, kc_res_utils):
        self.kc_res_utils = kc_res_utils

    def post_create(self, body, model_dict):
        self._post_create(body, model_dict)
        super().post_create(body, model_dict)

    def pre_update(self, body, model_uid):
        self._auth_resource(model_uid, [KcResource.DefaultScopes.UPDATE.value])
        super().pre_update(body, model_uid)

    def pre_delete(self, model_dict):
        try:
            self._auth_resource(model_dict['uid'], [KcResource.DefaultScopes.DELETE.value])
        except TypeError:
            raise
        super().pre_delete(model_dict)

    def post_delete(self, model_dict):
        self._post_delete(model_dict)
        super().post_delete(model_dict)

    @uses_kc
    def _post_create(self, body, model_dict):
        model_ins = self.helper.model_cls.find(model_dict['uid'])
        self._check_model()
        self.kc_res_utils.auth_creation(model_ins, model_ins.__class__.__name__)
        self.kc_res_utils.create_dependent_resources(model_ins)

    @uses_kc
    def _post_delete(self, model_dict):
        model_ins = self.helper.model_cls.find(model_dict['uid'])
        self._check_model()
        self.kc_res_utils.delete_resources(model_ins)

    @uses_kc
    def _auth_resource(self, model_uid, scopes):
        model_ins = self.helper.model_cls.find(model_uid)
        self._check_model()
        self.kc_res_utils.auth_resources(model_ins, scopes)

    def _check_model(self):
        if not issubclass(self.helper.model_cls, KeyCloakMixin):
            raise ValueError("Model should sublcass KeyCloakMixin")
