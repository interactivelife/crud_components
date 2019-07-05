from ..crud_hook import CrudHook
from medicall.integration.kc import kc_admin_api, kc_role_utils
from .uses_kc import uses_kc


class AppKcHook(CrudHook):

    def post_create(self, body, model_dict):
        self._kc_post_create(model_dict)
        super().post_create(body, model_dict)

    def post_delete(self, model_dict):
        self._kc_post_delete(model_dict)
        super().post_delete(model_dict)

    @uses_kc
    def _kc_post_create(self, model_dict):
        org_uid = model_dict['organization']['uid']
        org_grp = kc_role_utils.get_group(org_uid)
        role_grps = kc_admin_api.search_group(path='/{}/content_roles/*'.format(org_grp['name'])) \
                    + kc_admin_api.search_group(path='/{}/studio_roles/*'.format(org_grp['name']))
        for rg in role_grps:
            kc_admin_api.create_group({'name': model_dict['uid']}, rg['id'])

    @uses_kc
    def _kc_post_delete(self, model_dict):
        grps = kc_admin_api.search_group(model_dict['uid'])
        for g in grps:
            kc_admin_api.delete_group(g['id'])
