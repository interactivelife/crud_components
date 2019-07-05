from medicall.api import db
from medicall.models import UserDefinedContentRole, UserDefinedStudioRole, Organization
from medicall.models.authz.role import CONTENT_ROLE_FUNCTION, STUDIO_ROLE_FUNCTION
from ..crud_hook import CrudHook
from medicall.integration.kc import kc_admin_api, kc_role_utils
from .uses_kc import uses_kc


class OrgKcHook(CrudHook):

    def post_create(self, body, model_dict):
        cnt_roles, std_roles = self._create_org_roles(model_dict)
        self._kc_post_create(model_dict['uid'], cnt_roles, std_roles)
        super().post_create(body, model_dict)

    def post_delete(self, model_dict):
        self._kc_post_delete(model_dict)
        super().post_delete(model_dict)

    @uses_kc
    def _kc_post_create(self, org_uid, cnt_roles, std_roles): # todo create org resource
        kc_admin_api.create_group({'name': org_uid}, None)
        org_grp = kc_role_utils.get_group(name=org_uid)

        # create role groups
        kc_admin_api.create_group({'name': 'content_roles'}, org_grp['id'])
        kc_admin_api.create_group({'name': 'studio_roles'}, org_grp['id'])
        cnt_grp = kc_role_utils.get_group(None, '/{}/content_roles'.format(org_grp['name']))
        std_grp = kc_role_utils.get_group(None, '/{}/studio_roles'.format(org_grp['name']))

        for r in cnt_roles:
            kc_admin_api.create_group({'name': r.uid}, cnt_grp['id'])
        for r in std_roles:
            kc_admin_api.create_group({'name': r.uid}, std_grp['id'])

    @uses_kc
    def _kc_post_delete(self, model_dict):
        grp = kc_role_utils.get_group(model_dict['uid'])
        kc_admin_api.delete_group(grp['id'])

    def _create_org_roles(self, model_dict):
        org = Organization.find(model_dict['uid'])
        content_roles = []
        for func in CONTENT_ROLE_FUNCTION:
            role = UserDefinedContentRole(name='default content role for {} for org {}'.format(func, model_dict['name']),
                                          organization=org, function=func)
            db.session.add(role)
            content_roles.append(role)

        studio_roles = []
        for func in STUDIO_ROLE_FUNCTION:
            role = UserDefinedStudioRole(name='default studio role for {} for org {}'.format(func, model_dict['name']),
                                         organization=org, function=func)
            db.session.add(role)
            studio_roles.append(role)
        db.session.flush()
        return content_roles, studio_roles