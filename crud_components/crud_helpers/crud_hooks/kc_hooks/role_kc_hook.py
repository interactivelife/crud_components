from medicall.models import UserDefinedContentRole, UserDefinedStudioRole, Application
from medicall.validators import parse_uid
from ..crud_hook import CrudHook
from medicall.integration.kc import kc_role_utils, kc_admin_api
from .uses_kc import uses_kc


class RoleKcHook(CrudHook):

    def post_create(self, body, model_dict):
        role = self._get_role(model_dict['uid'])
        self._kc_post_create(role)
        super().post_create(body, model_dict)

    def post_delete(self, model_dict):
        self._kc_post_delete(model_dict['uid'])
        super().post_delete(model_dict)

    @uses_kc
    def _kc_post_create(self, role):
        role_type = 'content_roles' if isinstance(role, UserDefinedContentRole) else 'studio_roles'
        kc_role_utils.create_role(role.uid, role_type, role.organization.uid)
        role_grp = kc_role_utils.get_group(name=role.uid)

        # add all app in org to role
        apps = Application.query.filter_by(organization=role.organization)
        for app in apps:
            kc_admin_api.create_group({'name': app.uid}, role_grp['id'])

    @uses_kc
    def _kc_post_delete(self, uid):
        role = self._get_role(uid)
        role_type = 'content_roles' if isinstance(role, UserDefinedContentRole) else 'studio_roles'
        kc_role_utils.delete_role(role.uid, role_type, role.organization.uid)

    def _get_role(self, uid_str):
        uid_prefix = parse_uid(uid_str).prefix
        if uid_prefix == UserDefinedContentRole.UID_PREFIX:
            model_cls = UserDefinedContentRole
        elif uid_prefix == UserDefinedStudioRole.UID_PREFIX:
            model_cls = UserDefinedStudioRole
        else:
            raise ValueError("Model UID doesn't correspond to neither UserDefinedContentRole nor UserDefinedStudioRole")

        role = model_cls.find(uid_str)
        if not role:
            raise LookupError("No Role exists for UID {}".format(uid_str))
        return role
