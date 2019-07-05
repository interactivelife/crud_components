from .auth_server_hook import AuthServerHook


class UserAuthHook(AuthServerHook):

    AUTH_SUFFIX = 'user'
    STUDIO_MANAGED_FIELDS = ['name', 'phoneNumber', 'uid', 'belongs', 'contentBelongs']

    @property
    def auth_suffix(self):
        return self.AUTH_SUFFIX

    def post_create(self, body, model_dict):
        super().post_create(body, self._build_auth_request(body, model_dict['uid']))

    def post_update(self, body, model_dict):
        super().post_update(body, self._build_auth_request(body, model_dict['uid']))

    def post_delete(self, model_dict):
        auth_request = {'userId': model_dict['uid'], 'appId': model_dict['app_id']}
        self.auth_utils.notify_auth_server(auth_request, 'user/delete')

    def _build_auth_request(self, body, model_uid):
        auth_request = {}
        for tag in body:
            if tag not in self.STUDIO_MANAGED_FIELDS:
                auth_request[tag] = body[tag]
        auth_request['appId'] = body['appId']
        auth_request['userId'] = model_uid
        return auth_request
