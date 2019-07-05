from .auth_server_hook import AuthServerHook


class OrgAuthServerHook(AuthServerHook):
    
    AUTH_SUFFIX = 'org'

    @property
    def auth_suffix(self):
        return self.AUTH_SUFFIX
