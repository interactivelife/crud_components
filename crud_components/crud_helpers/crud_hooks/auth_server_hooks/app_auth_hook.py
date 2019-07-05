from .auth_server_hook import AuthServerHook


class AppAuthHook(AuthServerHook):

    AUTH_SUFFIX = 'app'

    @property
    def auth_suffix(self):
        return self.AUTH_SUFFIX
