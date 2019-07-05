import flask


def uses_kc(f):
    """
    This decorator is to be used on functions calling KC apis
    :param f:
    :return:
    """
    def wrapper(*args):
        if flask.current_app.config['ENABLE_KEYCLOAK_INTEGRATION']:
            return f(*args)
    return wrapper
