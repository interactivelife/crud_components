import base64
import json
from Crypto.Hash import MD5
from crud_components.crud_helpers.crud_hooks import CrudHook
from .db_helper import DbHelper


class BaseCrudHandler(CrudHook):

    def __init__(self, logger, db, **kwargs):
        self.logger = logger
        self.db = db
        self.helper = kwargs.pop('db_helper', self.init_helper(**kwargs))

    def init_helper(self, **kwargs):
        return DbHelper(self.logger, self.db, **kwargs)

    def read(self, model_uid, fields, include=None, exclude=None, **kwargs):
        return self.helper.get_helper(model_uid, fields, include_fields=include, exclude_fields=exclude, **kwargs)

    def create(self, body, **kwargs):
        self.db.session.begin(nested=True)
        try:
            self.pre_create(body)
            model_dict, code = self.helper.create_helper(body, **kwargs)
            self.post_create(body, model_dict)
            self.on_success()
            self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.on_failure(e)
            raise e
        self.db.session.commit()
        return model_dict, code

    def update(self, model_uid, body, **kwargs):
        self.db.session.begin(nested=True)
        try:
            self.pre_update(body, model_uid)
            model_dict, code = self.helper.update_helper(model_uid, body, **kwargs)
            self.post_update(body, model_dict)
            self.on_success()
            self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.on_failure(e)
            raise e
        self.db.session.commit()
        return model_dict, code

    def bulk_update(self, body, **kwargs):
        # if at least one update fails, we should rollback all changes including successful updates
        updates = body.get("updates", {})
        self.db.session.begin(nested=True)
        try:
            for uid_str, value in updates.items():
                self.pre_update(body, uid_str)
                model_dict, code = self.helper.update_helper(uid_str, value, **kwargs)
                self.post_update(body, model_dict)
                # if code != 200: todo why are we returning 404?
                #     return NoContent, 404
            self.on_success()
            self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.on_failure(e)
            raise e
        self.db.session.commit()
        return dict(changes=1), 200

    def delete(self, model_uid, **kwargs):
        model_dict, code = self.read(model_uid, None)
        self.db.session.begin(nested=True)
        try:
            self.pre_delete(model_dict)
            res = self.helper.delete_helper(model_uid, **kwargs)
            self.post_delete(model_dict)
            self.db.session.commit()
        except Exception as e:
            self.db.session.rollback()
            self.on_failure(e)
            raise e
        self.db.session.commit()
        return res

    def metadata(self, fields):
        return self.helper.metadata_helper(fields)

    def search(self, body, exclude_fields=None, include_fields=None, **kwargs):
        return self.helper.query_search_helper(body, summary=False, exclude_fields=exclude_fields,
                                               include_fields=include_fields, **kwargs)

    def search_summary(self, body):
        return self.helper.query_search_helper(body, summary=True)

    @staticmethod
    def generate_hash(data):
        data = json.dumps(dict(data=data), sort_keys=True)
        return MD5.new(base64.b64encode(data.encode())).hexdigest()

    def on_success(self):
        super().on_success()

    def pre_create(self, body):
        super().pre_create(body)

    def post_create(self, body, model_dict):
        super().post_create(body, model_dict)

    def pre_update(self, body, model_uid):
        super().pre_update(body, model_uid)

    def post_update(self, body, model_dict):
        super().post_update(body, model_dict)

    def pre_delete(self, model_dict):
        super().pre_delete(model_dict)

    def post_delete(self, model_dict):
        super().post_delete(model_dict)

    def on_failure(self, exception):
        super().on_failure(exception)
