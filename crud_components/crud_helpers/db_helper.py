import base64
import itertools
import json
from flask import current_app as app
from Crypto.Hash import MD5
from connexion import ProblemException, NoContent
from flask import current_app
from itsdangerous import JSONWebSignatureSerializer, BadSignature
from ..database import UserFilters, UidMixin, make_search_queries
from .model_visitor import ModelReadVisitor, ModelWriteVisitor


class DbHelper:

    def __init__(self, logger, db, **kwargs):
        self.model_cls = kwargs.pop('model_cls')
        self.logger = logger
        self.db = db
        self.read_visitor = kwargs.pop('read_visitor', ModelReadVisitor)
        self.write_visitor = kwargs.pop('write_visitor', ModelWriteVisitor)

    def query_search_helper(self, body, summary=False, exclude_fields=None, include_fields=None, **kwargs):
        with_extensions = kwargs.pop('with_extensions', None)
        custom_filter = kwargs.pop('custom_filter', None)

        body = body or dict()
        count = body.get("count", app.config['DEFAULT_COUNT'])
        current_token = body.get("paginationToken")
        current_page = body.get("page")
        filters = UserFilters(
            self.model_cls, custom_filter,
            filter=body.get("filter"), order=body.get("order"), term=body.get("term"),
            include=body.get("include"), exclude=body.get("exclude"),
        )
        field_names = body.get("fields")
        field_names = tuple(sorted(f for f in field_names if f and f.strip())) if field_names else tuple()

        identity_json = json.dumps(dict(
            filter=repr(filters),
            summary=summary,
            field_names=field_names,
        ), sort_keys=True)
        identity = MD5.new(base64.b64encode(identity_json.encode())).hexdigest()

        key = current_app.config['SEARCH_KEY']
        serializer = JSONWebSignatureSerializer(key)

        offset = 0
        if current_token:
            try:
                current_token_payload = serializer.loads(current_token)
            except BadSignature:
                self.logger.warning("Bad signature")
                raise ProblemException(title='Invalid request', detail="Bad pagination token")
            self.logger.debug('payload=%r identity=%r', current_token_payload, identity)
            if identity == current_token_payload.get('identity'):
                offset = current_token_payload.get('offset', 0)
                if current_page is not None:
                    offset = (current_page - 1) * count
        else:
            self.logger.debug('payload=%r identity=%r', None, identity)

        while True:
            try:
                query, total_query, has_extra = self.make_search_queries(
                    self.model_cls, filters, count + 1, offset, field_names,
                    with_extra_columns=True
                )
            except ValueError:
                self.logger.debug("Problem in the order", exc_info=True)
                raise ProblemException(title='Invalid request', detail="Problem in the requested order")

            # Fetch all results and store them in a list
            # We could do a .count() but that would result in an extra query
            # Anyway it looks like the sqlalchemy iterator just converts it to a list and exposes them as a generator
            # If we had access to the cursor, we could ask for rowcount before we iterate over the results
            query_results = query.all()

            # Get the total and page calculations in case we need to go "backwards" in the query
            total = total_query.scalar()
            pages, remainder = divmod(total, count)
            last_page = pages + (1 if remainder > 0 else 0)

            if offset == 0 or len(query_results) > 0:
                # We have some valid data (i.e. a valid offset was requested)
                # Break out of the loop and render them
                break

            # We got a request for a page that is out of bounds, go back to the last page and return that
            offset = (last_page - 1) * count

        iquery = iter(query_results)
        last_result = None

        r_visitor = self.read_visitor(session=self.db.session, with_extensions=with_extensions)
        results = []
        if has_extra:
            for r in itertools.islice(iquery, count):
                last_result = r[0]
                d = r_visitor.visit_model(r[0], field_names=field_names, summary=summary, exclude=exclude_fields, include=include_fields)
                extra = r._asdict()
                v = extra.pop(r.keys()[0])
                assert v is r[0], "We were assuming the result object is an ordered dict"
                d.update(extra)
                results.append(d)
        else:
            for r in itertools.islice(iquery, count):
                last_result = r
                d = r_visitor.visit_model(r, field_names=field_names, summary=summary, exclude=exclude_fields, include=include_fields)
                results.append(d)
        next_result = next(iquery, None)

        if next_result is not None:
            next_token_payload = dict(
                identity=identity,
                last_id=next_result.uid if isinstance(next_result, UidMixin) else None,
                offset=offset + count,
            )
        else:
            next_token_payload = dict(
                identity=identity,
                last_id=last_result.uid if isinstance(last_result, UidMixin) else None,
                offset=(offset + count) if last_result else offset,
            )
        next_token = serializer.dumps(next_token_payload, header_fields={'v': 1}).decode('ascii')

        output = dict(
            results=results,
            pagination=dict(
                nextToken=next_token,
                count=len(results),
                offset=offset + 1,
                total=total,
                more=bool(next_result is not None),
                page=offset//count + 1,
            ),
        )
        return output, 200

    def make_search_queries(self, model_cls, filters, count, offset, field_names, with_extra_columns=True):
        return make_search_queries(model_cls, filters, count, offset, field_names, with_extra_columns=with_extra_columns)

    def create_helper(self, body, **kwargs):
        only_field_names = kwargs.pop('only_field_names', None)
        with_whitelist_args = kwargs.pop('with_whitelist_args', None)
        with_extensions = kwargs.pop('with_extensions', None)

        w_visitor = self.write_visitor(session=self.db.session, with_whitelist_args=with_whitelist_args, with_extensions=with_extensions)
        model_ins = self.model_cls.create()
        self.db.session.add(model_ins)
        model_ins, _ = w_visitor.visit_model(model_ins, body, creating=True, only_field_names=only_field_names)
        model_ins.assert_uniqueness()
        w_visitor.pre_flush()
        self.db.session.flush()
        w_visitor.post_flush()
        self.db.session.flush()

        r_visitor = self.read_visitor(session=self.db.session, with_extensions=with_extensions)
        jsonable_dict = r_visitor.visit_model(model_ins)
        return jsonable_dict, 201

    def get_helper(self, uid_str, field_names, include_fields=None, exclude_fields=None, **kwargs):
        summary = kwargs.pop('summary', False)
        with_extensions = kwargs.pop('with_extensions', None)

        model_ins = self.model_cls.find(uid_str)
        if model_ins is None:
            return NoContent, 404
        field_names = tuple(sorted(f for f in field_names if f and f.strip())) if field_names else tuple()

        r_visitor = self.read_visitor(session=self.db.session, with_extensions=with_extensions)
        jsonable_dict = r_visitor.visit_model(model_ins, field_names=field_names, summary=summary, include=include_fields, exclude=exclude_fields)
        return jsonable_dict, 200

    def update_helper(self, uid_str, body, **kwargs):
        only_field_names = kwargs.pop('only_field_names', None)
        with_whitelist_args = kwargs.pop('with_whitelist_args', None)
        with_extensions = kwargs.pop('with_extensions', None)

        model_ins, _ = self._update(uid_str, body, only_field_names, with_whitelist_args, with_extensions)
        if model_ins is None:
            return NoContent, 404

        r_visitor = self.read_visitor(session=self.db.session, with_extensions=with_extensions)
        jsonable_dict = r_visitor.visit_model(model_ins)
        return jsonable_dict, 200

    def bulk_update_helper(self, body, **kwargs):
        only_field_names = kwargs.pop('only_field_names', None)
        with_whitelist_args = kwargs.pop('with_whitelist_args', None)
        with_extensions = kwargs.pop('with_extensions', None)

        updates = body.get("updates", {})

        total_changes = 0
        for uid_str, value in updates.items():
            # TODO the bulk update queries can be optimized
            model_ins, changes = self._update(uid_str, value, only_field_names, with_whitelist_args, with_extensions)
            if model_ins is None:
                return NoContent, 404

            total_changes += 1 if changes > 0 else 0
        return dict(changes=total_changes), 200

    def _update(self, uid_str, body, only_field_names=None, with_whitelist_args=None, with_extensions=None):
        model_ins = self.model_cls.find(uid_str)
        if model_ins is None:
            return None, 0

        w_visitor = self.write_visitor(session=self.db.session, with_whitelist_args=with_whitelist_args, with_extensions=with_extensions)
        model_ins, changes = w_visitor.visit_model(model_ins, body, creating=False, only_field_names=only_field_names)
        model_ins.assert_uniqueness()
        w_visitor.pre_flush()
        self.db.session.flush()
        changes += w_visitor.post_flush()
        self.db.session.flush()

        return model_ins, changes

    def delete_helper(self, uid_str, **kwargs):
        with_extensions = kwargs.pop('with_extensions', None)
        model_ins = self.model_cls.find(uid_str)

        if model_ins is None:
            return NoContent, 404
        self.db.session.delete(model_ins)

        del_visitor = self.write_visitor(session=self.db.session, with_extensions=with_extensions)
        del_visitor.queue_model_execution(model_ins, None)
        del_visitor.pre_flush_delete()

        return NoContent, 204

    def metadata_helper(self, field_names):
        field_names = tuple(f for f in field_names if f and f.strip()) if field_names else tuple()
        return self.model_cls.crud_metadata.as_dict(field_names), 200
