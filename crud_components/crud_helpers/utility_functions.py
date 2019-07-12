import logging
import base64
from Crypto.Hash import MD5
from itsdangerous import JSONWebSignatureSerializer, BadSignature
from flask import current_app, json
from connexion import ProblemException
from ..database import UserFilters

logger = logging.getLogger(__name__)


def special_pagination(body, body_to_paginate, model_cls):
    body = body or dict()
    count = body.get("count", 10)
    current_token = body.get("paginationToken")
    current_page = body.get("page")
    filters = UserFilters(
        model_cls, None,
    )

    identity_json = json.dumps(dict(
        filter=repr(filters),
        summary=False,
        field_names=tuple(),
    ), sort_keys=True)
    identity = MD5.new(base64.b64encode(identity_json.encode())).hexdigest()

    key = current_app.config['SEARCH_KEY']
    serializer = JSONWebSignatureSerializer(key)

    offset = 0
    if current_token:
        try:
            current_token_payload = serializer.loads(current_token)
        except BadSignature:
            logger.warning("Bad signature")
            raise ProblemException(title='Invalid request', detail="Bad pagination token")
        logger.debug('payload=%r identity=%r', current_token_payload, identity)
        if identity == current_token_payload.get('identity'):
            offset = current_token_payload.get('offset', 0)
            if current_page is not None:
                offset = (current_page - 1) * count
    else:
        logger.debug('payload=%r identity=%r', None, identity)

    total = len(body_to_paginate)

    results = body_to_paginate[offset:offset + count]

    has_next_result = offset + count < len(body_to_paginate)
    last_result = None

    if has_next_result:
        next_token_payload = dict(
            identity=identity,
            last_id=None,
            offset=offset + count,
        )
    else:
        next_token_payload = dict(
            identity=identity,
            last_id=None,
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
            more=bool(has_next_result),
            page=offset // count + 1,
        ),
    )
    return output, 200
