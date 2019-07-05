import logging
import medicall.constants as C

from sqlalchemy import func

from medicall.models import Tile, VersionedTile, TileTranslation, WidgetInstance, VersionedSharedWidgetInstance
from medicall.api import db
from medicall.database import UserFilters
from medicall.modelhelpers.traversal import ModelReadVisitor

import base64
from Crypto.Hash import MD5
from itsdangerous import JSONWebSignatureSerializer, BadSignature
from flask import current_app, json
from connexion import ProblemException


logger = logging.getLogger(__name__)


def get_swi_use(uid):
    result = []
    q = db.session.query(Tile, func.max(VersionedTile.version_id)) \
        .join(TileTranslation, Tile._current_translation) \
        .join(VersionedTile, Tile.version_map) \
        .join(WidgetInstance, VersionedTile.widget_instances) \
        .join(VersionedSharedWidgetInstance, WidgetInstance.shared_widget_instance) \
        .filter(
        (VersionedSharedWidgetInstance.id == uid.serial_id)
        # (SharedWidgetInstance.id == uid.serial_id) &
        # (SharedWidgetInstance.version_id == uid.version)
    ).group_by(Tile.id)

    r_visitor = ModelReadVisitor(session=db.session)
    for tile, max_version_id in q:
        jsonable_dict = r_visitor.visit_model(tile, summary=True)
        result.append(jsonable_dict)
    return result


def special_pagination(body, body_to_paginate, model_cls):
    body = body or dict()
    count = body.get("count", C.DEFAULT_COUNT)
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