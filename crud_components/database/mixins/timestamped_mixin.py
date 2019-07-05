import sqlalchemy as sa
from datetime import datetime
from ..metadata import FieldInfo


class TimestampedMixin:
    created = sa.Column(sa.DateTime, nullable=False, default=datetime.utcnow,
                        info=FieldInfo().ordering(2).generated().visible())

    updated = sa.Column(sa.DateTime, onupdate=datetime.utcnow,
                        info=FieldInfo().ordering(-1).generated().visible())
