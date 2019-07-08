import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import ARRAY


class ArrayOfEnum(ARRAY):
    """
    The combination of ENUM and ARRAY is not directly supported by backend DBAPIs at this time.
    In order to send and receive an ARRAY of ENUM, use the following workaround type:

    Usage example:

        Column('data', ArrayOfEnum(ENUM('a', 'b, 'c', name='myenum')))

    See docs: http://docs.sqlalchemy.org/en/latest/dialects/postgresql.html#using-enum-with-array
    """

    def bind_expression(self, bindvalue):
        return sa.cast(bindvalue, self)

    def result_processor(self, dialect, coltype):
        super_rp = super(ArrayOfEnum, self).result_processor(
            dialect, coltype)

        def handle_raw_string(value):
            inner = re.match(r"^{(.*)}$", value).group(1)
            return inner.split(",") if inner else []

        def process(value):
            if value is None:
                return None
            return super_rp(handle_raw_string(value))
        return process
