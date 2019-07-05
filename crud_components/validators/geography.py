__all__ = ('GeographyLocation', 'ga_point_from_dict', 'ga_point_from_tuple')

from decimal import Decimal, InvalidOperation
from collections import namedtuple

GeographyLocation = namedtuple('GeographyLocation', 'longitude,latitude,radius')


def ga_point_from_tuple(val):
    return ga_point_from_dict(val._asdict())


def ga_point_from_dict(val):
    """
    Takes in a dictionary with "longitude" and "latitude" keys and converts it to a format
    compatible with PostGIS (and GeoAlchemy2).
    If anything else is passed, it will raise a ValueError.

    :param val: Any user input
    :return: A PostGIS-compatible point geometry
    :raises ValueError: for any invalid value passed
    """
    try:
        lng = Decimal(val['longitude'])
        assert -180 <= lng <= 180, 'Longitude out of range'
        lat = Decimal(val['latitude'])
        assert -90 <= lat <= 90, 'Latitude out of range'
        return 'POINT({} {})'.format(lng, lat)
    except (KeyError, TypeError, InvalidOperation, AssertionError) as ex:
        raise ValueError('Invalid geography point {!r}'.format(val)) from ex
