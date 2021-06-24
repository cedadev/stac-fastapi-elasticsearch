# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '18 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from typing import List, Union
import functools

NumType = Union[float, int]


class Coordinates:
    """
    Takes care of coordinate transformations
    """

    def __init__(self, minlon, maxlon, minlat, maxlat):
        self.minlon = minlon
        self.maxlon = maxlon
        self.minlat = minlat
        self.maxlat = maxlat

    @classmethod
    def from_geojson(cls, coordinates: List[List[NumType]]) -> 'Coordinates':
        """
        GeoJSON formatted coordinates are in the form:
        NW[lon, lat], SE[lon, lat]

        :param coordinates: GeoJSON formatted coordinates from elasticsearch
        :return:
        """

        minlon = coordinates[0][0]
        maxlon = coordinates[1][0]
        minlat = coordinates[1][1]
        maxlat = coordinates[0][1]

        return cls(minlon, maxlon, minlat, maxlat)

    def wgs84_format(self) -> List[NumType]:
        """
        Exports the coordinates in WGS84 format
        :return:
        """

        return [self.minlon, self.minlat, self.maxlon, self.maxlat]


def rgetattr(obj, attr, *args):
    """
    Recursive getattr which can also use dotted attr strings to retrieve
    nested objects.

    :param obj: The object to access
    :param attr: The attribute to access, can be dotted
    :param args: args to pass to getattr

    :return:
    """
    def _getattr(obj, attr):
        return getattr(obj, attr, *args)
    return functools.reduce(_getattr, [obj] + attr.split('.'))