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

        [[minLon, maxLat],[maxLon, minLat]]

        :param coordinates: GeoJSON formatted coordinates from elasticsearch
        """

        minlon = coordinates[0][0]
        maxlon = coordinates[1][0]
        minlat = coordinates[1][1]
        maxlat = coordinates[0][1]

        return cls(minlon, maxlon, minlat, maxlat)

    @classmethod
    def from_wgs84(cls, coordinates: List) -> 'Coordinates':
        """
        WGS84 formatted coordinates are in the form:

        [minLon, minLat, maxLon, maxLat]

        :param coordinates: WGS84 formatted coordinates
        """

        minlon = coordinates[0]
        maxlon = coordinates[2]
        minlat = coordinates[1]
        maxlat = coordinates[3]

        return cls(minlon, maxlon, minlat, maxlat)

    def to_wgs84(self) -> List[NumType]:
        """
        Exports the coordinates in WGS84 format

        [minLon, minLat, maxLon, maxLat]
        """
        
        return [self.minlon, self.minlat, self.maxlon, self.maxlat]

    def to_geojson(self) -> List[List[NumType]]:
        """
        Exports the coordinates in GeoJSON format

        [[minLon, maxLat],[maxLon, minLat]]
        """

        return [[self.minlon, self.maxlat],[self.maxlon, self.minlat]]


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