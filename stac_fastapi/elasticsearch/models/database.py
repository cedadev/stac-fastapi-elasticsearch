# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '14 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from elasticsearch_dsl import Document, InnerDoc, Nested
from elasticsearch_dsl import DateRange, GeoShape
from .utils import Coordinates, rgetattr

from typing import Optional, List, Dict


DEFAULT_EXTENT = {
    'temporal': [[None, None]],
    'spatial': [[-180, -90, 180, 90]]
}



class Extent(InnerDoc):

    temporal = DateRange()
    spatial = GeoShape()


class ElasticsearchCollection(Document):
    """
    Collection class
    """
    type = 'Collection'
    extent = Nested(Extent)

    @classmethod
    def search(cls, **kwargs):
        s = super().search(**kwargs)
        s = s.filter('term', type='collection')

        return s

    def get_summaries(self) -> Optional[Dict]:
        """
        Turns the elastic-dsl AttrDict into a dict or None

        """
        try:
            summaries = getattr(self, 'summaries')
        except AttributeError:
            return

        if summaries:
            return summaries.to_dict()
        return

    def get_extent(self) -> Dict:
        """
        Takes the elastic-dsl Document and extracts the
        extent information from it.

        """
        try:
            extent = getattr(self, 'extent')
        except AttributeError:
            return DEFAULT_EXTENT

        # Throw away inclusivity flag with _
        lower, _ = extent.temporal.lower
        upper, _ = extent.temporal.upper

        lower = lower.isoformat() if lower else None
        upper = upper.isoformat() if upper else None

        coordinates = Coordinates.from_geojson(extent.spatial.coordinates)

        return dict(
            temporal=dict(interval=[[lower, upper]]),
            spatial=dict(bbox=[coordinates.to_wgs84()])
        )

    def get_keywords(self) -> Optional[List]:
        try:
            keywords = getattr(self, 'keywords')
        except AttributeError:
            return

        return list(keywords)


class ElasticsearchItem(Document):
    type = 'Feature'

    @classmethod
    def search(cls, **kwargs):
        s = super().search(**kwargs)
        s = s.filter('term', type='item')

        return s

    def search_assets(self):
        s = ElasticsearchAsset.search()
        s = s.filter('term', collection_id=self.meta.id)
        s = s.exclude('term', categories__keyword='hidden')
        s = s.filter('exists', field='filepath_type_location')
        return s

    @property
    def assets(self):
        return list(self.search_assets().scan())

    def get_stac_assets(self):
        return {asset.meta.id: asset.to_stac() for asset in self.assets}

    def get_properties(self) -> Dict:

        try:
            properties = getattr(self, 'properties')
        except AttributeError:
            return {}

        return properties.to_dict()

    def get_bbox(self):
        """
        Return a WGS84 formatted bbox

        :return:
        """

        try:
            coordinates = rgetattr(self, 'spatial.bbox.coordinates')
        except AttributeError:
            return

        return Coordinates.from_geojson(coordinates).to_wgs84()

    def get_collection_id(self) -> str:
        """
        Return the collection id
        """
        try:
            return getattr(self, 'collection_id')
        except AttributeError:
            return


class ElasticsearchAsset(Document):

    def get_url(self) -> str:
        """
        Convert the path into a url where you can access the asset
        """
        if getattr(self, 'media_type','POSIX'):
            return f'https://dap.ceda.ac.uk{self.filepath_type_location}'

    def get_roles(self) -> Optional[List]:

        try:
            roles = getattr(self, 'categories')
        except AttributeError:
            return

        return list(roles)

    def to_stac(self) -> Dict:
        """
        Convert Elasticsearch DSL asset into a STAC asset.
        """

        asset = dict(
            href=self.get_url(),
            type=getattr(self, 'magic_number', None),
            title=getattr(self, 'filename', None),
            roles=self.get_roles()
        )

        return asset