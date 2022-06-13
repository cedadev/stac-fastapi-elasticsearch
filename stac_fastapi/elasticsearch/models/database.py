# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '14 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.elasticsearch.config import settings

from elasticsearch_dsl import Document, InnerDoc, Nested
from elasticsearch_dsl import DateRange, GeoShape
from .utils import Coordinates, rgetattr

from typing import Optional, List, Dict
from fnmatch import fnmatch


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
    
    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

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

        try:
            # Throw away inclusivity flag with _
            lower, _ = extent.temporal.lower
            upper, _ = extent.temporal.upper

            lower = lower.isoformat() if lower else None
            upper = upper.isoformat() if upper else None
        except AttributeError:
            lower, upper = None, None

        try:
            coordinates = Coordinates.from_geojson(extent.spatial.coordinates)
        except AttributeError:
            coordinates = Coordinates.from_wgs84(DEFAULT_EXTENT['spatial'][0])

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
    
    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

    def search_assets(self):
        s = ElasticsearchAsset.search()
        s = s.filter('term', item_id=self.meta.id)
        s = s.exclude('term', categories='hidden')
        s = s.filter('exists', field='location')
        return s

    @property
    def assets(self):
        return list(self.search_assets().scan())

    @property
    def metadata_assets(self):
        s = self.search_assets()
        s = s.exclude('term', categories='data')
        return list(s.scan())

    def get_stac_assets(self):
        return {asset.meta.id: asset.to_stac() for asset in self.assets}

    def get_stac_metadata_assets(self):
        return {asset.meta.id: asset.to_stac() for asset in self.metadata_assets}

    def get_properties(self) -> Dict:

        try:
            properties = getattr(self, 'properties')
        except AttributeError:
            return {}

        if not hasattr(self, 'datetime'):
            if "start_datetime" not in properties or "end_datetime" not in properties:
                properties["start_datetime"] = None
                properties["end_datetime"] = None

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

    def get_geometry(self):
        ...

    def get_collection_id(self) -> str:
        """
        Return the collection id
        """
        try:
            return getattr(self, 'collection_id')
        except AttributeError:
            return


class ElasticsearchAsset(Document):

    type = 'Feature'

    @classmethod
    def search(cls, **kwargs):
        s = super().search(**kwargs)
        # s = s.exclude('term', categories__keyword='hidden')
        # s = s.filter('exists', field='filepath_type_location')

        return s
    
    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        return True

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

    def get_item_id(self) -> str:
        """
        Return the item id
        """
        try:
            return getattr(self, 'item_id')
        except AttributeError:
            return

    def get_roles(self) -> Optional[List]:

        try:
            roles = getattr(self, 'categories')
        except AttributeError:
            return

        return list(roles)

    def get_url(self) -> str:
        """
        Convert the path into a url where you can access the asset
        """
        if getattr(self, 'media_type', 'POSIX') == 'POSIX':
            return f'{settings.posix_download_url}{self.location}'
        else:
            return self.href
    
    def get_size(self) -> int:

        try:
            return getattr(self, 'size')
        except AttributeError:
            return

    def get_media_type(self) -> str:

        try:
            return getattr(self, 'media_type')
        except AttributeError:
            return

    def get_filename(self) -> str:

        try:
            return getattr(self, 'filename')
        except AttributeError:
            return

    def get_modified_time(self) -> str:

        try:
            return getattr(self, 'modified_time')
        except AttributeError:
            return

    def get_magic_number(self) -> str:

        try:
            return getattr(self, 'magic_number')
        except AttributeError:
            return

    def get_extension(self) -> str:

        try:
            return getattr(self, 'extension')
        except AttributeError:
            return

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
