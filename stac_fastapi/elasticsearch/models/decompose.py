# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '16 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pydantic.utils import GetterDict
from stac_fastapi.types.links import CollectionLinks
from elasticsearch_dsl import Document
from .utils import Coordinates

from typing import Any, Optional, Dict, List

DEFAULT_EXTENT = {
    'temporal': [[None, None]],
    'spatial': [[-180, -90, 180, 90]]
}


class Container:
    """
    Container class to translate between the Elasticsearch DSL document
    and pydantic. This container has not predudice and will accept anything
    with no opinions.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


def translate_extent(obj: Document, key: str) -> Dict:
    """
    Takes the elastic-dsl Document and extracts the
    extent information from it.

    :param obj: object to get extent from
    :param key: key to access extent
    """
    try:
        extent = getattr(obj, key)
    except AttributeError:
        return DEFAULT_EXTENT

    # Throw away inclusivity flag with _
    lower, _ = extent.temporal.lower
    upper, _ = extent.temporal.upper

    coordinates = Coordinates.from_geojson(extent.spatial.coordinates)

    return dict(
        temporal=dict(interval=[[lower.isoformat(), upper.isoformat()]]),
        spatial=dict(bbox=[coordinates.wgs84_format()])
    )


def get_summaries(obj: Document, key: str) -> Optional[Dict]:
    """
    Turns the elastic-dsl AttrDict into a dict or None

    :param obj:
    :param key:
    :return:
    """
    try:
        summaries = getattr(obj, key)
    except AttributeError:
        return

    return summaries.to_dict()


def get_keywords(obj: Any, key: str) -> Optional[List]:
    try:
        keywords = getattr(obj, key)
    except AttributeError:
        return

    return list(keywords)


def get_assets(obj: Any, key:str):
    return


class CollectionGetter(GetterDict):
    """
    Custom GetterDict

    Used by pydantic ORM mode when converting from collection ORM model
    to pydantic model
    """

    def __init__(self, obj: Any):
        """
        Decompose ORM model to pydantic model.

        :param obj: Object to decompose
        """

        collection_links = CollectionLinks(
            collection_id=obj.meta.id, base_url=obj.base_url
        ).create_links()

        stac_extensions = getattr(obj, 'stac_extensions', [])

        db_model = Container(
            id=obj.meta.id,
            stac_version=getattr(obj, 'stac_version', '1.0.0-beta.2'),
            stac_extensions=stac_extensions,
            title=getattr(obj, 'title', ''),
            description=getattr(obj, 'description', ''),
            keywords=get_keywords(obj, 'keywords'),
            license=getattr(obj, 'license', ''),
            providers=getattr(obj, 'providers', None),
            summaries=get_summaries(obj, 'properties'),
            extent=translate_extent(obj, 'extent'),
            links=collection_links
        )

        super().__init__(db_model)


class ItemGetter(GetterDict):

    def __init__(self, obj: Any):

        stac_extensions = getattr(obj, 'stac_extensions', [])

        db_model = Container(
            id = obj.meta.id,
            stac_version=getattr(obj, 'stac_version', '1.0.0-beta.2'),
            stac_extensions=stac_extensions,
            geometry=None,
            bbox=obj.get_bbox(),
            properties=obj.get_properties(),
            links=[],
            assets=obj.get_stac_assets(),
            collection=None
        )

        super().__init__(db_model)