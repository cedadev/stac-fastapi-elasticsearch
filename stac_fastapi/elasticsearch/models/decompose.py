# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '16 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pydantic.utils import GetterDict
from stac_fastapi.types.links import CollectionLinks, ItemLinks

from typing import Any


class Container:
    """
    Container class to translate between the Elasticsearch DSL document
    and pydantic. This container has not predudice and will accept anything
    with no opinions.
    """

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)


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
            stac_version=getattr(obj, 'stac_version', '1.0.0'),
            stac_extensions=stac_extensions,
            title=getattr(obj, 'title', ''),
            description=getattr(obj, 'description', ''),
            keywords=obj.get_keywords('keywords'),
            license=getattr(obj, 'license', ' '),
            providers=getattr(obj, 'providers', None),
            summaries=obj.get_summaries('properties'),
            extent=obj.get_extent('extent'),
            links=collection_links
        )

        super().__init__(db_model)


class ItemGetter(GetterDict):

    def __init__(self, obj: Any):

        stac_extensions = getattr(obj, 'stac_extensions', [])

        item_links = ItemLinks(
            base_url=obj.base_url,
            collection_id=obj.get_collection_id(),
            item_id=obj.meta.id
        ).create_links()

        db_model = Container(
            id = obj.meta.id,
            stac_version=getattr(obj, 'stac_version', '1.0.0'),
            stac_extensions=stac_extensions,
            geometry=None,
            bbox=obj.get_bbox(),
            properties=obj.get_properties(),
            links=item_links,
            assets=obj.get_stac_assets(),
            collection=obj.get_collection_id()
        )

        super().__init__(db_model)