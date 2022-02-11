# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Aug 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import abc
from typing import TypedDict, Dict, Any
import elasticsearch_dsl

from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import CollectionLinks, ItemLinks

from stac_fastapi.elasticsearch.models import database

from dateutil import parser
from urllib.parse import urlparse

STAC_VERSION_DEFAULT = '1.0.0'


class Serializer(abc.ABC):
    """
    Defines serialization methods between API and data model
    """

    @classmethod
    @abc.abstractmethod
    def stac_to_db(
            cls,
            stac_data: TypedDict,
            exclude_geometry: bool = False
    ) -> elasticsearch_dsl.Document:
        """Transforms stac to database model"""
        ...

    @classmethod
    @abc.abstractmethod
    def db_to_stac(
            cls,
            db_model: elasticsearch_dsl.Document,
            base_url: str

    ) -> TypedDict:
        """Transform database model to stac"""
        ...


class ItemSerializer(Serializer):

    @classmethod
    def db_to_stac(cls,
                   db_model: database.ElasticsearchItem,
                   base_url: str
                   ) -> stac_types.Item:
        stac_extensions = getattr(db_model, 'stac_extensions', [])

        item_links = ItemLinks(
            base_url=base_url,
            collection_id=db_model.get_collection_id(),
            item_id=db_model.meta.id
        ).create_links()

        return stac_types.Item(
            type='Feature',
            stac_version=getattr(db_model, 'stac_version', STAC_VERSION_DEFAULT),
            stac_extensions=stac_extensions,
            id=db_model.meta.id,
            collection=db_model.get_collection_id(),
            bbox=db_model.get_bbox(),
            properties=db_model.get_properties(),
            links=item_links,
            assets=db_model.get_stac_assets()
        )

    @classmethod
    def stac_to_db(
            cls,
            stac_data: stac_types.Item,
            exclude_geometry=False
    ) -> database.ElasticsearchItem:
        db_item = database.ElasticsearchItem(
            type='item',
            id=stac_data.get('id'),
            bbox=stac_data.get('bbox'),
            collection_id=stac_data.get('collection'),
            properties=stac_data.get('properties', {}),
            stac_version=stac_data.get('stac_version'),
            stac_extensions=stac_data.get('stac_extensions')
        )
        db_item.meta.id = stac_data.get('id')
        return db_item


class CollectionSerializer(Serializer):

    @classmethod
    def db_to_stac(
            cls,
            db_model: database.ElasticsearchCollection,
            base_url: str
    ) -> stac_types.Collection:
        collection_links = CollectionLinks(
            collection_id=db_model.meta.id, base_url=base_url
        ).create_links()

        stac_extensions = getattr(db_model, 'stac_extensions', [])
        
        return stac_types.Collection(
            type='Collection',
            id=db_model.meta.id,
            stac_extensions=stac_extensions,
            stac_version=getattr(db_model, 'stac_version', STAC_VERSION_DEFAULT),
            title=getattr(db_model, 'title', ''),
            description=getattr(db_model, 'description', ''),
            keywords=db_model.get_keywords(),
            license=getattr(db_model, 'license', ' '),
            providers=getattr(db_model, 'providers', None),
            summaries=db_model.get_summaries(),
            extent=db_model.get_extent(),
            links=collection_links
        )

    @classmethod
    def stac_to_db(
            cls,
            stac_data: stac_types.Collection,
            exclude_geometry=False
    ) -> database.ElasticsearchCollection:
        db_collection = database.ElasticsearchCollection(
            id=stac_data.get('id'),
            stac_extensions=stac_data.get('stac_extensions'),
            stac_version=stac_data.get('stac_version'),
            title=stac_data.get('title'),
            description=stac_data.get('description'),
            license=stac_data.get('license'),
            summaries=stac_data.get('summaries'),
            providers=stac_data.get('providers'),
            assets=stac_data.get('assets'),
            type='collection',
            extent=cls.stac_to_db_extent(stac_data.get('extent')),
            keywords=stac_data.get('keywords')
        )
        db_collection.meta.id = stac_data.get('id')
        return db_collection

    @staticmethod
    def stac_to_db_extent(extent: Dict[str, Any]) -> Dict[str, Any]:
        extent = extent
        temporal = extent.get('temporal')
        if temporal:
            for k, d in temporal.items():
                extent['temporal'][k] = parser.parse(d).isoformat()
        return extent


class AssetSerializer(Serializer):

    @classmethod
    def db_to_stac(
            cls,
            db_model: database.ElasticsearchAsset,
            base_url: str

    ) -> TypedDict:
        pass

    @classmethod
    def stac_to_db(
            cls,
            stac_data: Dict,
            id: str,
            collection_id: str,
            exclude_geometry: bool = False
    ) -> elasticsearch_dsl.Document:
        url = urlparse(stac_data.get('href'))

        asset_db = database.ElasticsearchAsset(
            categories=stac_data.get('roles'),
            filename=stac_data.get('title'),
            filepath_type_location=url.path,
            magic_number=stac_data.get('type'),
            media_type=stac_data.get('media_type', 'POSIX')
        )
        asset_db.meta.id = id
        asset_db.collection_id = collection_id
        return asset_db
