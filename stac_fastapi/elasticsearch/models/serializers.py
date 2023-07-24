# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "11 Aug 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import abc
from typing import Any, Dict, TypedDict

import elasticsearch_dsl
from dateutil import parser
from stac_fastapi.elasticsearch.models import database
from stac_fastapi.types import stac as stac_types
from stac_fastapi_asset_search.types import Asset


class Serializer(abc.ABC):
    """
    Defines serialization methods between API and data model
    """

    @classmethod
    @abc.abstractmethod
    def stac_to_db(
        cls, stac_data: TypedDict, exclude_geometry: bool = False
    ) -> elasticsearch_dsl.Document:
        """Transforms stac to database model"""
        ...

    @classmethod
    @abc.abstractmethod
    def db_to_stac(
        cls, db_model: elasticsearch_dsl.Document, base_url: str
    ) -> TypedDict:
        """Transform database model to stac"""
        ...


class ItemSerializer(Serializer):
    @classmethod
    def db_to_stac(
        cls, db_model: database.ElasticsearchItem, base_url: str
    ) -> stac_types.Item:

        return stac_types.Item(
            type="Feature",
            stac_version=db_model.get_stac_version(),
            stac_extensions=db_model.get_stac_extensions(),
            id=db_model.get_id(),
            collection=db_model.get_collection_id(),
            bbox=db_model.get_bbox(),
            geometry=None,
            properties=db_model.get_properties(),
            links=db_model.get_item_links(base_url),
            assets=db_model.get_stac_assets(),
        )

    @classmethod
    def stac_to_db(
        cls, stac_data: stac_types.Item, exclude_geometry=False
    ) -> database.ElasticsearchItem:
        db_item = database.ElasticsearchItem(
            type="item",
            id=stac_data.get("id"),
            bbox=stac_data.get("bbox"),
            collection_id=stac_data.get("collection"),
            properties=stac_data.get("properties", {}),
            stac_version=stac_data.get("stac_version"),
            stac_extensions=stac_data.get("stac_extensions"),
        )
        db_item.meta.id = stac_data.get("id")
        return db_item


class CollectionSerializer(Serializer):
    @classmethod
    def db_to_stac(
        cls, db_model: database.ElasticsearchCollection, base_url: str
    ) -> stac_types.Collection:

        return stac_types.Collection(
            type="Collection",
            id=db_model.get_id(),
            stac_extensions=db_model.get_stac_extensions(),
            stac_version=db_model.get_stac__version(),
            title=db_model.get_title(),
            description=db_model.get_description(),
            keywords=db_model.get_keywords(),
            license=db_model.get_license(),
            providers=db_model.get_providers(),
            summaries=db_model.get_summaries(),
            extent=db_model.get_extent(),
            links=db_model.get_links(base_url),
        )

    @classmethod
    def stac_to_db(
        cls, stac_data: stac_types.Collection, exclude_geometry=False
    ) -> database.ElasticsearchCollection:
        db_collection = database.ElasticsearchCollection(
            id=stac_data.get("id"),
            stac_extensions=stac_data.get("stac_extensions"),
            stac_version=stac_data.get("stac_version"),
            title=stac_data.get("title"),
            description=stac_data.get("description"),
            license=stac_data.get("license"),
            summaries=stac_data.get("summaries"),
            providers=stac_data.get("providers"),
            assets=stac_data.get("assets"),
            type="collection",
            extent=cls.stac_to_db_extent(stac_data.get("extent")),
            keywords=stac_data.get("keywords"),
        )
        db_collection.meta.id = stac_data.get("id")
        return db_collection

    @staticmethod
    def stac_to_db_extent(extent: Dict[str, Any]) -> Dict[str, Any]:
        extent = extent
        temporal = extent.get("temporal")
        if temporal:
            for k, d in temporal.items():
                extent["temporal"][k] = parser.parse(d).isoformat()
        return extent


class AssetSerializer(Serializer):
    @classmethod
    def db_to_stac(
        cls,
        db_model: database.ElasticsearchAsset,
        base_url: str,
        collection_id: str,
    ) -> Asset:

        return Asset(
            type="Feature",
            stac_version=db_model.get_stac_version(),
            stac_extensions=db_model.get_stac_extensions(),
            asset_id=db_model.get_id(),
            roles=db_model.get_roles(),
            item=db_model.get_item_id(),
            bbox=db_model.get_bbox(),
            href=db_model.get_url(),
            filename=db_model.get_filename(),
            size=db_model.get_size(),
            modified_time=db_model.get_modified_time(),
            magic_number=db_model.get_magic_number(),
            extension=db_model.get_extension(),
            media_type=db_model.get_media_type(),
            properties=db_model.get_properties(),
            links=db_model.get_links(base_url, collection_id),
        )

    @classmethod
    def stac_to_db(
        cls, stac_data: Asset, exclude_geometry=False
    ) -> database.ElasticsearchAsset:
        db_item = database.ElasticsearchAsset(
            id=stac_data.get("id"),
            roles=stac_data.get("categories"),
            bbox=stac_data.get("bbox"),
            item_id=stac_data.get("item"),
            location=stac_data.get("location"),
            filename=stac_data.get("filename"),
            size=stac_data.get("size"),
            modified_time=stac_data.get("modified_time"),
            magic_number=stac_data.get("magic_number"),
            extension=stac_data.get("extension"),
            media_type=stac_data.get("media_type"),
            properties=stac_data.get("properties", {}),
            stac_version=stac_data.get("stac_version"),
            stac_extensions=stac_data.get("stac_extensions"),
        )
        db_item.meta.id = stac_data.get("id")
        return db_item
