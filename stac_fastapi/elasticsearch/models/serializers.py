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
from urllib.parse import urljoin

import elasticsearch_dsl
from dateutil import parser
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.links import CollectionLinks, ItemLinks
from stac_fastapi_asset_search.types import Asset, AssetLinks
from stac_pydantic.shared import MimeTypes

from stac_fastapi.elasticsearch.models import database

STAC_VERSION_DEFAULT = "1.0.0"


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
        stac_extensions = getattr(db_model, "stac_extensions", [])

        item_links = ItemLinks(
            base_url=base_url,
            collection_id=db_model.get_collection_id(),
            item_id=db_model.meta.id,
        ).create_links()

        return stac_types.Item(
            type="Feature",
            stac_version=getattr(db_model, "stac_version", STAC_VERSION_DEFAULT),
            stac_extensions=stac_extensions,
            id=db_model.meta.id,
            collection=db_model.get_collection_id(),
            bbox=db_model.get_bbox(),
            geometry=None,
            properties=db_model.get_properties(),
            links=item_links,
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


class ItemAssetSearchSerializer(ItemSerializer):
    @classmethod
    def db_to_stac(
        cls, db_model: database.ElasticsearchItem, base_url: str
    ) -> stac_types.Item:
        item = super().db_to_stac(db_model, base_url)

        if db_model.data_assets_count > 25:

            inline_assets = [
                {
                    "href": urljoin(
                        base_url,
                        f"collections/{item['collection']}/items/{item['id']}/asset_filelist.json",
                    ),
                    "roles": ["filelist", "https"],
                }
            ]

            meta_assets = list(db_model.get_stac_metadata_assets().values())

            if len(meta_assets) > 0:

                inline_assets.append(meta_assets)

        else:
            inline_assets = list(db_model.get_stac_assets().values())

        item["assets"] = inline_assets

        return item


class CollectionSerializer(Serializer):
    @classmethod
    def db_to_stac(
        cls, db_model: database.ElasticsearchCollection, base_url: str
    ) -> stac_types.Collection:
        collection_links = CollectionLinks(
            collection_id=db_model.meta.id, base_url=base_url
        ).create_links()

        stac_extensions = getattr(db_model, "stac_extensions", [])

        return stac_types.Collection(
            type="Collection",
            id=db_model.meta.id,
            stac_extensions=stac_extensions,
            stac_version=getattr(db_model, "stac_version", STAC_VERSION_DEFAULT),
            title=getattr(db_model, "title", ""),
            description=getattr(db_model, "description", ""),
            keywords=db_model.get_keywords(),
            license=getattr(db_model, "license", " "),
            providers=getattr(db_model, "providers", None),
            summaries=db_model.get_summaries(),
            extent=db_model.get_extent(),
            links=collection_links,
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
        stac_extensions = getattr(db_model, "stac_extensions", [])

        asset_links = AssetLinks(
            base_url=base_url,
            collection_id=collection_id,
            item_id=db_model.get_item_id(),
            asset_id=db_model.meta.id,
        ).create_links()

        return Asset(
            type="Feature",
            stac_version=getattr(db_model, "stac_version", STAC_VERSION_DEFAULT),
            stac_extensions=stac_extensions,
            asset_id=db_model.meta.id,
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
            links=asset_links,
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


class InlineAssetSerializer(Serializer):
    @classmethod
    def db_to_stac(
        cls,
        db_model: database.ElasticsearchAsset,
    ) -> Asset:

        return Asset(
            href=db_model.get_url(),
            roles=db_model.get_roles(),
        )

    @classmethod
    def stac_to_db(
        cls, stac_data: Asset, exclude_geometry=False
    ) -> database.ElasticsearchAsset:
        pass
