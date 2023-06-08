# encoding: utf-8
"""

"""
__author__ = "Rhys Evans"
__date__ = "28 Jan 2022"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "rhys.r.evans@stfc.ac.uk"

import logging

# Python imports
from datetime import datetime

# Typing imports
from typing import List, Optional, Type, Union

# Third-party imports
import attr
from elasticsearch import NotFoundError
from fastapi import HTTPException

# Stac FastAPI asset search imports
from stac_fastapi_asset_search.client import BaseAssetSearchClient
from stac_fastapi_asset_search.types import (
    Asset,
    AssetCollection,
    AssetSearchPostRequest,
)

from stac_fastapi.elasticsearch.context import generate_context
from stac_fastapi.elasticsearch.models import serializers

# Package imports
from stac_fastapi.elasticsearch.models.database import ElasticsearchAsset
from stac_fastapi.elasticsearch.pagination import generate_pagination_links

from .utils import get_queryset

# Stac FastAPI imports


logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class AssetSearchClient(BaseAssetSearchClient):

    asset_table: Type[ElasticsearchAsset] = attr.ib(default=ElasticsearchAsset)

    def post_asset_search(
        self, search_request: Type[AssetSearchPostRequest], **kwargs
    ) -> AssetCollection:
        """Cross catalog asset search (POST).

        Called with `POST /asset/search`.

        Args:
            search_request: search request parameters.

        Returns:
            AssetCollection containing assets which match the search criteria.
        """
        request_dict = search_request.dict()
        if "items" in request_dict.keys():
            request_dict["item_ids"] = request_dict.pop("items")

        if "ids" in request_dict.keys():
            request_dict["asset_ids"] = request_dict.pop("ids")

        assets = get_queryset(self, self.asset_table, **request_dict)
        result_count = assets.count()

        response = []
        base_url = str(kwargs["request"].base_url)

        for asset in assets.execute():
            response_asset = serializers.AssetSerializer.db_to_stac(
                asset, base_url, getattr(kwargs["request"], "collection", None)
            )
            response.append(response_asset)

        asset_collection = AssetCollection(
            type="FeatureCollection",
            features=response,
            links=generate_pagination_links(
                kwargs["request"], result_count, search_request.limit
            ),
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            context = generate_context(
                search_request.limit, result_count, getattr(search_request, "page")
            )
            asset_collection["context"] = context

        return asset_collection

    def get_asset_search(
        self,
        ids: Optional[List[str]] = None,
        items: Optional[List[str]] = None,
        collection: Optional[str] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        role: Optional[List[str]] = None,
        limit: Optional[int] = 10,
        **kwargs,
    ) -> AssetCollection:
        """Cross catalog asset search (GET).

        Called with `GET /asset/search`.

        Returns:
            AssetCollection containing assets which match the search criteria.
        """
        search = {
            "asset_ids": ids,
            "item_ids": items,
            "bbox": bbox,
            "datetime": datetime,
            "role": role,
            "limit": limit,
            **kwargs,
        }

        if "filter-lang" not in search.keys():
            search["filter-lang"] = "cql-text"

        assets = get_queryset(self, self.asset_table, **search)
        result_count = assets.count()

        response = []
        base_url = str(kwargs["request"].base_url)

        for asset in assets.execute():
            response_asset = serializers.AssetSerializer.db_to_stac(
                asset, base_url, collection
            )
            response.append(response_asset)

        links = generate_pagination_links(kwargs["request"], result_count, limit)

        # Create base response
        asset_collection = AssetCollection(
            type="FeatureCollection",
            features=response,
            links=links,
        )

        # Modify response with extensions
        # if self.extension_is_enabled('ContextExtension'):
        #     context = generate_context(limit, result_count, kwargs.get('page', 1))
        #     asset_collection['context'] = context

        return asset_collection

    def get_assets(
        self, item_id: str = None, collection_id: str = None, **kwargs
    ) -> AssetCollection:
        """Get item assets (GET).

        Called with `GET /collection/{collection_id}/items/{item_id}/assets`.

        Returns:
            AssetCollection containing the item's assets.
        """

        return self.get_asset_search(
            items=[item_id], collection=collection_id, **kwargs
        )

    def get_asset(
        self, collection_id: str, item_id: str, asset_id: str, **kwargs
    ) -> Asset:
        """Get asset by id.

        Called with `GET /collections/{collection_id}/items/{item_id}/assets/{asset_id}`.

        Args:
            asset_id: Id of the asset.
            item_id: Id of the asset's item.
            collection_id: Id of the asset's item's collection.

        Returns:
            Asset.
        """
        try:
            asset = self.asset_table.get(id=asset_id)
        except NotFoundError:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f"Asset: {asset_id} from Item: {item_id} not found",
                )
            )

        if not getattr(asset, "item_id", None) == item_id:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f"Asset: {asset_id} from Item: {item_id} not found",
                )
            )

        base_url = str(kwargs["request"].base_url)

        return serializers.AssetSerializer.db_to_stac(asset, base_url, collection_id)
