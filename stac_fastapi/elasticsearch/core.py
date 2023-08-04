# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "11 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import json
import logging

# Python imports
from datetime import datetime

# Typing imports
from typing import List, Optional, Type, Union
from urllib.parse import urljoin

# Third-party imports
import attr
from elasticsearch import NotFoundError
from fastapi import HTTPException
from stac_fastapi.types import stac as stac_types

# Stac FastAPI imports
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_pydantic.links import Relations

# Stac pydantic imports
from stac_pydantic.shared import MimeTypes
from starlette.requests import Request as StarletteRequest

from stac_fastapi.elasticsearch.context import generate_context
from stac_fastapi.elasticsearch.models import database, serializers
from stac_fastapi.elasticsearch.pagination import generate_pagination_links

# Package imports
from stac_fastapi.elasticsearch.session import Session

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreCrudClient(BaseCoreClient):
    """
    Client for the core endpoints defined by STAC
    """

    session: Session = attr.ib(default=None)
    item_table: Type[database.ElasticsearchItem] = attr.ib(
        default=database.ElasticsearchItem
    )
    collection_table: Type[database.ElasticsearchCollection] = attr.ib(
        default=database.ElasticsearchCollection
    )
    item_serializer: Type[serializers.ItemSerializer] = attr.ib(
        default=serializers.ItemSerializer
    )

    def conformance(self, **kwargs) -> stac_types.Conformance:
        """Conformance classes.

        Called with `GET /conformance`.

        Returns:
            Conformance classes which the server conforms to.
        """

        return stac_types.Conformance(conformsTo=self.list_conformance_classes())

    def post_search(
        self,
        search_request: Type[BaseSearchPostRequest],
        request: StarletteRequest,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        request_dict = search_request.dict()

        # Be specific about the ids
        request_dict["item_ids"] = request_dict.pop("ids")
        request_dict["collection_ids"] = request_dict.pop("collections")

        items, count = self.item_table.search(
            catalog=request.get("root_path"), **request_dict
        )

        response = []

        for item in items:
            response.append(self.item_serializer.db_to_stac(item, request))

        item_collection = stac_types.ItemCollection(
            type="FeatureCollection",
            features=response,
            links=generate_pagination_links(request, count, search_request.limit),
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            context = generate_context(
                search_request.limit, count, getattr(search_request, "page", 1)
            )
            item_collection["context"] = context

        if self.extension_is_enabled("ContextCollectionExtension"):
            if (
                "context_collection" in request_dict
                and request_dict["context_collection"]
            ):
                context = item_collection.get("context", {})

                if request_dict.get("collection_ids"):
                    context["collections"] = request_dict["collection_ids"]
                else:
                    context["collections"] = [
                        c.key for c in items.aggregations.collections
                    ]

                if context:
                    item_collection["context"] = context

        return item_collection

    def get_search(
        self,
        request: StarletteRequest,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime]] = None,
        limit: Optional[int] = 10,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Cross catalog item search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """

        search = {
            "collection_ids": collections,
            "item_ids": ids,
            "bbox": bbox,
            "datetime": datetime,
            "limit": limit,
            **kwargs,
        }

        if "filter-lang" not in search.keys():
            search["filter-lang"] = "cql-text"

        items, count = self.item_table.search(
            catalog=request.get("root_path").strip("/"), **search
        )

        response = []

        for item in items:
            response.append(self.item_serializer.db_to_stac(item, request))

        links = generate_pagination_links(request, count, limit)

        # Create base response
        item_collection = stac_types.ItemCollection(
            type="FeatureCollection",
            features=response,
            links=links,
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            item_collection["context"] = generate_context(
                limit, count, kwargs.get("page", 1)
            )

        if self.extension_is_enabled("ContextCollectionExtension"):
            if "context_collection" in search and search["context_collection"]:
                context = item_collection.get("context", {})

                # Short circuit if there collections specified
                if collections:
                    context["collections"] = collections
                else:
                    context["collections"] = [
                        c.key for c in items.aggregations.collections
                    ]

                if context:
                    item_collection["context"] = context

        return item_collection

    def get_item(
        self, request: StarletteRequest, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Get item by id.

        Called with `GET /collections/{collection_id}/items/{item_id}`.

        Args:
            id: Id of the item.

        Returns:
            Item.
        """
        try:
            item = self.item_table.get(id=item_id)
        except NotFoundError as exc:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f"Item: {item_id} from collection: {collection_id} not found",
                )
            ) from exc

        if item.get_collection_id() != collection_id:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f"Item: {item_id} from collection: {collection_id} not found",
                )
            )

        return self.item_serializer.db_to_stac(item, request)

    def all_collections(self, request: StarletteRequest, **kwargs) -> dict:
        """Get all available collections.

        Called with `GET /collections`.

        Returns:
            A list of collections.
        """

        response = []

        collections, count = self.collection_table.search(
            catalog=request.get("root_path").strip("/")
        )

        for collection in collections:
            response.append(
                serializers.CollectionSerializer.db_to_stac(collection, request)
            )

        links = [
            {
                "rel": Relations.root,
                "type": MimeTypes.json,
                "href": str(request.base_url),
            },
            {
                "rel": Relations.self,
                "type": MimeTypes.json,
                "href": urljoin(str(request.base_url), "collections"),
            },
        ]

        return {
            "collections": response,
            "links": links,
        }

    def get_collection(
        self, request: StarletteRequest, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Get collection by id.

        Called with `GET /collections/{collection_id}`.

        Args:
            id: Id of the collection.

        Returns:
            Collection.
        """
        try:
            collection = self.collection_table.get(id=collection_id)
        except NotFoundError:
            raise (NotFoundError(404, f"Collection: {collection_id} not found"))

        collection = serializers.CollectionSerializer.db_to_stac(collection, request)

        if self.extension_is_enabled("FilterExtension"):
            collection["links"].append(
                {
                    "rel": "https://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": MimeTypes.json,
                    "href": urljoin(
                        str(request.base_url),
                        f"collections/{collection.get('id')}/queryables",
                    ),
                }
            )

        return collection

    def item_collection(
        self, request: StarletteRequest, collection_id: str, limit: int = 10, **kwargs
    ) -> stac_types.ItemCollection:
        """Get all items from a specific collection.

        Called with `GET /collections/{collection_id}/items`

        Args:
            id: id of the collection.
            limit: number of items to return.
            page: page number.

        Returns:
            An ItemCollection.
        """
        query_params = dict(request.query_params)
        page = int(query_params.get("page", "1"))
        limit = int(query_params.get("limit", "10"))

        items, count = self.item_table.search(
            catalog=request.get("root_path").strip("/"),
            collection=[collection_id],
            page=page,
            limit=limit,
        )

        # TODO: support filter parameter https://portal.ogc.org/files/96288#filter-param

        response = []

        for item in items:
            response.append(self.item_serializer.db_to_stac(item, request))

        # Generate the base response
        item_collection = stac_types.ItemCollection(
            type="FeatureCollection",
            features=response,
            links=generate_pagination_links(request, count, limit),
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            context = generate_context(limit, count, page)
            item_collection["context"] = context

        return item_collection
