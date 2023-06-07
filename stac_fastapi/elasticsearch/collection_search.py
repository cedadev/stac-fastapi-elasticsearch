# encoding: utf-8
"""

"""
__author__ = "Rhys Evans"
__date__ = "10 May 2023"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "rhys.r.evans@stfc.ac.uk"

import logging

# Python imports
from datetime import datetime as datetime_type

# Typing imports
from typing import List, Optional, Type, Union

# Third-party imports
import attr
from fastapi import HTTPException
from stac_fastapi.elasticsearch.context import generate_context
from stac_fastapi.elasticsearch.models.database import ElasticsearchCollection

# Package imports
from stac_fastapi.elasticsearch.models.serializers import CollectionSerializer
from stac_fastapi.elasticsearch.pagination import generate_pagination_links
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types.stac import Collections

# Stac FastAPI asset search imports
from stac_fastapi_collection_search.client import BaseCollectionSearchClient

from elasticsearch import NotFoundError

from .utils import get_queryset

# Stac FastAPI imports


logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CollectionSearchClient(BaseCollectionSearchClient):

    collection_table: Type[ElasticsearchCollection] = attr.ib(
        default=ElasticsearchCollection
    )

    def post_collection_search(
        self, search_request: Type[BaseSearchPostRequest], **kwargs
    ) -> Collections:
        """Cross catalog asset search (POST).

        Called with `POST /collection`.

        Args:
            search_request: search request parameters.

        Returns:
            Collections containing assets which match the search criteria.
        """
        request_dict = search_request.dict()
        if "ids" in request_dict.keys():
            request_dict["ids"] = request_dict.pop("ids")

        collections = get_queryset(self, self.asset_table, **request_dict)
        result_count = collections.count()

        response = []
        request = kwargs["request"]

        for collection in collections.execute():
            response.append(CollectionSerializer.db_to_stac(collection, request))

        collections = Collections(
            features=collections,
            links=generate_pagination_links(
                request, result_count, search_request.limit
            ),
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            context = generate_context(
                search_request.limit, result_count, getattr(search_request, "page")
            )
            collections["context"] = context

        return collections

    def get_collection_search(
        self,
        ids: Optional[List[str]] = None,
        bbox: Optional[List[NumType]] = None,
        datetime: Optional[Union[str, datetime_type]] = None,
        limit: Optional[int] = 10,
        **kwargs,
    ) -> Collections:
        """Cross catalog asset search (GET).

        Called with `GET /asset/search`.

        Returns:
            AssetCollection containing assets which match the search criteria.
        """
        search = {
            "ids": ids,
            "bbox": bbox,
            "datetime": datetime,
            "limit": limit,
            **kwargs,
        }

        if "filter-lang" not in search.keys():
            search["filter-lang"] = "cql-text"

        collections = get_queryset(self, self.collection_table, **search)
        result_count = collections.count()

        response = []
        request = kwargs["request"]

        for collection in collections.execute():
            response_asset = CollectionSerializer.db_to_stac(collection, request)
            response.append(response_asset)

        links = generate_pagination_links(request, result_count, limit)

        # Create base response
        collections = Collections(
            collections=response,
            links=links,
        )

        # Modify response with extensions
        if self.extension_is_enabled("ContextExtension"):
            context = generate_context(limit, result_count, kwargs.get("page", 1))
            collections["context"] = context

        return collections
