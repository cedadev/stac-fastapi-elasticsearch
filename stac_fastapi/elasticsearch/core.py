# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

# Python imports
from datetime import datetime
import logging
from string import Template

# Package imports
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.elasticsearch.models import database
from stac_fastapi.elasticsearch.models import serializers
from stac_fastapi.elasticsearch.models.utils import Coordinates
from stac_fastapi.elasticsearch.pagination import generate_pagination_links
from stac_fastapi.elasticsearch.context import generate_context

# Stac FastAPI imports
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.search import BaseSearchPostRequest
from stac_fastapi.types import stac as stac_types

# Stac pydantic imports
from stac_pydantic.shared import MimeTypes
from stac_pydantic.links import Relations

# CQL Filters imports
from pygeofilter.parsers.cql_json import parse as parse_json
from pygeofilter_elasticsearch import to_filter

# Third-party imports
import attr
from elasticsearch import NotFoundError
from urllib.parse import urljoin
from fastapi import HTTPException

# Typing imports
from typing import Type, Dict, Optional, List, Union
from .utils import get_queryset


logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreCrudClient(BaseCoreClient):
    """
    Client for the core endpoints defined by STAC
    """

    session: Session = attr.ib(default=None)
    item_table: Type[database.ElasticsearchItem] = attr.ib(default=database.ElasticsearchItem)
    collection_table: Type[database.ElasticsearchCollection] = attr.ib(default=database.ElasticsearchCollection)

    def conformance(self, **kwargs) -> stac_types.Conformance:
        """Conformance classes.

        Called with `GET /conformance`.

        Returns:
            Conformance classes which the server conforms to.
        """

        return stac_types.Conformance(
            conformsTo=self.list_conformance_classes()
        )

    def post_search(self, search_request: Type[BaseSearchPostRequest], **kwargs) -> stac_types.ItemCollection:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        request_dict = search_request.dict()

        request_dict["item_ids"] = request_dict.pop("ids")
        request_dict["collection_ids"] = request_dict.pop("collections")

        items = self.get_queryset(self, **request_dict)
        result_count = items.count()

        response = []
        base_url = str(kwargs['request'].base_url)

        items = items.execute()

        item_serializer = serializers.ItemAssetSearchSerializer if self.extension_is_enabled('AssetSearchExtension') else serializers.ItemSerializer
        for item in items:
            response_item = item_serializer.db_to_stac(item, base_url)
            response.append(response_item)

        item_collection = stac_types.ItemCollection(
            type='FeatureCollection',
            features=response,
            links=generate_pagination_links(kwargs['request'], result_count, search_request.limit)

        )

        # Modify response with extensions
        if self.extension_is_enabled('ContextExtension'):
            context = generate_context(
                search_request.limit,
                result_count,
                int(getattr(search_request, 'page'))
            )
            item_collection['context'] = context

        if self.extension_is_enabled('ContextCollectionExtension'):
            context = item_collection.get('context', {})

            if request_dict.get('collections'):
                context['collections'] = request_dict['collections']
            else:
                context['collections'] = [c.key for c in items.aggregations.collections]

            if context:
                item_collection['context'] = context

        return item_collection

    def get_search(
            self,
            collections: Optional[List[str]] = None,
            ids: Optional[List[str]] = None,
            bbox: Optional[List[NumType]] = None,
            datetime: Optional[Union[str, datetime]] = None,
            limit: Optional[int] = 10,
            **kwargs
    ) -> stac_types.ItemCollection:
        """Cross catalog item search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """

        search = {
            'collection_ids': collections,
            'item_ids': ids,
            'bbox': bbox,
            'datetime': datetime,
            'limit': limit,
            **kwargs
        }

        items = get_queryset(self, self.item_table, **search)
        result_count = items.count()

        response = []
        base_url = str(kwargs['request'].base_url)

        items = items.execute()
        # hits = items.hits.hits

        item_serializer = serializers.ItemAssetSearchSerializer if self.extension_is_enabled('AssetSearchExtension') else serializers.ItemSerializer

        for item in items:
            # item = database.ElasticsearchItem.from_es(hit.to_dict())
            response_item = item_serializer.db_to_stac(item, base_url)
            response.append(response_item)

        links = generate_pagination_links(kwargs['request'], result_count, limit)

        # Create base response
        item_collection = stac_types.ItemCollection(
            type='FeatureCollection',
            features=response,
            links=links,
        )

        # Modify response with extensions
        if self.extension_is_enabled('ContextExtension'):
            context = generate_context(limit, result_count, kwargs.get('page', 1))
            item_collection['context'] = context

        if self.extension_is_enabled('ContextCollectionExtension'):
            context = item_collection.get('context', {})

            # Short circuit if there collections specified
            if collections:
                context['collections'] = collections
            else:
                context['collections'] = [c.key for c in items.aggregations.collections]

            if context:
                item_collection['context'] = context

        return item_collection

    def get_item(self, item_id: str, collection_id: str, **kwargs) -> stac_types.Item:
        """Get item by id.

        Called with `GET /collections/{collection_id}/items/{item_id}`.

        Args:
            id: Id of the item.

        Returns:
            Item.
        """
        try:
            item = self.item_table.get(id=item_id)
        except NotFoundError:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f'Item: {item_id} from collection: {collection_id} not found'
                )
            )

        if not getattr(item, 'collection_id', None) == collection_id:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f'Item: {item_id} from collection: {collection_id} not found'
                )
            )

        base_url = str(kwargs['request'].base_url)

        item_serializer = serializers.ItemAssetSearchSerializer if self.extension_is_enabled('AssetSearchExtension') else serializers.ItemSerializer

        return item_serializer.db_to_stac(item, base_url)

    def all_collections(self, **kwargs) -> Dict:
        """Get all available collections.

        Called with `GET /collections`.

        Returns:
            A list of collections.
        """
        query_params = dict(kwargs['request'].query_params)

        collections = self.collection_table.search()

        response = []

        base_url = str(kwargs['request'].base_url)

        for collection in collections:
            collection.base_url = base_url

            coll_response = serializers.CollectionSerializer.db_to_stac(
                collection, base_url
            )

            if self.extension_is_enabled('FilterExtension'):
                coll_response['links'].extend([
                    {
                        'rel': 'https://www.opengis.net/def/rel/ogc/1.0/queryables',
                        'type': MimeTypes.json,
                        'href': urljoin(base_url, f"collections/{coll_response.get('id')}/queryables")
                    }
                ])

            response.append(coll_response)

        links = [
            {
                'rel': Relations.root,
                'type': MimeTypes.json,
                'href': base_url
            },
            {
                'rel': Relations.self,
                'type': MimeTypes.json,
                'href': urljoin(base_url, 'collections')
            }
        ]

        return {
            'collections': response,
            'links': links,
        }

    def get_collection(self, collection_id: str, **kwargs) -> stac_types.Collection:
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
            raise (NotFoundError(404, f'Collection: {collection_id} not found'))

        base_url = str(kwargs['request'].base_url)
        collection.base_url = base_url

        collection = serializers.CollectionSerializer.db_to_stac(collection, base_url)

        if self.extension_is_enabled('FilterExtension'):
            collection['links'].append(
                {
                    "rel": "https://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, f"collections/{collection.get('id')}/queryables")
                }
            )

        return collection

    def item_collection(
            self, collection_id: str, limit: int = 10, **kwargs
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
        query_params = dict(kwargs['request'].query_params)
        page = int(query_params.get('page', '1'))
        limit = int(query_params.get('limit', '10'))

        items = self.item_table.search().filter('term', collection_id__keyword=collection_id)
        result_count = items.count()

        items = items[(page - 1) * limit:page * limit]

        # TODO: support filter parameter https://portal.ogc.org/files/96288#filter-param

        response = []

        base_url = str(kwargs['request'].base_url)

        for item in items:
            response.append(
                serializers.ItemSerializer.db_to_stac(
                    item, base_url
                )
            )

        # Generate the base response
        item_collection = stac_types.ItemCollection(
            type='FeatureCollection',
            features=response,
            links=generate_pagination_links(kwargs['request'], result_count, limit)
        )

        # Modify response with extensions
        if self.extension_is_enabled('ContextExtension'):
            context = generate_context(limit, result_count, page)
            item_collection['context'] = context

        return item_collection
