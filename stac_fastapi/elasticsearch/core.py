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
from elasticsearch_dsl.search import Search
from elasticsearch_dsl.query import QueryString

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

        items = self.get_queryset(**request_dict)
        result_count = items.count()

        response = []
        base_url = str(kwargs['request'].base_url)

        items = items.execute()

        for item in items:
            response_item = serializers.ItemSerializer.db_to_stac(item, base_url)
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
                getattr(search_request, 'page')
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

    def get_queryset(self, **kwargs) -> Search:

        # base_search = BaseSearch(**kwargs)

        qs = self.item_table.search()

        if collections := kwargs.get('collections'):
            qs = qs.filter('terms', collection_id__keyword=collections)

        if items := kwargs.get('ids'):
            qs = qs.filter('terms', item_id__keyword=items)

        if bbox := kwargs.get('bbox'):
            qs = qs.filter('geo_shape', spatial__bbox={
                'shape': {
                    'type': 'envelope',
                    'coordinates': Coordinates.from_wgs84(bbox).to_geojson()
                }
            })

        if kwargs.get('datetime'):
            if start_date := kwargs.get('start_date'):
                qs = qs.filter('range', properties__datetime={'gte': start_date})

            if end_date := kwargs.get('end_date'):
                qs = qs.filter('range', properties__datetime={'lte': end_date})

        if limit := kwargs.get('limit'):
            if limit > 10000:
                raise (
                    HTTPException(
                        status_code=424,
                        detail="The number of results requested is outside the maximum window 10,000")
                )
            qs = qs.extra(size=limit)

        if page := int(kwargs.get('page')):
            qs = qs[(page - 1) * limit:page * limit]

        if self.extension_is_enabled('FilterExtension'):

            field_mapping = {
                'datetime': 'properties.datetime',
                'bbox': 'spatial.bbox.coordinates'
            }

            if qfilter := kwargs.get('filter'):
                ast = parse_json(qfilter)
                qfilter = to_filter(
                    ast,
                    field_mapping,
                    field_default=Template('properties__${name}__keyword')
                )
                qs = qs.query(qfilter)

        if self.extension_is_enabled('FreeTextExtension'):
            if q := kwargs.get('q'):
                qs = qs.query(
                    QueryString(
                        query=q,
                        default_field='properties.*',
                        lenient=True
                    )
                )

        if self.extension_is_enabled('ContextCollectionExtension'):
            if not collections:
                qs.aggs.bucket('collections', 'terms', field='collection_id.keyword')

        return qs

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
            'collections': collections,
            'ids': ids,
            'bbox': bbox,
            'datetime': datetime,
            'limit': limit,
            **kwargs
        }

        items = self.get_queryset(**search)
        result_count = items.count()

        response = []
        base_url = str(kwargs['request'].base_url)

        items = items.execute()

        for item in items:
            response_item = serializers.ItemSerializer.db_to_stac(item, base_url)
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
            context = generate_context(limit, result_count, kwargs['page'])
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

    def get_item(self, itemId: str, collectionId: str, **kwargs) -> stac_types.Item:
        """Get item by id.

        Called with `GET /collections/{collectionId}/items/{itemId}`.

        Args:
            id: Id of the item.

        Returns:
            Item.
        """
        try:
            item = self.item_table.get(id=itemId)
        except NotFoundError:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f'Item: {itemId} from collection: {collectionId} not found'
                )
            )

        if not getattr(item, 'collection_id', None) == collectionId:
            raise (
                HTTPException(
                    status_code=404,
                    detail=f'Item: {itemId} from collection: {collectionId} not found'
                )
            )

        base_url = str(kwargs['request'].base_url)

        return serializers.ItemSerializer.db_to_stac(item, base_url)

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
                        'rel': 'http://www.opengis.net/def/rel/ogc/1.0/queryables',
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

    def get_collection(self, collectionId: str, **kwargs) -> stac_types.Collection:
        """Get collection by id.

        Called with `GET /collections/{collectionId}`.

        Args:
            id: Id of the collection.

        Returns:
            Collection.
        """
        try:
            collection = self.collection_table.get(id=collectionId)
        except NotFoundError:
            raise (NotFoundError(404, f'Collection: {collectionId} not found'))

        base_url = str(kwargs['request'].base_url)
        collection.base_url = base_url

        collection = serializers.CollectionSerializer.db_to_stac(collection, base_url)

        if self.extension_is_enabled('FilterExtension'):
            collection['links'].append(
                {
                    "rel": "http://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, f"collections/{collection.get('id')}/queryables")
                }
            )

        return collection

    def item_collection(
            self, collectionId: str, limit: int = 10, **kwargs
    ) -> stac_types.ItemCollection:
        """Get all items from a specific collection.

        Called with `GET /collections/{collectionId}/items`

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

        items = self.item_table.search().filter('term', collection_id__keyword=collectionId)
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
