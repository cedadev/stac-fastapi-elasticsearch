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
from stac_fastapi.elasticsearch.models import schemas
from stac_fastapi.elasticsearch.models.utils import Coordinates
from stac_fastapi.elasticsearch.types import BaseSearch

# Stac FastAPI imports
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.search import STACSearch

# Stac pydantic imports
from stac_pydantic.api import ConformanceClasses
from stac_pydantic import ItemCollection
from stac_pydantic.links import Link
from stac_pydantic.shared import MimeTypes

# CQL Filters imports
from pygeofilter.parsers.cql_json import parse as parse_json
from pygeofilter_elasticsearch import to_filter

# Third-party imports
import attr
from elasticsearch import NotFoundError
from urllib.parse import urljoin
from fastapi import HTTPException
from pydantic.error_wrappers import ValidationError

# Typing imports
from typing import Type, Dict, Any, Optional, List, Union
from elasticsearch_dsl.search import Search

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

    def conformance(self, **kwargs) -> ConformanceClasses:
        """Conformance classes.

        Called with `GET /conformance`.

        Returns:
            Conformance classes which the server conforms to.
        """

        return ConformanceClasses(
            conformsTo=self.list_conformance_classes()
        )

    def post_search(self, search_request: STACSearch, **kwargs) -> Dict[str, Any]:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """

        items = self.get_queryset(**search_request.dict())

        response = []
        base_url = str(kwargs['request'].base_url)

        for item in items:
            item.base_url = base_url
            response_item = schemas.Item.from_orm(item)
            response.append(response_item)

        return ItemCollection(
            type='FeatureCollection',
            features=response,
            links=[]
        )


    def get_queryset(self, **kwargs) -> Search:

        base_search = BaseSearch(**kwargs)

        qs = self.item_table.search()

        if collections := base_search.collections:
            qs = qs.filter('terms', collection_id__keyword=collections)

        if items := base_search.ids:
            qs = qs.filter('terms', item_id__keyword=items)

        if bbox := base_search.bbox:
            qs = qs.filter('geo_shape', spatial__bbox={
                'shape': {
                    'type': 'envelope',
                    'coordinates': Coordinates.from_wgs84(bbox).to_geojson()
                }
            })

        if base_search.datetime:
            if base_search.start_date:
                qs = qs.filter('range', properties__datetime={'gte': base_search.start_date})

            if base_search.end_date:
                qs = qs.filter('range', properties__datetime={'lte': base_search.end_date})

        if limit := kwargs.get('limit'):
            if limit > 10000:
                raise(
                    HTTPException(
                        status_code=424,
                        detail="The number of results requested is outside the maximum window 10,000")
                      )
            qs.extra(size=limit)

        if self.extension_is_enabled('FilterExtension'):

            field_mapping = {
                'datetime': 'properties.datetime',
                'bbox': 'spatial.bbox.coordinates'
            }

            if filter := getattr(base_search, 'filter', None):
                ast = parse_json(filter)
                filter = to_filter(ast, field_mapping, field_default=Template('properties__${name}__keyword'))

                qs = qs.query(filter)

        return qs

    def get_search(
            self,
            collections: Optional[List[str]] = None,
            ids: Optional[List[str]] = None,
            bbox: Optional[List[NumType]] = None,
            datetime: Optional[Union[str, datetime]] = None,
            limit: Optional[int] = 10,
            **kwargs
    ) -> ItemCollection:
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

        response = []
        base_url = str(kwargs['request'].base_url)

        for item in items:
            item.base_url = base_url
            try:
                response_item = schemas.Item.from_orm(item)
            except ValidationError:
                # Don't append if it does not validate as a stac
                # item.
                logger.warning(f'Ignoring {item.item_id}')
                continue
            response.append(response_item)

        return ItemCollection(
            type='FeatureCollection',
            features=response,
            links=[]
        )

    def get_item(self, item_id: str, collection_id: str, **kwargs) -> schemas.Item:
        """Get item by id.

        Called with `GET /collections/{collectionId}/items/{itemId}`.

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

        item.base_url = str(kwargs['request'].base_url)

        try:
            from_orm = schemas.Item.from_orm(item)
        except ValidationError:
            raise(
                HTTPException(
                    status_code=404,
                    detail=f'Item: {item_id} from collection: {collection_id} does not'
                           f' validate and has been excluded.'
                )
            )
        return from_orm

    def all_collections(self, **kwargs) -> List[schemas.Collection]:
        """Get all available collections.

        Called with `GET /collections`.

        Returns:
            A list of collections.
        """
        # TODO: This only gets first 20, need pagination/scroll
        collections = self.collection_table.search()
        response = []

        base_url = str(kwargs['request'].base_url)

        for collection in collections:
            collection.base_url = base_url

            coll_response = schemas.Collection.from_orm(collection)

            if self.extension_is_enabled('FilterExtension'):
                coll_response.links.append(
                    Link(
                        rel="http://www.opengis.net/def/rel/ogc/1.0/queryables",
                        type=MimeTypes.json,
                        href=urljoin(base_url, f"collections/{coll_response.id}/queryables")
                    )
                )
            response.append(coll_response)

        return response

    def get_collection(self, id: str, **kwargs) -> schemas.Collection:
        """Get collection by id.

        Called with `GET /collections/{collectionId}`.

        Args:
            id: Id of the collection.

        Returns:
            Collection.
        """
        try:
            collection = self.collection_table.get(id=id)
        except NotFoundError:
            raise (NotFoundError(404, f'Collection: {id} not found'))

        base_url = str(kwargs['request'].base_url)
        collection.base_url = base_url

        collection = schemas.Collection.from_orm(collection)

        if self.extension_is_enabled('FilterExtension'):
            collection.links.append(
                Link(
                    rel="http://www.opengis.net/def/rel/ogc/1.0/queryables",
                    type=MimeTypes.json,
                    href=urljoin(base_url, f"collections/{collection.id}/queryables")
                )
            )

        return collection

    def item_collection(
            self, id: str, limit: int = 10, token: str = None, **kwargs
    ) -> ItemCollection:
        """Get all items from a specific collection.

        Called with `GET /collections/{collectionId}/items`

        Args:
            id: id of the collection.
            limit: number of items to return.
            token: pagination token.

        Returns:
            An ItemCollection.
        """
        # TODO: This only gets first 20, need pagination/scroll
        items = self.item_table.search().filter('term', collection_id__keyword=id)

        # TODO: support filter parameter https://portal.ogc.org/files/96288#filter-param

        response = []

        for item in items:
            item.base_url = str(kwargs['request'].base_url)
            try:
                from_orm = schemas.Item.from_orm(item)
            except ValidationError:
                # Do not append if it does not validate as a STAC
                # item
                logger.warning(f'Ignoring {item.item_id}')
                continue
            response.append(from_orm)

        return ItemCollection(
            type='FeatureCollection',
            features=response,
            links=[]
        )
