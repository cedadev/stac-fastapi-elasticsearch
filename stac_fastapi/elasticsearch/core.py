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

# Package imports
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.elasticsearch.models import database
from stac_fastapi.elasticsearch.models import schemas

# Stac FastAPI imports
from stac_fastapi.types.core import BaseCoreClient
from stac_fastapi.types.search import STACSearch

# Stac pydantic imports
from stac_pydantic.api import ConformanceClasses
from stac_pydantic import ItemCollection

# Third-party imports
import attr
from elasticsearch import NotFoundError

# Typing imports
from typing import Type, Dict, Any, Optional, List, Union


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
            conformsTo=[
                "https://stacspec.org/STAC-api.html",
                "http://docs.opengeospatial.org/is/17-069r3/17-069r3.html#ats_geojson",
            ]
        )

    def post_search(self, search_request: STACSearch, **kwargs) -> Dict[str, Any]:
        """Cross catalog search (POST).

        Called with `POST /search`.

        Args:
            search_request: search request parameters.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        pass

    def get_search(
            self,
            collections: Optional[List[str]] = None,
            ids: Optional[List[str]] = None,
            bbox: Optional[List[NumType]] = None,
            datetime: Optional[Union[str, datetime]] = None,
            limit: Optional[int] = 10,
            query: Optional[str] = None,
            token: Optional[str] = None,
            fields: Optional[List[str]] = None,
            sortby: Optional[str] = None,
            **kwargs
    ) -> Dict[str, Any]:
        """Cross catalog search (GET).

        Called with `GET /search`.

        Returns:
            ItemCollection containing items which match the search criteria.
        """
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": json.loads(query) if query else query,
        }
        if datetime:
            base_args["datetime"] = datetime
        if sortby:
            # https://github.com/radiantearth/stac-spec/tree/master/api-spec/extensions/sort#http-get-or-post-form
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "asc" if sort[0] == "+" else "desc",
                    }
                )
            base_args["sortby"] = sort_param

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

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
            raise(NotFoundError(404, f'Item: {item_id} from collection: {collection_id} not found'))

        if not getattr(item, 'collection_id', None) == collection_id:
            raise(NotFoundError(404, f'Item: {item_id} from collection: {collection_id} not found'))

        item.base_url = str(kwargs['request'].base_url)
        return schemas.Item.from_orm(item)

    def all_collections(self, **kwargs) -> List[schemas.Collection]:
        """Get all available collections.

        Called with `GET /collections`.

        Returns:
            A list of collections.
        """
        #TODO: This only gets first 20, need pagination/scroll
        collections = self.collection_table.search().execute()
        response = []

        for collection in collections:
            collection.base_url = str(kwargs['request'].base_url)
            response.append(schemas.Collection.from_orm(collection))

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
        collection.base_url = str(kwargs['request'].base_url)

        return schemas.Collection.from_orm(collection)

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
        #TODO: This only gets first 20, need pagination/scroll
        items = self.item_table.search().filter('term', collection_id=id).execute()

        response = []

        for item in items:
            item.base_url = str(kwargs['request'].base_url)
            response.append(schemas.Item.from_orm(item))

        return ItemCollection(
            type='FeatureCollection',
            features=response,
            links=[]
        )

