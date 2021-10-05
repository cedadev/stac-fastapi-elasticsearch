# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from typing import Dict
from urllib.error import HTTPError

from elasticsearch import NotFoundError
from fastapi import Request
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.core import BaseTransactionsClient

from stac_fastapi.elasticsearch.models.database import (
    ElasticsearchCollection,
    ElasticsearchItem,
    ElasticsearchAsset)
from stac_fastapi.elasticsearch.models.serializers import (
    ItemSerializer,
    CollectionSerializer,
    AssetSerializer)
from stac_fastapi.elasticsearch.models.prevalidation_types import ItemValidator, CollectionValidator


class TransactionsClient(BaseTransactionsClient):
    """Defines a pattern for implementing the STAC transaction extension."""

    def create_item(self, collection_id: str, item: stac_types.Item, request: Request) -> stac_types.Item:
        """Create a new item.

        Called with `POST /collections/{collection_id}/items`.

        Args:
            collection_id: ID of the collection to add item to.
            item: the item
            request: starlette request object

        Returns:
            The item that was created.

        """
        base_url = f'{request.url.scheme}://{request.url.netloc}'

        try:
            ItemValidator(**item)
        except ValueError as error:
            raise HTTPError(url=f"{request.url}",
                            code=400, msg=error, hdrs=None, fp=None)

        try:
            ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise NotFoundError(404, f'Collection: {collection_id} not found')

        db_item = ItemSerializer.stac_to_db(item)
        db_item.save()
        if assets := item.get('assets'):
            for asset_id, asset in assets.items():
                self.create_asset({asset_id: asset}, db_item.meta.id)
        item = ElasticsearchItem.get(id=db_item.meta.id)
        return ItemSerializer.db_to_stac(item, base_url=base_url)

    def update_item(self, collection_id: str,
                    item_id: str, item: stac_types.Item, request: Request
                    ) -> stac_types.Item:
        """Perform a complete update on an existing item.

        Called with `PUT /collections/{collection_id}/items/{item_id}`. It is expected that this item already exists.
        The update should do a diff against the saved item and perform any necessary updates.
        Partial updates are not supported by the transactions extension.

        Args:
            collection_id: the ID of the item's collection
            item_id: the ID of the item
            item: the item (must be complete)
            request: starlette request object

        Returns:
            The updated item.
        """
        base_url = f'{request.url.scheme}://{request.url.netloc}'

        try:
            ItemValidator(**item)
        except ValueError as error:
            raise HTTPError(url=f"{request.url}",
                            code=400, msg=error, hdrs=None, fp=None)

        try:
            ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise NotFoundError(404, f'Collection: {collection_id} not found')
        try:
            item_db = ElasticsearchItem.get(id=item_id)
        except NotFoundError:
            raise NotFoundError(404, f'Item: {item_id} not found')

        if item.get('assets'):
            old_item = ItemSerializer.db_to_stac(item_db, base_url=base_url)
            for asset_id, asset in old_item.get('assets').items():
                self.delete_asset({asset_id: asset})

        for asset_id, asset in item.get('assets').items():
            self.create_asset({asset_id: asset}, item_db.meta.id)

        item = ItemSerializer.stac_to_db(item)
        item_db.update(**item.to_dict())
        item = ItemSerializer.db_to_stac(item, base_url=base_url)

        return item

    def delete_item(
            self, item_id: str, collection_id: str, request: Request
    ) -> stac_types.Item:
        """Delete an item from a collection.

        Called with `DELETE /collections/{collection_id}/items/{item_id}`

        Args:
            item_id: id of the item.
            collection_id: id of the collection.
            request: starlette request object.

        Returns:
            The deleted item.
        """
        base_url = f'{request.url.scheme}://{request.url.netloc}'

        try:
            ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise NotFoundError(404, f'collection: {collection_id} not found')

        try:
            item_db = ElasticsearchItem.get(id=item_id)
        except NotFoundError:
            raise NotFoundError(404, f'Item: {item_id} not found')

        item = ItemSerializer.db_to_stac(db_model=item_db, base_url=base_url)

        for asset_id, asset in item.get('assets').items():
            self.delete_asset({asset_id: asset})

        # delete item from elastic search item index
        item_db.delete()

        return item

    def create_collection(
            self, collection: stac_types.Collection, request: Request
    ) -> stac_types.Collection:
        """Create a new collection.

        Called with `POST /collections`.

        Args:
            collection: the collection
            request: starlette request object

        Returns:
            The collection that was created.
        """
        try:
            CollectionValidator(**collection)
        except ValueError as error:
            raise HTTPError(url=f"{request.url}",
                            code=400, msg=error, hdrs=None, fp=None)

        # serialise collection, stac to db
        db_collection = CollectionSerializer.stac_to_db(collection)

        # add collection to elasticsearch collection index
        db_collection.save()
        return collection

    def update_collection(
            self, collection_id: str, collection: stac_types.Collection, request: Request,
    ) -> stac_types.Collection:
        """Perform a complete update on an existing collection.

        Called with `PUT /collections/{collection_id}`. It is expected that this item already exists.
        The update should do a diff against the saved collection and perform any necessary updates.
        Partial updates are not supported by the transactions extension.

        Args:
            collection_id: id of the collection
            collection: the collection (must be complete)
            request: starlette request object

        Returns:
            The updated collection.
        """
        base_url = f'{request.url.scheme}://{request.url.netloc}'

        try:
            CollectionValidator(**collection)
        except ValueError as error:
            raise HTTPError(url=f"{request.url}",
                            code=400, msg=error, hdrs=None, fp=None)

        try:
            collection_db = ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise NotFoundError(404, f'Collection: {collection_id} not found')

        # serialise collection, stac to db
        collection = CollectionSerializer.stac_to_db(collection)

        # compare the two and update, or remove old_collection and add collection to index
        collection_db.update(**collection.to_dict())
        collection = CollectionSerializer.db_to_stac(collection, base_url=base_url)

        return collection

    def delete_collection(
            self, collection_id: str, request: Request
    ) -> stac_types.Collection:
        """Delete a collection.

        Called with `DELETE /collections/{collection_id}`

        Args:
            collection_id: id of the collection.
            request: starlette request object

        Returns:
            The deleted collection.
        """
        base_url = f'{request.url.scheme}://{request.url.netloc}'

        try:
            collection_db = ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise NotFoundError(404, f'collection: {collection_id} not found')

        collection = CollectionSerializer.db_to_stac(db_model=collection_db, base_url=base_url)

        # remove collection from elasticsearch index
        items = ElasticsearchItem.search()
        items = items.filter("term", collection_id__keyword=collection_db.meta.id)

        collection_db.delete()
        items.delete()

        return collection

    @staticmethod
    def create_asset(asset: Dict, item_id: str):
        for asset_id, data in asset.items():
            db_asset = AssetSerializer.stac_to_db(data)
            db_asset.meta.id = asset_id
            db_asset.collection_id = item_id
            db_asset.save()
        return db_asset

    @staticmethod
    def delete_asset(asset: Dict):
        for asset_id, data in asset.items():
            asset_db = ElasticsearchAsset.get(id=asset_id)
            asset_db.delete()
