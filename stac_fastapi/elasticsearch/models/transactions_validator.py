from stac_fastapi.types import stac as stac_types
from elasticsearch import NotFoundError
from fastapi import HTTPException
from stac_pydantic import Item, Collection
from pydantic import ValidationError

from stac_fastapi.elasticsearch.models.database import (
    ElasticsearchCollection,
    ElasticsearchItem,
    ElasticsearchAsset)


class TransactionsValidator:

    @staticmethod
    def item_validator(item: stac_types.Item, collection_id: str) -> Item:

        try:
            ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise HTTPException(status_code=404, detail="collection not found")

        try:
            stac_item = Item(**item)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e)

        return stac_item

    @staticmethod
    def collection_validator(collection: stac_types.Collection) -> Collection:

        try:
            stac_collection = Collection(**collection)
        except ValidationError as e:
            raise HTTPException(status_code=422, detail=e)

        return stac_collection
