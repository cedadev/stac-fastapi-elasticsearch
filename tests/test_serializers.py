from stac_fastapi.elasticsearch.models.serializers import (ItemSerializer,
                                                           CollectionSerializer,
                                                           AssetSerializer)
from tests.stact_test_data import test_item, test_collection
from stac_fastapi.elasticsearch.models.database import (ElasticsearchCollection,
                                                        ElasticsearchItem,
                                                        ElasticsearchAsset)

import pytest


def test_item_serializer():
    item = ItemSerializer.stac_to_db(test_item)
    assert isinstance(item, ElasticsearchItem)


def test_collection_serializer():
    collection = CollectionSerializer.stac_to_db(test_collection)
    assert isinstance(collection, ElasticsearchCollection)


def test_asset_serializer():
    test_asset = test_item['assets']['test_asset_1']
    asset = AssetSerializer.stac_to_db(stac_data=test_asset,
                                       id='test_asset_1',
                                       collection_id='test_item')
    assert isinstance(asset, ElasticsearchAsset)
