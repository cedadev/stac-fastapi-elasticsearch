from fastapi import FastAPI
from fastapi.testclient import TestClient

from stac_fastapi.elasticsearch.app import app
from tests.test_transaction.stact_test_data import test_item, test_collection
from stac_fastapi.elasticsearch.models.database import ElasticsearchCollection, ElasticsearchItem, ElasticsearchAsset
from stac_fastapi.elasticsearch.settings import ELASTICSEARCH_CONNECTION, COLLECTION_INDEX, ITEM_INDEX, ASSET_INDEX

import pytest

client = TestClient(app)


def test_app():
    r = client.get('/')
    assert r.status_code == 200


def test_create_collection():
    url = f"/collections"
    r = client.post(url, json=test_collection)
    assert r.status_code == 200


def test_create_item():
    url = f"/collections/{test_collection['id']}/items"
    r = client.post(url, json=test_item)
    assert r.status_code == 200


def test_delete_item():
    url = f"/collections/{test_collection['id']}/items/{test_item['id']}"
    r = client.delete(url)
    assert r.status_code == 200


def test_delete_collection():
    url = f"/collections/{test_collection['id']}"
    r = client.delete(url)
    assert r.status_code == 200
