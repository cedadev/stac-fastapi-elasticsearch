from fastapi.testclient import TestClient

from stac_fastapi.elasticsearch.app import app
from tests.stact_test_data import test_item, test_collection

import pytest
pytest.skip(allow_module_level=True)

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


def test_update_collection():
    url = f"/collections/{test_collection['id']}"
    r = client.put(url, json=test_collection)
    assert r.status_code == 200


def test_update_item():
    url = f"/collections/{test_collection['id']}/items/{test_item['id']}"
    r = client.put(url, json=test_item)
    assert r.status_code == 200


def test_delete_item():
    url = f"/collections/{test_collection['id']}/items/{test_item['id']}"
    r = client.delete(url)
    assert r.status_code == 200


def test_delete_collection():
    url = f"/collections/{test_collection['id']}"
    r = client.delete(url)
    assert r.status_code == 200



