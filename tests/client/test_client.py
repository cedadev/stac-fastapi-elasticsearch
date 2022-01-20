# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import pytest
from elasticsearch import NotFoundError

def test_get_collection(app_client):
    coll_id = "Fj3reHsBhuk7QqVbt7P-"
    resp = app_client.get(f"/collections/{coll_id}")
    resp_json = resp.json()
    assert resp_json["id"] == coll_id


def test_get_collection_does_not_exist(app_client):
    coll_id = "Fj3reHsriut7QqVb34f*"
    with pytest.raises(NotFoundError):
        app_client.get(f"/collections/{coll_id}")
 

def test_get_item(app_client):
    coll_id = "Fj3reHsBhuk7QqVbt7P-"
    item_id = "8c462277a5877d4adc00642e2a78af6e"

    resp = app_client.get(f"/collections/{coll_id}/items/{item_id}")
    resp_json = resp.json()
    assert resp_json['collection'] == coll_id
    assert resp_json["id"] == item_id


def test_get_collection_items(app_client):
    coll_id = "Fj3reHsBhuk7QqVbt7P-"
    resp = app_client.get(f"/collections/{coll_id}/items")
    resp_json = resp.json()

    assert resp_json['context']["returned"] ==  resp_json["context"]["matched"] == 2

    for item in resp_json['features']:
        assert item["collection"] == coll_id