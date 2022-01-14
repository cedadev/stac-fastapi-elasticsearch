# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '15 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


STAC_CORE_ROUTES = [
    "GET /",
    "GET /collections",
    "GET /collections/{collection_id}",
    "GET /collections/{collection_id}/items",
    "GET /collections/{collection_id}/items/{item_id}",
    "GET /conformance",
    "GET /search",
    "POST /search",
]

FILTER_EXTENSION_ROUTES = [
    "GET /queryables",
    "GET /collections/{collection_id}/queryables"
]


def test_core_router(api_client):
    """Test API serves all the core routes."""
    core_routes = set(STAC_CORE_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )
    print(api_routes)

    assert not core_routes - api_routes


def test_filter_extension_router(api_client):
    """Test API serves all the routes for the filter extension."""
    filter_routes = set(FILTER_EXTENSION_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )

    assert not filter_routes - api_routes


def test_app_search_response(app_client):
    """Check application returns a FeatureCollection"""

    resp = app_client.get("/search")
    assert resp.status_code == 200
    resp_json = resp.json()

    assert resp_json.get("type") == "FeatureCollection"


def test_app_context_extension(app_client):
    """Check context extension returns the correct number of results"""

    resp = app_client.get("/search")
    assert resp.status_code == 200
    resp_json = resp.json()

    assert "context" in resp_json
    assert resp_json["context"]["returned"] ==  resp_json["context"]["matched"] == 2