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
    "GET /collections/{collectionId}",
    "GET /collections/{collectionId}/items",
    "GET /collections/{collectionId}/items/{itemId}",
    "GET /conformance",
    "GET /search",
    "POST /search",
]

FILTER_EXTENSION_ROUTES = [
    "GET /queryables",
    "GET /collections/{collectionId}/queryables"
]


def test_core_router(api_client):
    """Test API serves all the core routes."""
    core_routes = set(STAC_CORE_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )

    assert not core_routes - api_routes


def test_filter_extension_router(api_client):
    """Test API serves all the routes for the filter extension."""
    filter_routes = set(FILTER_EXTENSION_ROUTES)
    api_routes = set(
        [f"{list(route.methods)[0]} {route.path}" for route in api_client.app.routes]
    )

    assert not filter_routes - api_routes
