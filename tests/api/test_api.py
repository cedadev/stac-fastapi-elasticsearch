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


def test_search_for_collection(app_client):
    pass


def test_search_date_interval(app_client):
    """Check searching with a date interval"""
    
    params = {
        "datetime": "2013-12-01T00:00:00Z/2014-05-01T00:00:00Z"
    }

    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["context"]["returned"] == resp_json["context"]["matched"] == 1
    assert resp_json["features"][0]["properties"]["datetime"] == "2014-04-09T00:00:00Z"


def test_search_invalid_date(app_client):
    """Given an invalid date, we should get a 400"""
    
    params = {
        "datetime": "2020-XX-01/2020-10-30",
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 400


def test_datetime_non_interval(app_client):
    """Checking search with a single date."""
    alternate_formats = [
        "2008-01-31T00:00:00+00:00",
        "2008-01-31T00:00:00.00Z",
        "2008-01-31T00:00:00Z",
        "2008-01-31T00:00:00.00+00:00",
    ]
    for date in alternate_formats:
        params = {
            "datetime": date,
        }
        resp = app_client.post("/search", json=params)
        assert resp.status_code == 200

        resp_json = resp.json()
        assert resp_json["context"]["returned"] == resp_json["context"]["matched"] == 1
        assert resp_json["features"][0]["properties"]["datetime"][0:19] == date[0:19]


def test_search_point_intersects(app_client):
    """Check that items intersect with the given point.
    As our test items don't contain a bounding bbox, this will return 0 results.
    """

    point = [150.04, -33.14]
    intersects = {"type": "Point", "coordinates": point}

    params = {
        "intersects": intersects,
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


def test_bbox_3d(app_client):
    """Test 3d bbox"""
    australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
    params = {
        "bbox": australia_bbox,
    }
    
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1


def test_search_line_string_intersects(app_client):
    """Test linstring intersect. Test data doesn't have a bbox so will fail."""
    line = [[150.04, -33.14], [150.22, -33.89]]
    intersects = {"type": "LineString", "coordinates": line}

    params = {
        "intersects": intersects,
    }
    resp = app_client.post("/search", json=params)
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["features"]) == 1