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


# def test_search_invalid_date(load_test_data, app_client, postgres_transactions):
#     """Given an invalid date, we should get a 400"""
#     item = load_test_data("test_item.json")
#     postgres_transactions.create_item(item, request=MockStarletteRequest)
#
#     params = {
#         "datetime": "2020-XX-01/2020-10-30",
#         "collections": [item["collection"]],
#     }
#
#     resp = app_client.post("/search", json=params)
#     assert resp.status_code == 400
#
#

# def test_search_point_intersects(load_test_data, app_client, postgres_transactions):
#    """Check that items intersect with the given point.
#    As our test items don't contain a bounding bbox, this will return 0 results.
#    """

#     item = load_test_data("test_item.json")
#     postgres_transactions.create_item(item, request=MockStarletteRequest)
#
#     point = [150.04, -33.14]
#     intersects = {"type": "Point", "coordinates": point}
#
#     params = {
#         "intersects": intersects,
#         "collections": [item["collection"]],
#     }
#     resp = app_client.post("/search", json=params)
#     assert resp.status_code == 200
#     resp_json = resp.json()
#     assert len(resp_json["features"]) == 1
#
#
# def test_datetime_non_interval(load_test_data, app_client, postgres_transactions):
#       """Checking search with a single date. Need to provide an example datetime in test data"""
#     item = load_test_data("test_item.json")
#     postgres_transactions.create_item(item, request=MockStarletteRequest)
#     alternate_formats = [
#         "2020-02-12T12:30:22+00:00",
#         "2020-02-12T12:30:22.00Z",
#         "2020-02-12T12:30:22Z",
#         "2020-02-12T12:30:22.00+00:00",
#     ]
#     for date in alternate_formats:
#         params = {
#             "datetime": date,
#             "collections": [item["collection"]],
#         }
#
#         resp = app_client.post("/search", json=params)
#         assert resp.status_code == 200
#         resp_json = resp.json()
#         # datetime is returned in this format "2020-02-12T12:30:22+00:00"
#         assert resp_json["features"][0]["properties"]["datetime"][0:19] == date[0:19]
#
#
# def test_bbox_3d(load_test_data, app_client, postgres_transactions):
#     """TEst 3d bbox. Test data doesn't have a bbox so will fail."""
#     item = load_test_data("test_item.json")
#     postgres_transactions.create_item(item, request=MockStarletteRequest)
#
#     australia_bbox = [106.343365, -47.199523, 0.1, 168.218365, -19.437288, 0.1]
#     params = {
#         "bbox": australia_bbox,
#         "collections": [item["collection"]],
#     }
#     resp = app_client.post("/search", json=params)
#     assert resp.status_code == 200
#     resp_json = resp.json()
#     assert len(resp_json["features"]) == 1
#
#
#
# def test_search_line_string_intersects(
#     load_test_data, app_client, postgres_transactions
# ):
#     """TEst linstring intersect. Test data doesn't have a bbox so will fail."""
#     item = load_test_data("test_item.json")
#     postgres_transactions.create_item(item, request=MockStarletteRequest)
#
#     line = [[150.04, -33.14], [150.22, -33.89]]
#     intersects = {"type": "LineString", "coordinates": line}
#
#     params = {
#         "intersects": intersects,
#         "collections": [item["collection"]],
#     }
#     resp = app_client.post("/search", json=params)
#     assert resp.status_code == 200
#     resp_json = resp.json()
#     assert len(resp_json["features"]) == 1