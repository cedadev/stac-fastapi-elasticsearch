# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '30 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'
#
#
# def test_get_collection(
#     postgres_core: CoreCrudClient,
#     postgres_transactions: TransactionsClient,
#     load_test_data: Callable,
# ):
#     data = load_test_data("test_collection.json")
#     postgres_transactions.create_collection(data, request=MockStarletteRequest)
#     coll = postgres_core.get_collection(data["id"], request=MockStarletteRequest)
#     assert Collection(**data).dict(exclude={"links"}) == Collection(**coll).dict(
#         exclude={"links"}
#     )
#     assert coll["id"] == data["id"]
#
#
# def test_get_item(
#     postgres_core: CoreCrudClient,
#     postgres_transactions: TransactionsClient,
#     load_test_data: Callable,
# ):
#     collection_data = load_test_data("test_collection.json")
#     postgres_transactions.create_collection(
#         collection_data, request=MockStarletteRequest
#     )
#     data = load_test_data("test_item.json")
#     postgres_transactions.create_item(data, request=MockStarletteRequest)
#     coll = postgres_core.get_item(
#         item_id=data["id"],
#         collection_id=data["collection"],
#         request=MockStarletteRequest,
#     )
#     assert coll["id"] == data["id"]
#     assert coll["collection"] == data["collection"]
#
#
# def test_get_collection_items(
#     postgres_core: CoreCrudClient,
#     postgres_transactions: TransactionsClient,
#     load_test_data: Callable,
# ):
#     coll = load_test_data("test_collection.json")
#     postgres_transactions.create_collection(coll, request=MockStarletteRequest)
#
#     item = load_test_data("test_item.json")
#
#     for _ in range(5):
#         item["id"] = str(uuid.uuid4())
#         postgres_transactions.create_item(item, request=MockStarletteRequest)
#
#     fc = postgres_core.item_collection(coll["id"], request=MockStarletteRequest)
#     assert len(fc["features"]) == 5
#
#     for item in fc["features"]:
#         assert item["collection"] == coll["id"]
#
#
# def test_landing_page_no_collection_title(
#     postgres_core: CoreCrudClient,
#     postgres_transactions: TransactionsClient,
#     load_test_data: Callable,
#     api_client: StacApi,
# ):
#     class MockStarletteRequestWithApp(MockStarletteRequest):
#         app = api_client.app
#
#     coll = load_test_data("test_collection.json")
#     del coll["title"]
#     postgres_transactions.create_collection(coll, request=MockStarletteRequest)
#
#     landing_page = postgres_core.landing_page(request=MockStarletteRequestWithApp)
#     for link in landing_page["links"]:
#         if link["href"].split("/")[-1] == coll["id"]:
#             assert link["title"]