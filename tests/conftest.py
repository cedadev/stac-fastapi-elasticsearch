# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "15 Nov 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from typing import List

import pytest
from stac_fastapi.api.app import StacApi
from stac_fastapi.elasticsearch.asset_search import AssetSearchClient
from stac_fastapi.elasticsearch.config import settings
from stac_fastapi.elasticsearch.core import CoreCrudClient
from stac_fastapi.elasticsearch.filters import FiltersClient
from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.elasticsearch.transactions import TransactionsClient
from stac_fastapi.extensions.core import (  # TransactionExtension
    ContextExtension,
    FieldsExtension,
    FilterExtension,
    PaginationExtension,
    SortExtension,
)
from stac_fastapi_asset_search.asset_search import AssetSearchExtension
from stac_fastapi_asset_search.client import (
    create_asset_search_get_request_model,
    create_asset_search_post_request_model,
)
from stac_fastapi_context_collections.context_collections import (
    ContextCollectionExtension,
)
from stac_fastapi_freetext.free_text import FreeTextExtension
from starlette.testclient import TestClient


@pytest.fixture
def extensions() -> List:
    return [
        ContextExtension(),
        # FieldsExtension(),
        # SortExtension(),
        FilterExtension(client=FiltersClient()),
        FreeTextExtension(),
        ContextCollectionExtension(),
        PaginationExtension(),
        # TransactionExtension(client=TransactionsClient(), settings=settings),
    ]


@pytest.fixture
def db_session() -> Session:
    return Session.create_from_settings(settings)


@pytest.fixture
def api_client(db_session, extensions):
    extensions.append(
        AssetSearchExtension(
            client=AssetSearchClient(extensions=extensions),
            asset_search_get_request_model=create_asset_search_get_request_model(
                extensions
            ),
            asset_search_post_request_model=create_asset_search_post_request_model(
                extensions
            ),
            settings=settings,
        )
    )
    return StacApi(
        settings=settings,
        extensions=extensions,
        client=CoreCrudClient(session=db_session, extensions=extensions),
        pagination_extension=PaginationExtension,
        description=settings.STAC_DESCRIPTION,
        title=settings.STAC_TITLE,
    )


@pytest.fixture
def app_client(api_client):

    with TestClient(api_client.app) as test_app:
        yield test_app
