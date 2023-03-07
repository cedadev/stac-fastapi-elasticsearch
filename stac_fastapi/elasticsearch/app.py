# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "11 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.extensions.core import (
    ContextExtension,
    FieldsExtension,
    FilterExtension,
    PaginationExtension,
    SortExtension,
    TransactionExtension,
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

from stac_fastapi.elasticsearch.asset_search import AssetSearchClient
from stac_fastapi.elasticsearch.config import settings
from stac_fastapi.elasticsearch.core import CoreCrudClient
from stac_fastapi.elasticsearch.filters import FiltersClient
from stac_fastapi.elasticsearch.session import Session

extensions = [
    ContextExtension(),
    FieldsExtension(),
    # SortExtension(),
    FilterExtension(client=FiltersClient()),
    FreeTextExtension(),
    ContextCollectionExtension(),
    PaginationExtension(),
]

# Adding the asset search extension seperately as it uses the other extensions
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

session = Session.create_from_settings(settings)
api = StacApi(
    settings=settings,
    extensions=extensions,
    client=CoreCrudClient(session=session, extensions=extensions),
    pagination_extension=PaginationExtension,
    description=settings.STAC_DESCRIPTION,
    title=settings.STAC_TITLE,
    search_get_request_model=create_get_request_model(extensions),
    search_post_request_model=create_post_request_model(extensions),
)

app = api.app
