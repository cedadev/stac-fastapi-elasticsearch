# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.api.app import StacApi
from stac_fastapi.extensions.core import (
    ContextExtension,
    # FieldsExtension,
    # SortExtension,
    FilterExtension,
    PaginationExtension
)


from stac_fastapi.elasticsearch.session import Session
from stac_fastapi.elasticsearch.core import CoreCrudClient
from stac_fastapi.elasticsearch.filters import FiltersClient
from stac_fastapi.elasticsearch.config import settings


from stac_fastapi_freetext.free_text import FreeTextExtension
from stac_fastapi_context_collections.context_collections import ContextCollectionExtension

extensions = [
    ContextExtension(),
    # FieldsExtension(),
    # SortExtension(),
    FilterExtension(client=FiltersClient()),
    FreeTextExtension(),
    ContextCollectionExtension(),
    PaginationExtension()
]

session = Session.create_from_settings(settings)
api = StacApi(
    settings=settings,
    extensions=extensions,
    client=CoreCrudClient(session=session, extensions=extensions),
    pagination_extension=PaginationExtension,
    description=settings.STAC_DESCRIPTION,
    title=settings.STAC_TITLE,
)

app = api.app


def run():
    """Run app from command line using uvicorn if available.
    This has been built specifically for use with docker-compose.
    """
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.elasticsearch.app:app",
            host=settings.APP_HOST,
            port=settings.APP_PORT,
            log_level="info",
            reload_dirs=["/app/stac_fastapi", "/app/conf"],
            reload=True,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()
