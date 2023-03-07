# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "11 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from stac_fastapi.elasticsearch.config import settings


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
