# encoding: utf-8
"""

"""
__author__ = "Rhys Evans"
__date__ = "28 Jan 2022"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "rhys.r.evans@stfc.ac.uk"

import logging

# Typing imports
from typing import List, Type, Union

# Third-party imports
import attr

# Stac FastAPI asset search imports
from stac_fastapi_asset_filelist.client import BaseAssetFileListClient
from stac_fastapi_asset_filelist.types import Asset

# Package imports
from stac_fastapi.elasticsearch.models.database import ElasticsearchAsset
from stac_fastapi.elasticsearch.models.serializers import InlineAssetSerializer

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class AssetFileListClient(BaseAssetFileListClient):

    asset_table: Type[ElasticsearchAsset] = attr.ib(default=ElasticsearchAsset)

    def get_asset_filelist(
        self, item_id: str = None, collection_id: str = None, **kwargs
    ) -> List[Asset]:
        """Get item asset file list (GET).

        Called with `GET /collection/{collection_id}/items/{item_id}/asset_filelist.json`.

        Returns:
            List containing assets for given item.
        """

        assets = (
            self.asset_table.search()
            .filter("term", item_id=item_id)
            .filter("term", properties__catagories="data")
            .extra(size=10000)
        )

        response = []

        for asset in assets.execute():
            response_asset = InlineAssetSerializer.db_to_stac(asset)
            response.append(response_asset)

        return response
