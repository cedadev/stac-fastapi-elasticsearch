# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.types.config import ApiSettings

from typing import List, Optional, Union, Dict, Set


class ElasticsearchSettings(ApiSettings):
    """
    Elasticsearch specific API settings

    Attributes:
        elasticsearch_hosts: Hostnames for elasticsearch connection pool
        elasticsearch_collection_index: index name containing STAC Collections
        elasticsearch_item_index: index name containing STAC items
        elasticsearch_asset_index: index name containing STAC assets
        connection_kwargs: dict of additional connection kwargs
    """

    elasticsearch_hosts: Union[str, List[str], Dict]
    elasticsearch_item_index: str
    elasticsearch_asset_index: str
    connection_kwargs: Optional[Dict]

    # Fields which are defined by STAC but not included in the database model
    forbidden_fields: Set[str] = set()

    # Fields which are item properties but indexed as distinct fields in the database model
    indexed_fields: Set[str] = set()
