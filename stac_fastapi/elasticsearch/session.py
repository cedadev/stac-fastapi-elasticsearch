# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.elasticsearch.config import ElasticsearchSettings
import attr
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Index, connections
from .models.database import ElasticsearchCollection, ElasticsearchItem, ElasticsearchAsset
from types import ModuleType

@attr.s
class Session:
    """
    An elasticsearch session
    """

    client: Elasticsearch = attr.ib()


    @classmethod
    def create_from_settings(cls, settings: ModuleType) -> "Session":

        # Create the 'default' connection, available globally
        connections.create_connection(
            **settings.ELASTICSEARCH_CONNECTION
        )

        cls.set_indices_from_settings(settings)

        return cls(
            client=connections.get_connection(),
        )

    @classmethod
    def set_indices_from_settings(cls, settings: ElasticsearchSettings) -> None:

        collections = Index(settings.COLLECTION_INDEX)
        items = Index(settings.ITEM_INDEX)
        assets = Index(settings.ASSET_INDEX)

        collections.document(ElasticsearchCollection)
        items.document(ElasticsearchItem)
        assets.document(ElasticsearchAsset)


