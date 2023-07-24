# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "11 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from types import ModuleType

import attr
from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections


@attr.s
class Session:
    """
    An elasticsearch session
    """

    client: Elasticsearch = attr.ib()

    @classmethod
    def create_from_settings(cls, settings: ModuleType) -> "Session":

        # Create the 'default' connection, available globally
        connections.create_connection(**settings.ELASTICSEARCH_CONNECTION)

        return cls(
            client=connections.get_connection(),
        )
