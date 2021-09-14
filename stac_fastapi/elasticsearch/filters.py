# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '29 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.elasticsearch.models.database import ElasticsearchCollection

from stac_fastapi.types.core import BaseFiltersClient
from .utils import dict_merge

import attr
from elasticsearch import NotFoundError

from typing import Dict, Any, Optional


@attr.s
class FiltersClient(BaseFiltersClient):

    def collection_summaries(self, collection_id: str) -> Dict:

        properties = {}

        try:
            collection = ElasticsearchCollection.get(id=collection_id)
        except NotFoundError:
            raise (NotFoundError(404, f'Collection: {collection_id} not found'))

        if summaries := collection.get_summaries():
            for k, v in summaries.items():
                prop = {
                    k: {
                        'title': k.replace('_', ' ').title(),
                        'type': 'string',
                        'enum': v
                    }
                }
                properties.update(prop)

        if extent := collection.get_extent():
            temp_min, temp_max = extent['temporal']['interval'][0]
            prop = {
                'datetime': {
                    'type': 'datetime',
                    'minimum': temp_min,
                    'maximum': temp_max
                },
                'bbox': {
                    'description': 'bounding box for the collection',
                    'type': 'array',
                    'minItems': 4,
                    'maxItems': 6,
                    'items': {
                        'type': 'number'
                    }
                }
            }

            properties.update(prop)

        return properties

    def get_queryables(
            self, collectionId: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:

        schema = super().get_queryables()

        if collectionId:

            properties = self.collection_summaries(collectionId)

            schema['$id'] = f'{kwargs["request"].base_url}/{collectionId}/queryables'
            schema['title'] = f'Queryables for {collectionId}'
            schema['description'] = f'Queryable names and values for the {collectionId} collection'
            schema['properties'] = properties

        else:
            query_params = kwargs['request'].query_params
            collections = query_params.get('collections', [])
            if collections:
                collections = collections.split(',')

            properties = {}

            for collection in collections:
                if not properties:
                    properties = self.collection_summaries(collection)
                else:
                    new_props = self.collection_summaries(collection)
                    intersect = {}
                    for prop, value in properties.items():
                        if prop in new_props:
                            if value.get('type') == 'string':
                                intersect[prop] = dict_merge(value, new_props[prop])
                    properties = intersect

            schema['$id'] = f'{kwargs["request"].base_url}/queryables'
            schema['title'] = f'Global queryables, reduced by collection context'
            schema['description'] = f'Queryable names and values'
            schema['properties'] = properties

        return schema
