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

import attr
from elasticsearch import NotFoundError

from typing import Dict, Any, Optional


@attr.s
class FiltersClient(BaseFiltersClient):

    def get_queryables(
            self, collectionId: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:

        schema = super().get_queryables()

        if id:
            try:
                collection = ElasticsearchCollection.get(id=collectionId)
            except NotFoundError:
                raise (NotFoundError(404, f'Collection: {collectionId} not found'))

            schema['$id'] = f'{kwargs["request"].base_url}/{collectionId}/queryables'
            schema['title'] = f'Queryables for {collectionId}'
            schema['description'] = f'Queryable names and values for the {collectionId} collection'

            if summaries := collection.get_summaries('properties'):
                for k, v in summaries.items():
                    prop = {
                        k: {
                            'title': k.replace('_', ' ').title(),
                            'type': 'string',
                            'enum': v
                        }
                    }
                    schema['properties'].update(prop)

            if extent := collection.get_extent('extent'):
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

                schema['properties'].update(prop)

        else:
            # What to do for the root level queryables? Perhaps this is semi-static and built
            # from the top level in the vocabulary tree from the vocab service? For now, return
            # blank schema.
            ...
        return schema
