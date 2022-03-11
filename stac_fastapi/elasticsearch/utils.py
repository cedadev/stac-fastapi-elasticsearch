# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '13 Sep 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


# Python imports
import collections
from string import Template

# Package imports
from stac_fastapi.elasticsearch.models.utils import Coordinates

# CQL Filters imports
from pygeofilter.parsers.cql_json import parse as parse_json
from pygeofilter.parsers.cql2_text import parse as parse_text
from pygeofilter.parsers.cql2_json import parse as parse_json2
from pygeofilter_elasticsearch import to_filter

# Third-party imports
from fastapi import HTTPException

# Typing imports
from elasticsearch_dsl.search import Search
from elasticsearch_dsl.query import QueryString
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from elasticsearch_dsl import Document


def dict_merge(*args, add_keys=True) -> dict:
    assert len(args) >= 2, "dict_merge requires at least two dicts to merge"

    # Make a copy of the root dict
    rtn_dct = args[0].copy()

    merge_dicts = args[1:]

    for merge_dct in merge_dicts:

        if add_keys is False:
            merge_dct = {key: merge_dct[key] for key in set(rtn_dct).intersection(set(merge_dct))}

        for k, v in merge_dct.items():

            # This is a new key. Add as is.
            if not rtn_dct.get(k):
                rtn_dct[k] = v

            # This is an existing key with mismatched types
            elif k in rtn_dct and type(v) != type(rtn_dct[k]):
                raise TypeError(f"Overlapping keys exist with different types: original is {type(rtn_dct[k])}, new value is {type(v)}")

            # Recursive merge the next level
            elif isinstance(rtn_dct[k], dict) and isinstance(merge_dct[k], collections.abc.Mapping):
                rtn_dct[k] = dict_merge(rtn_dct[k], merge_dct[k], add_keys=add_keys)

            # If the item is a list, append items avoiding duplictes
            elif isinstance(v, list):
                for list_value in v:
                    if list_value not in rtn_dct[k]:
                        rtn_dct[k].append(list_value)
            else:
                rtn_dct[k] = v

    return rtn_dct


def get_queryset(client, table: "Document" , **kwargs) -> Search:
    """
    Turn the query into an `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`_
    :param client: The client class
    :param table: The table to build the query for
    :param kwargs: 
    :return: `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`
    """

    qs = table.search()

    if asset_ids := kwargs.get('asset_ids'):
        qs = qs.filter('terms', asset_id=asset_ids)

    if item_ids := kwargs.get('item_ids'):
        qs = qs.filter('terms', item_id=item_ids)

    if collection_ids := kwargs.get('collection_ids'):
        qs = qs.filter('terms', collection_id=collection_ids)

    if intersects := kwargs.get('intersects'):
        qs = qs.filter('geo_shape', geometry={
            'shape': {
                'type': intersects.get('type'),
                'coordinates': intersects.get('coordinates')
            }
        })

    if bbox := kwargs.get('bbox'):
        qs = qs.filter('geo_shape', spatial_bbox={
            'shape': {
                'type': 'envelope',
                'coordinates': Coordinates.from_wgs84(bbox).to_geojson()
            }
        })
    
    if datetime := kwargs.get('datetime'):
        # currently based on datetime being provided in item
        # if a date range, get start and end datetimes and find any items with dates in this range
        # .. identifies an open date range
        # if one datetime, find any items with dates that this intersects
        if "/" in datetime:
            split = datetime.split('/')
            start_date = split[0]
            end_date = split[1]

            if start_date != '..':
                qs = qs.filter('range', properties__datetime={'gte': start_date})

            if end_date != '..':
                qs = qs.filter('range', properties__datetime={'lte': end_date})

            # TODO: add in option that searches start and end datetime if datetime is null in item

        else:

            qs = qs.filter('match', properties__datetime=kwargs.get('datetime'))
        
            # TODO: add in option for if item specifies start datetime and end datetime instead of datetime
            # should return items which cover a range that the specified datetime falls in
            

    if limit := kwargs.get('limit'):
        if limit > 10000:
            raise (
                HTTPException(
                    status_code=424,
                    detail="The number of results requested is outside the maximum window 10,000")
            )
        qs = qs.extra(size=limit)

    if page := kwargs.get('page'):
        page = int(page)
        qs = qs[(page - 1) * limit:page * limit]

    if role := kwargs.get('role'):
        qs = qs.filter('terms', categories=role)

    if client.extension_is_enabled('FilterExtension'):

        field_mapping = {
            'datetime': 'properties.datetime',
            'bbox': 'spatial.bbox.coordinates'
        }

        if qfilter := kwargs.get('filter'):
            if filter_lang := kwargs.get('filter-lang'):
                if filter_lang == "cql2-json":
                    ast = parse_json2(qfilter)
                elif filter_lang == "cql-text":
                    ast = parse_text(qfilter)
                elif filter_lang == "cql-json":
                    ast = parse_json(qfilter)
            else:
                ast = parse_json(qfilter)

            try:
                qfilter = to_filter(
                    ast,
                    field_mapping,
                    field_default=Template('properties__${name}__keyword')
                )
            except NotImplementedError:
                raise (
                    HTTPException(
                        status_code=400,
                        detail=f'Invalid filter expression'
                    )
                )
            else:
                qs = qs.query(qfilter)

    if client.extension_is_enabled('FreeTextExtension'):
        if q := kwargs.get('q'):
            qs = qs.query(
                QueryString(
                    query=q,
                    default_field='properties.*',
                    lenient=True
                )
            )

    if client.extension_is_enabled('ContextCollectionExtension'):
        if not collection_ids:
            qs.aggs.bucket('collections', 'terms', field='collection')

    return qs
