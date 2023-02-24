# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "13 Sep 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

# Python imports
import collections
import re
from string import Template

from elasticsearch_dsl import Document
from elasticsearch_dsl.query import QueryString

# Typing imports
from elasticsearch_dsl.search import Q, Search

# Third-party imports
from fastapi import HTTPException
from pygeofilter.parsers.cql2_json import parse as parse_json2
from pygeofilter.parsers.cql2_text import parse as parse_text

# CQL Filters imports
from pygeofilter.parsers.cql_json import parse as parse_json
from pygeofilter_elasticsearch import to_filter

# Package imports
from stac_fastapi.elasticsearch.models.utils import Coordinates


def dict_merge(*args, add_keys=True) -> dict:
    assert len(args) >= 2, "dict_merge requires at least two dicts to merge"

    # Make a copy of the root dict
    rtn_dct = args[0].copy()

    merge_dicts = args[1:]

    for merge_dct in merge_dicts:

        if add_keys is False:
            merge_dct = {
                key: merge_dct[key] for key in set(rtn_dct).intersection(set(merge_dct))
            }

        for k, v in merge_dct.items():

            # This is a new key. Add as is.
            if not rtn_dct.get(k):
                rtn_dct[k] = v

            # This is an existing key with mismatched types
            elif k in rtn_dct and type(v) != type(rtn_dct[k]):
                raise TypeError(
                    f"Overlapping keys exist with different types: original is {type(rtn_dct[k])}, new value is {type(v)}"
                )

            # Recursive merge the next level
            elif isinstance(rtn_dct[k], dict) and isinstance(
                merge_dct[k], collections.abc.Mapping
            ):
                rtn_dct[k] = dict_merge(rtn_dct[k], merge_dct[k], add_keys=add_keys)

            # If the item is a list, append items avoiding duplictes
            elif isinstance(v, list):
                for list_value in v:
                    if list_value not in rtn_dct[k]:
                        rtn_dct[k].append(list_value)
            else:
                rtn_dct[k] = v

    return rtn_dct


def get_queryset(client, table: Document, **kwargs) -> Search:
    """
    Turn the query into an `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`_
    :param client: The client class
    :param table: The table to build the query for
    :param kwargs:
    :return: `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`
    """

    qs = table.search()

    # Query list for must match queries. Equivalent to a logical AND.
    filter_queries = []

    # Query list for should queries. Equivalent to a logical OR.
    should_queries = []

    if asset_ids := kwargs.get("asset_ids"):
        filter_queries.append(Q("terms", asset_id=asset_ids))

    if item_ids := kwargs.get("item_ids"):
        filter_queries.append(Q("terms", item_id=item_ids))

    if collection_ids := kwargs.get("collection_ids"):
        filter_queries.append(Q("terms", collection_id=collection_ids))

    if intersects := kwargs.get("intersects"):
        filter_queries.append(
            Q(
                "geo_shape",
                geometry={
                    "shape": {
                        "type": intersects.get("type"),
                        "coordinates": intersects.get("coordinates"),
                    }
                },
            )
        )

    if bbox := kwargs.get("bbox"):
        bbox = [float(x) for x in bbox]
        filter_queries.append(
            Q(
                "geo_shape",
                spatial__bbox={
                    "shape": {
                        "type": "envelope",
                        "coordinates": Coordinates.from_wgs84(bbox).to_geojson(),
                    }
                },
            )
        )

    if datetime := kwargs.get("datetime"):
        # currently based on datetime being provided in item
        # if a date range, get start and end datetimes and find any items with dates in this range
        # .. identifies an open date range
        # if one datetime, find any items with dates that this intersects
        if match := re.match(
            "(?P<start_datetime>[\S]+)/(?P<end_datetime>[\S]+)", datetime
        ):
            start_date = match.group("start_datetime")
            end_date = match.group("end_datetime")

            if start_date != ".." and end_date != "..":
                should_queries.extend(
                    [
                        Q(
                            "bool",
                            filter=[
                                Q("range", properties__datetime={"gte": start_date}),
                                Q("range", properties__datetime={"lte": end_date}),
                            ],
                        ),
                        Q(
                            "bool",
                            filter=[
                                Q(
                                    "range",
                                    properties__start_datetime={"gte": start_date},
                                ),
                                Q(
                                    "range",
                                    properties__start_datetime={"lte": end_date},
                                ),
                            ],
                        ),
                        Q(
                            "bool",
                            filter=[
                                Q(
                                    "range",
                                    properties__end_datetime={"gte": start_date},
                                ),
                                Q(
                                    "range",
                                    properties__end_datetime={"lte": end_date},
                                ),
                            ],
                        ),
                    ]
                )

            elif start_date != "..":
                should_queries.extend(
                    [
                        Q("range", properties__datetime={"gte": start_date}),
                        Q("range", properties__end_datetime={"gte": start_date}),
                    ]
                )

            elif end_date != "..":
                should_queries.extend(
                    [
                        Q("range", properties__datetime={"lte": end_date}),
                        Q("range", properties__start_datetime={"lte": end_date}),
                    ]
                )

        elif match := re.match("(?P<date>[-\d]+)T(?P<time>[:.\d]+)[Z]?", datetime):
            should_queries.extend(
                [
                    Q("match", properties__datetime=datetime),
                    Q(
                        "bool",
                        filter=[
                            Q("range", properties__start_datetime={"gte": datetime}),
                            Q("range", properties__end_datetime={"lte": datetime}),
                        ],
                    ),
                ]
            )

        elif match := re.match(
            "(?P<year>\d{2,4})[-/.](?P<month>\d{1,2})[-/.](?P<day>\d{1,2})", datetime
        ):
            should_queries.extend(
                [
                    Q("match", properties__datetime=datetime),
                    Q(
                        "bool",
                        filter=[
                            Q(
                                "range",
                                properties__start_datetime={
                                    "gte": f"{datetime}T00:00:00"
                                },
                            ),
                            Q(
                                "range",
                                properties__start_datetime={
                                    "lte": f"{datetime}T23:59:59"
                                },
                            ),
                        ],
                    ),
                    Q(
                        "bool",
                        filter=[
                            Q(
                                "range",
                                properties__end_datetime={
                                    "gte": f"{datetime}T00:00:00"
                                },
                            ),
                            Q(
                                "range",
                                properties__end_datetime={
                                    "lte": f"{datetime}T23:59:59"
                                },
                            ),
                        ],
                    ),
                ]
            )

    if limit := kwargs.get("limit"):
        if limit > 10000:
            raise (
                HTTPException(
                    status_code=424,
                    detail="The number of results requested is outside the maximum window 10,000",
                )
            )
        qs = qs.extra(size=limit)

    if page := kwargs.get("page"):
        page = int(page)
        qs = qs[(page - 1) * limit : page * limit]

    if role := kwargs.get("role"):
        filter_queries.append(Q("terms", categories=role))

    if client.extension_is_enabled("FilterExtension"):

        field_mapping = {
            "datetime": "properties.datetime",
            "bbox": "spatial.bbox.coordinates",
        }

        if qfilter := kwargs.get("filter"):
            if filter_lang := kwargs.get("filter-lang"):
                if filter_lang == "cql2-json":
                    ast = parse_json2(qfilter)
                elif filter_lang == "cql-text":
                    ast = parse_text(qfilter)
                elif filter_lang == "cql-json":
                    ast = parse_json(qfilter)
            else:
                ast = parse_json(qfilter)

            try:
                # TODO: Add support beyond just keyword for boolean and integer filtering.
                qfilter = to_filter(
                    ast,
                    field_mapping,
                    field_default=Template("properties__${name}__keyword"),
                )
            except NotImplementedError:
                raise (
                    HTTPException(status_code=400, detail=f"Invalid filter expression")
                )
            else:
                qs = qs.query(qfilter)

    if client.extension_is_enabled("FreeTextExtension"):
        if q := kwargs.get("q"):
            qs = qs.query(QueryString(query=q, fields=["properties.*"], lenient=True))

    if client.extension_is_enabled("ContextCollectionExtension"):
        if (
            "context_collection" in kwargs
            and kwargs["context_collection"]
            and not collection_ids
        ):
            qs.aggs.bucket("collections", "terms", field="collection_id.keyword")

    qs = qs.query(
        Q(
            "bool",
            must=[Q("bool", should=should_queries), Q("bool", filter=filter_queries)],
        )
    )

    if client.extension_is_enabled("FieldsExtension"):
        if fields := kwargs.get("fields"):

            if isinstance(fields, dict):

                if exclude_fields := fields.get("include"):
                    qs = qs.source(include=list(exclude_fields))

                if exclude_fields := fields.get("exclude"):
                    qs = qs.source(exclude=list(exclude_fields))

            elif isinstance(fields, list):
                qs.source(include=fields)

            else:
                qs.source(include=[fields])

    return qs
