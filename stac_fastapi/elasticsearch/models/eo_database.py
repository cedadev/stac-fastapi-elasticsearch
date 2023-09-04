# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "14 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import re
from string import Template
from typing import Optional
from urllib.parse import urljoin

from elasticsearch import NotFoundError
from elasticsearch_dsl import A, Search
from elasticsearch_dsl.query import QueryString
from elasticsearch_dsl.search import Q, Search

# Third-party imports
from fastapi import HTTPException
from pygeofilter.parsers.cql2_json import parse as parse_json2
from pygeofilter.parsers.cql2_text import parse as parse_text

# CQL Filters imports
from pygeofilter.parsers.cql_json import parse as parse_json
from pygeofilter_elasticsearch import to_filter
from stac_fastapi.elasticsearch.config import settings
from stac_fastapi.elasticsearch.models import database
from stac_fastapi.types.links import CollectionLinks, ItemLinks
from stac_pydantic.shared import MimeTypes

from .utils import Coordinates, rgetattr

DEFAULT_EXTENT = {"temporal": [[None, None]], "spatial": [[-180, -90, 180, 90]]}
STAC_VERSION_DEFAULT = "1.0.0"


class ElasticsearchEOItem(database.STACDocument):
    type = "Feature"

    def _search(self, **kwargs):
        search = super()._search(catalog=kwargs.get("catalog", None))

        return self.get_queryset(search, **kwargs)

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

    def get(self, **kwargs):
        try:
            items = self.search(**kwargs)
            return items[0]

        except IndexError as exc:
            raise NotFoundError from exc

    def get_queryset(self, qs: Search, **kwargs) -> Search:
        """
        Turn the query into an `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`_
        :param client: The client class
        :param table: The table to build the query for
        :param kwargs:
        :return: `elasticsearch_dsl.Search object <https://elasticsearch-dsl.readthedocs.io/en/latest/api.html#search>`
        """

        # Query list for must match queries. Equivalent to a logical AND.
        filter_queries = []

        # Query list for should queries. Equivalent to a logical OR.
        should_queries = []

        if item_ids := kwargs.get("item_ids"):
            filter_queries.append(Q("terms", _id=item_ids))

        if collection_ids := kwargs.get("collection_ids"):
            filter_queries.append(
                Q("terms", misc__platform__Satellite__raw=collection_ids)
            )

        if intersects := kwargs.get("intersects"):
            filter_queries.append(
                Q(
                    "geo_shape",
                    spatial__geometries__full_search={
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
                    spatial__geometries__full_search={
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
                                    Q(
                                        "range",
                                        temporal__end_time={"gte": start_date},
                                    ),
                                    Q("range", temporal__start_time={"lte": end_date}),
                                ],
                            ),
                            Q(
                                "bool",
                                filter=[
                                    Q(
                                        "range",
                                        temporal__start_time={"gte": start_date},
                                    ),
                                    Q(
                                        "range",
                                        temporal__start_time={"lte": end_date},
                                    ),
                                ],
                            ),
                            Q(
                                "bool",
                                filter=[
                                    Q(
                                        "range",
                                        temporal__end_time={"gte": start_date},
                                    ),
                                    Q(
                                        "range",
                                        temporal__end_time={"lte": end_date},
                                    ),
                                ],
                            ),
                        ]
                    )

                elif start_date != "..":
                    should_queries.extend(
                        [
                            Q("range", temporal__end_time={"gte": start_date}),
                        ]
                    )

                elif end_date != "..":
                    should_queries.extend(
                        [
                            Q("range", temporal__start_time={"lte": end_date}),
                        ]
                    )

            elif match := re.match("(?P<date>[-\d]+)T(?P<time>[:.\d]+)[Z]?", datetime):
                should_queries.extend(
                    [
                        Q(
                            "bool",
                            filter=[
                                Q(
                                    "range",
                                    temporal__start_time={"gte": datetime},
                                ),
                                Q("range", temporal__end_time={"lte": datetime}),
                            ],
                        ),
                    ]
                )

            elif match := re.match(
                "(?P<year>\d{2,4})[-/.](?P<month>\d{1,2})[-/.](?P<day>\d{1,2})",
                datetime,
            ):
                should_queries.extend(
                    [
                        Q(
                            "bool",
                            filter=[
                                Q(
                                    "range",
                                    temporal__start_time={
                                        "gte": f"{datetime}T00:00:00"
                                    },
                                ),
                                Q(
                                    "range",
                                    temporal__start_time={
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
                                    temporal__end_time={"gte": f"{datetime}T00:00:00"},
                                ),
                                Q(
                                    "range",
                                    temporal__end_time={"lte": f"{datetime}T23:59:59"},
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

        if self.extension_is_enabled("FilterExtension"):
            field_mapping = {
                "datetime": "temporal.start_time",
                "bbox": "spatial.geometries.full_search",
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
                        field_default=Template("misc__*__${name}__keyword"),
                    )
                except NotImplementedError:
                    raise (
                        HTTPException(
                            status_code=400, detail=f"Invalid filter expression"
                        )
                    )
                else:
                    qs = qs.query(qfilter)

        if self.extension_is_enabled("FreeTextExtension"):
            if q := kwargs.get("q"):
                qs = qs.query(QueryString(query=q, fields=["misc.*"], lenient=True))

        if self.extension_is_enabled("ContextCollectionExtension"):
            if (
                "context_collection" in kwargs
                and kwargs["context_collection"]
                and not collection_ids
            ):
                qs.aggs.bucket(
                    "collections", "terms", field="misc.platform.Satellite.keyword"
                )

        qs = qs.query(
            Q(
                "bool",
                must=[
                    Q("bool", should=should_queries),
                    Q("bool", filter=filter_queries),
                ],
            )
        )

        if self.extension_is_enabled("FieldsExtension"):
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

        if self.extension_is_enabled("SortExtension"):
            sort_params = []
            if sortby := kwargs.get("sortby"):
                for s in sortby:
                    if isinstance(s, str):
                        s = s.lstrip("+")
                    elif isinstance(s, dict):
                        s = {s["field"]: {"order": s["direction"]}}
                    sort_params.append(s)
            qs = qs.sort(*sort_params)

        return qs

    def get_stac_assets(self) -> dict:
        """
        Return stac assets
        """
        file = getattr(self, "file", {})
        directory = getattr(file, "directory", "")
        data_file = getattr(file, "data_file", "")
        data_files = getattr(file, "data_files", "")
        metadata_file = getattr(file, "metadata_file", "")
        quicklook_file = getattr(file, "quicklook_file", "")

        stac_assets = {}

        if data_file and not data_files:
            stac_assets["data_file"] = {
                "href": f"{settings.posix_download_url}{directory}/{data_file}",
                "title": data_file,
                "type": "application/zip",
                "roles": ["data"],
            }

        if data_files:
            for count, value in enumerate(data_files.split(",")):
                stac_assets[f"data_file_{count}"] = {
                    "href": f"{settings.posix_download_url}{directory}/{value}",
                    "title": value,
                    "type": "application/zip",
                    "roles": ["data"],
                }

        if metadata_file:
            stac_assets["metadata_file"] = {
                "href": f"{settings.posix_download_url}{directory}/{metadata_file}",
                "title": metadata_file,
                "type": "application/xml",
                "roles": ["metadata"],
            }

        if quicklook_file:
            stac_assets["quicklook_file"] = {
                "href": f"{settings.posix_download_url}{directory}/{quicklook_file}",
                "title": quicklook_file,
                "type": "image/png",
                "roles": ["thumbnail"],
            }

        return stac_assets

    def get_properties(self) -> dict:
        """
        Return properties
        """
        properties = getattr(self, "misc", {})

        if not isinstance(properties, dict):
            properties.to_dict()

        properties["size"] = rgetattr(self, "file.size")
        properties["location"] = rgetattr(self, "file.location")

        if hasattr(self, "temporal"):
            if "start_datetime" not in properties or "end_datetime" not in properties:
                properties["datetime"] = None
                properties["start_datetime"] = rgetattr(self, "temporal.start_time")
                properties["end_datetime"] = rgetattr(self, "temporal.end_time")

        return properties.to_dict() if not isinstance(properties, dict) else {}

    def get_bbox(self):
        """
        Return a WGS84 formatted bbox

        :return:
        """

        try:
            coordinates = rgetattr(self, "spatial.geometries.search.coordinates")
        except AttributeError:
            return

        return

    def get_geometry(self):
        return rgetattr(self, "spatial.geometries.full_search").to_dict()

    def get_collection_id(self) -> str:
        """
        Return the collection id
        """

        return rgetattr(self, "misc.platform.Satellite")

    def get_links(self, base_url: str) -> list:
        """
        Returns list of links
        """
        links = ItemLinks(
            base_url=str(base_url),
            collection_id=self.get_collection_id(),
            item_id=self.get_id(),
        ).create_links()

        if self.extension_is_enabled("AssetSearchExtension"):
            links.append(
                dict(
                    rel="assets",
                    type=MimeTypes.json,
                    href=urljoin(
                        base_url,
                        f"collections/{self.get_collection_id()}/items/{self.get_id()}/assets",
                    ),
                )
            )

        return links


class ElasticsearchEOCollection(database.STACDocument):
    """
    Collection class
    """

    type = "FeatureCollection"

    def get(self, id, **kwargs):
        try:
            result = self.search(id=id)
            return next(result)

        except (IndexError, StopIteration):
            raise NotFoundError

    @classmethod
    def _search(cls, id: str = None, **kwargs):
        agg = A("terms", field="misc.platform.Satellite.raw", size=15)

        # orbit_info
        agg.bucket(
            "Cycle Number",
            "terms",
            field="misc.orbit_info.Cycle Number.keyword",
        )
        agg.bucket(
            "Pass Direction",
            "terms",
            field="misc.orbit_info.Pass Direction.keyword",
        )
        agg.bucket(
            "Phase Identifier",
            "terms",
            field="misc.orbit_info.Phase Identifier.keyword",
        )
        agg.bucket(
            "Start Orbit Number",
            "terms",
            field="misc.orbit_info.Start Orbit Number.keyword",
        )
        agg.bucket(
            "Start Relative Orbit Number",
            "terms",
            field="misc.orbit_info.Start Relative Orbit Number.keyword",
        )
        agg.bucket(
            "Stop Orbit Number",
            "terms",
            field="misc.orbit_info.Stop Orbit Number.keyword",
        )
        agg.bucket(
            "Stop Relative Orbit Number",
            "terms",
            field="misc.orbit_info.Stop Relative Orbit Number.keyword",
        )

        # platform
        agg.bucket("Family", "terms", field="misc.platform.Family.keyword")
        agg.bucket(
            "Instrument Abbreviation",
            "terms",
            field="misc.platform.Instrument Abbreviation.keyword",
        )
        agg.bucket(
            "Instrument Family Name",
            "terms",
            field="misc.platform.Instrument Family Name.keyword",
        )
        agg.bucket(
            "Instrument Mode",
            "terms",
            field="misc.platform.Instrument Mode.keyword",
        )
        agg.bucket(
            "Mission",
            "terms",
            field="misc.platform.Mission.keyword",
        )
        agg.bucket(
            "NSSDC Identifier",
            "terms",
            field="misc.platform.NSSDC Identifier.keyword",
        )
        agg.bucket(
            "Platform Family Name",
            "terms",
            field="misc.platform.Platform Family Name.keyword",
        )
        agg.bucket(
            "Platform Number",
            "terms",
            field="misc.platform.Platform Number.keyword",
        )

        # product_info
        agg.bucket(
            "Polarisation",
            "terms",
            field="misc.product_info.Polarisation.keyword",
        )
        agg.bucket(
            "Product Class",
            "terms",
            field="misc.product_info.Product Class.keyword",
        )
        agg.bucket(
            "Product Class Description",
            "terms",
            field="misc.product_info.Product Class Description.keyword",
        )
        agg.bucket(
            "Product Composition",
            "terms",
            field="misc.product_info.Product Composition.keyword",
        )
        agg.bucket(
            "Product Type",
            "terms",
            field="misc.product_info.Product Type.keyword",
        )
        agg.bucket(
            "Resolution",
            "terms",
            field="misc.product_info.Resolution.keyword",
        )
        agg.bucket(
            "Timeliness Category",
            "terms",
            field="misc.product_info.Timeliness Category.keyword",
        )

        # quality_info
        agg.bucket(
            "Min Cloud Coverage Assessment",
            "min",
            field="misc.quality_info.Cloud Coverage Assessment",
        )
        agg.bucket(
            "Max Cloud Coverage Assessment",
            "max",
            field="misc.quality_info.Cloud Coverage Assessment",
        )
        agg.bucket(
            "Average Cloud Coverage Assessment",
            "avg",
            field="misc.quality_info.Cloud Coverage Assessment",
        )

        # solar_zenith
        # nadir
        agg.bucket("nadir_min", "min", field="misc.solar_zenith.nadir.min")
        agg.bucket("nadir_max", "max", field="misc.solar_zenith.nadir.max")
        # oblique
        agg.bucket("oblique_min", "min", field="misc.solar_zenith.oblique.min")
        agg.bucket("oblique_max", "max", field="misc.solar_zenith.oblique.max")

        # solar_zenith_angle
        agg.bucket("solar_zenith_angle_min", "min", field="misc.solar_zenith_angle.min")
        agg.bucket("solar_zenith_angle_max", "max", field="misc.solar_zenith_angle.max")

        # spatial
        # VERY SLOW
        # if id:
        #     agg.bucket(
        #         "geo_extent",
        #         "scripted_metric",
        #         init_script="state.x = []; state.y = []",
        #         map_script="for (c in params['_source']['spatial']['geometries']['display']['coordinates'][0]) { state.x.add(c[0]); state.y.add(c[1]) }",
        #         combine_script="double min_x = 500, max_x = -500, min_y = 500, max_y = -500; for (t in state.x) { if (t < min_x) {min_x=t} else if (t > max_x) {max_x=t} } for (t in state.y) { if (t < min_y) {min_y=t} else if (t > max_y) {max_y=t} } return [min_x, max_x, min_y, max_y]",
        #         reduce_script="double min_x = 500, max_x = -500, min_y = 500, max_y = -500; for (a in states) { if (a[0] < min_x) {min_x=a[0]} if (a[1] > max_x) {max_x=a[1]} if (a[2] < min_y) {min_y=a[2]} if (a[3] > max_y) {max_y=a[3]} } return [min_x, max_x, min_y, max_y]",
        #     )

        # temporal
        agg.bucket("temporal_min", "min", field="temporal.start_time")
        agg.bucket("temporal_max", "max", field="temporal.end_time")

        search = super()._search(**kwargs)

        if id:
            search = search.query("term", misc__platform__Satellite__raw=id)

        search.aggs.bucket("satallites", agg)

        response = search.execute()

        if not response.aggregations.satallites.buckets:
            raise IndexError

        for aggregation in response.aggregations.satallites.buckets:
            # try:
            #     coordinates = Coordinates(
            #         aggregation.geo_extent.value[0],
            #         aggregation.geo_extent.value[1],
            #         aggregation.geo_extent.value[2],
            #         aggregation.geo_extent.value[3],
            #     )
            # except AttributeError:
            #     coordinates = Coordinates.from_wgs84(DEFAULT_EXTENT["spatial"][0])

            collection = {
                "id": aggregation.key,
                "item_count": aggregation.doc_count,
                "extent": {
                    "temporal": {
                        "interval": [
                            [
                                aggregation.temporal_min.value_as_string,
                                aggregation.temporal_min.value_as_string,
                            ]
                        ]
                    },
                    "spatial": {
                        # "bbox": coordinates.to_wgs84()],
                        "bbox": DEFAULT_EXTENT["spatial"],
                    },
                },
                "properties": {},
            }

            # think there's a cleaner way to do this
            for key, term in aggregation.to_dict().items():
                if isinstance(term, dict):
                    if (
                        "value" in term
                        and term["value"]
                        and "value_as_string" not in term
                    ):
                        collection[key] = term["value"]

                    elif "buckets" in term and term["buckets"]:
                        collection["properties"][key] = [
                            bucket["key"] for bucket in term["buckets"]
                        ]

            yield ElasticsearchEOCollection(
                meta={"id": collection.get("id")},
                id=collection.get("id"),
                title=collection.get("id"),
                description=collection.get("description", ""),
                license=collection.get("license", ""),
                properties=collection.get("properties", {}),
                extent=collection.get("extent", {}),
            )

    def search(self, **kwargs):
        return self._search(**kwargs)

    def count(self, **kwargs):
        agg = A("value_count", field="misc.platform.Satellite.raw")

        search = super()._search(**kwargs)

        search.aggs.bucket("collection_count", agg)

        response = search.execute()

        return response.aggregations.collection_count.value

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

    def get_summaries(self) -> Optional[dict]:
        """
        Turns the elastic-dsl AttrDict into a dict or None

        """
        if hasattr(self, "properties"):
            return getattr(self, "properties").to_dict()

        return {}

    def get_extent(self) -> dict:
        """
        Takes the elastic-dsl Document and extracts the
        extent information from it.

        """
        if hasattr(self, "extent"):
            return getattr(self, "extent").to_dict()

        return DEFAULT_EXTENT

    def get_keywords(self) -> list:
        return getattr(self, "keywords", [])

    def get_title(self) -> str:
        return getattr(self, "title", "")

    def get_description(self) -> str:
        return getattr(self, "description", "")

    def get_license(self) -> str:
        return getattr(self, "license", "")

    def get_providers(self) -> list:
        return getattr(self, "providers", [])

    def get_links(self, base_url: str) -> list:
        """
        Returns list of links
        """
        collection_links = CollectionLinks(
            base_url=base_url,
            collection_id=self.get_id(),
        ).create_links()

        if self.extension_is_enabled("FilterExtension"):
            collection_links.append(
                {
                    "rel": "https://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": MimeTypes.json,
                    "href": urljoin(
                        base_url,
                        f"collections/{self.get_id()}/queryables",
                    ),
                }
            )

        return collection_links
