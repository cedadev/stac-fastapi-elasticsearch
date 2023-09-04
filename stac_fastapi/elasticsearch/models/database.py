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
from elasticsearch_dsl import Document, Search
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
from stac_fastapi.types.links import CollectionLinks, ItemLinks
from stac_fastapi_asset_search.types import AssetLinks
from stac_pydantic.shared import MimeTypes

from .utils import Coordinates, rgetattr

DEFAULT_EXTENT = {"temporal": [[None, None]], "spatial": [[-180, -90, 180, 90]]}
STAC_VERSION_DEFAULT = "1.0.0"


DEFAULT_MODELS = {
    "ASSET_INDEX": "asset",
    "ITEM_INDEX": "item",
    "COLLECTION_INDEX": "collection",
}


class STACDocument(Document):
    def __init__(
        self, catalog: str = None, index: str = None, extensions: list = [], **kwargs
    ) -> None:
        super().__init__(**kwargs)
        if catalog and index:
            self.add(catalog, index)
            self.Index(name=index)
        self.extensions = extensions

    class Index:
        name = ""

        def __init__(self, name: str):
            self.name = name

    def get(self, **kwargs):
        return super().get(id=kwargs["id"], index=self.catalogs[kwargs["catalog"]])

    def add(self, catalog, index) -> None:
        self.catalogs[catalog] = index

    def search(self, **kwargs):
        return self._search(**kwargs).execute()

    def count(self, **kwargs):
        return self._search(**kwargs).count()

    def extension_is_enabled(self, extension: str) -> bool:
        """Check if an api extension is enabled."""
        if self.extensions:
            return any((type(ext).__name__ == extension for ext in self.extensions))
        return False

    def get_id(self) -> str:
        return self.meta.id

    def get_stac_extensions(self) -> list:
        """
        Return STAC extensions
        """
        return getattr(self, "stac_extensions", [])

    def get_stac_version(self) -> str:
        """
        Return STAC version
        """
        return getattr(self, "stac_version", STAC_VERSION_DEFAULT)

    def _search(self, catalog: str = None, **kwargs) -> Search:
        """
        Return Elasticsearch DSL Search
        """
        if catalog and catalog in self.catalogs:
            search = super().search(index=self.catalogs[catalog])

        else:
            search = super().search(index=",".join(self.catalogs.values()))

        return self.get_queryset(search, **kwargs)

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
                                    Q(
                                        "range",
                                        properties__datetime={"gte": start_date},
                                    ),
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
                                Q(
                                    "range",
                                    properties__start_datetime={"gte": datetime},
                                ),
                                Q("range", properties__end_datetime={"lte": datetime}),
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

        if self.extension_is_enabled("FilterExtension"):
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
                        HTTPException(
                            status_code=400, detail=f"Invalid filter expression"
                        )
                    )
                else:
                    qs = qs.query(qfilter)

        if self.extension_is_enabled("FreeTextExtension"):
            if q := kwargs.get("q"):
                qs = qs.query(
                    QueryString(query=q, fields=["properties.*"], lenient=True)
                )

        if self.extension_is_enabled("ContextCollectionExtension"):
            if (
                "context_collection" in kwargs
                and kwargs["context_collection"]
                and not collection_ids
            ):
                qs.aggs.bucket("collections", "terms", field="collection_id.keyword")

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


class ElasticsearchAsset(STACDocument):
    type = "Feature"
    extensions: list
    catalogs: dict = {}

    def search(self, **kwargs):
        search = super()._search(catalog=kwargs.get("catalog", None))

        return self.get_queryset(search, **kwargs)

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        return True

    def get_properties(self) -> dict:
        """
        Return properties
        """
        if hasattr(self, "properties"):
            return getattr(self, "properties").to_dict()

        return {}

    def get_bbox(self):
        """
        Return a WGS84 formatted bbox
        """
        try:
            coordinates = rgetattr(self, "spatial.bbox.coordinates")
        except AttributeError:
            return

        return Coordinates.from_geojson(coordinates).to_wgs84()

    def get_item_id(self) -> str:
        """
        Return item id
        """
        return getattr(self, "item_id", None)

    def get_roles(self) -> list:
        """
        Return roles
        """
        return list(getattr(self.get_properties(), "categories", []))

    def get_uri(self) -> list:
        """
        Return uri
        """
        return getattr(self.get_properties(), "uri", "")

    def get_url(self) -> str:
        """
        Convert the path into a url where you can access the asset
        """
        if getattr(self, "media_type", "POSIX") == "POSIX":
            return f"{settings.posix_download_url}{self.get_uri()}"

        return self.get_uri()

    def get_media_type(self) -> str:
        """
        Return media type
        """
        return getattr(self, "media_type", None)

    def to_stac(self) -> dict:
        """
        Convert Elasticsearch DSL asset into a STAC asset.
        """
        properties = getattr(self, "properties", {})

        asset = dict(
            href=self.get_url(),
            type=getattr(properties, "magic_number", None),
            title=getattr(properties, "filename", None),
            roles=self.get_roles(),
        )

        return asset

    def get_links(self, base_url: str, collection_id: str) -> list:
        """
        Returns list of links
        """
        return AssetLinks(
            base_url=str(base_url),
            collection_id=collection_id,
            item_id=self.get_item_id(),
            asset_id=self.get_id(),
        ).create_links()


class ElasticsearchItem(STACDocument):
    type = "Feature"
    extensions: list
    catalogs: dict = {}

    def __init__(
        self, catalog: str = None, index: str = None, extensions: list = [], **kwargs
    ) -> None:
        super().__init__(catalog, index, extensions, **kwargs)

    def get(self, **kwargs):
        item = super().get(**kwargs)

        if item.get_collection_id() != kwargs["collection_id"]:
            raise NotFoundError

        return item

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

    def asset_search(self):
        asset_search = (
            ElasticsearchAsset.search()
            .exclude("term", properties__categories="hidden")
            .filter("exists", field="properties.uri")
            .filter("term", item_id=self.get_id())
        )

        return asset_search

    @property
    def elasticsearch_assets(self) -> list:
        """
        Return elasticsearch assets
        """
        if self.extension_is_enabled("ContextCollectionExtension"):
            return []

        return []  # list(self.asset_search().scan())

    def get_stac_assets(self) -> dict:
        """
        Return stac assets
        """
        return {asset.get_id(): asset.to_stac() for asset in self.elasticsearch_assets}

    def get_properties(self) -> dict:
        """
        Return properties
        """
        properties = getattr(self, "properties", {})

        if not hasattr(self, "datetime"):
            if "start_datetime" not in properties or "end_datetime" not in properties:
                properties["start_datetime"] = None
                properties["end_datetime"] = None

        return properties.to_dict() if not isinstance(properties, dict) else {}

    def get_bbox(self):
        """
        Return a WGS84 formatted bbox

        :return:
        """

        try:
            coordinates = rgetattr(self, "spatial.bbox.coordinates")
        except AttributeError:
            return

        return Coordinates.from_geojson(coordinates).to_wgs84()

    def get_geometry(self):
        ...

    def get_collection_id(self) -> str:
        """
        Return the collection id
        """
        return getattr(self, "collection_id", None)

    def get_links(self, base_url: str) -> list:
        """
        Returns list of links
        """
        links = ItemLinks(
            base_url=str(base_url),
            collection_id=self.get_collection_id(),
            item_id=self.get_id(),
        ).create_links()

        if self.extension_is_enabled("ContextCollectionExtension"):
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


class ElasticsearchCollection(STACDocument):
    """
    Collection class
    """

    type = "FeatureCollection"
    extensions: list
    catalogs: dict = {}

    @classmethod
    def _matches(cls, hit):
        # override _matches to match indices in a pattern instead of just ALIAS
        # hit is the raw dict as returned by elasticsearch
        return True

    def get_summaries(self) -> Optional[dict]:
        """
        Turns the elastic-dsl AttrDict into a dict or None

        """
        properties = getattr(self, "properties", {})

        return properties.to_dict() if not isinstance(properties, dict) else {}

    def get_extent(self) -> dict:
        """
        Takes the elastic-dsl Document and extracts the
        extent information from it.

        """
        extent = getattr(self, "extent", DEFAULT_EXTENT)

        try:
            # Throw away inclusivity flag with _
            lower, _ = extent.temporal.lower
            upper, _ = extent.temporal.upper

            lower = lower.isoformat() if lower else None
            upper = upper.isoformat() if upper else None
        except AttributeError:
            lower, upper = None, None

        try:
            coordinates = Coordinates.from_geojson(extent.spatial.coordinates)
        except AttributeError:
            coordinates = Coordinates.from_wgs84(DEFAULT_EXTENT["spatial"][0])

        return dict(
            temporal=dict(interval=[[lower, upper]]),
            spatial=dict(bbox=[coordinates.to_wgs84()]),
        )

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
            base_url=str(base_url),
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
