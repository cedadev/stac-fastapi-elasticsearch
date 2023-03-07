# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "14 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from typing import Optional
from urllib.parse import urljoin

from elasticsearch_dsl import DateRange, Document, GeoShape, Index, InnerDoc
from stac_fastapi.types.links import CollectionLinks, ItemLinks
from stac_fastapi_asset_search.types import AssetLinks
from stac_pydantic.shared import MimeTypes

from stac_fastapi.elasticsearch import app
from stac_fastapi.elasticsearch.config import settings

from .utils import Coordinates, rgetattr

DEFAULT_EXTENT = {"temporal": [[None, None]], "spatial": [[-180, -90, 180, 90]]}
STAC_VERSION_DEFAULT = "1.0.0"

collections = Index(settings.COLLECTION_INDEX)
items = Index(settings.ITEM_INDEX)
assets = Index(settings.ASSET_INDEX)


class Extent(InnerDoc):

    temporal = DateRange()
    spatial = GeoShape()


class STACDocument(Document):

    extensions: list

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.extensions = app.extensions

    def extension_is_enabled(self, extension: str) -> bool:
        """Check if an api extension is enabled."""
        if self.extensions:
            return any((type(ext).__name__ == extension for ext in self.extensions))
        return False

    def get_stac_extensions(self) -> list:
        """
        Return STAC extensions
        """
        return getattr(self, "stac_extensions", [])

    def get_stac_version(self) -> list:
        """
        Return STAC extensions
        """
        return getattr(self, "stac_version", STAC_VERSION_DEFAULT)


@assets.document
class ElasticsearchAsset(STACDocument):

    type = "Feature"

    @classmethod
    def search(cls, **kwargs):
        return (
            super().search(**kwargs)
            # .exclude("term", properties__categories__keyword="hidden")
            # .filter("exists", field="filepath_type_location")
        )

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
            base_url=base_url,
            collection_id=collection_id,
            item_id=self.get_item_id(),
            asset_id=self.meta.id,
        ).create_links()


@items.document
class ElasticsearchItem(STACDocument):
    type = "Feature"

    @classmethod
    def search(cls, **kwargs):
        return super().search(**kwargs).filter("term", type="item")

    # @classmethod
    # def _matches(cls, hit):
    #     # override _matches to match indices in a pattern instead of just ALIAS
    #     # hit is the raw dict as returned by elasticsearch
    #     return True

    def asset_search(self):
        asset_search = (
            ElasticsearchAsset.search()
            .exclude("term", properties__categories="hidden")
            .filter("exists", field="properties.uri")
            .filter("term", item_id=self.meta.id)
        )

        return asset_search

    @property
    def elasticsearch_assets(self) -> list:
        """
        Return elasticsearch assets
        """
        if self.extension_is_enabled("ContextCollectionExtension"):
            return []

        return list(self.asset_search().scan())

    def get_stac_assets(self) -> dict:
        """
        Return stac assets
        """
        return {asset.meta.id: asset.to_stac() for asset in self.elasticsearch_assets}

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

    def get_links(self, base_url) -> list:
        """
        Returns list of links
        """
        links = ItemLinks(
            base_url=base_url,
            collection_id=self.collection_id,
            item_id=self.meta.id,
        ).create_links()

        if self.extension_is_enabled("ContextCollectionExtension"):
            links.append(
                dict(
                    rel="assets",
                    type=MimeTypes.json,
                    href=urljoin(
                        base_url,
                        f"collections/{self.collection_id}/items/{self.meta.id}/assets",
                    ),
                )
            )

        return links


@collections.document
class ElasticsearchCollection(STACDocument):
    """
    Collection class
    """

    type = "Collection"

    @classmethod
    def search(cls, **kwargs):
        return super().search(**kwargs).filter("term", type="collection")

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

    def get_links(self, base_url) -> list:
        """
        Returns list of links
        """
        collection_links = CollectionLinks(
            base_url=base_url,
            collection_id=self.meta.id,
        ).create_links()

        if self.extension_is_enabled("FilterExtension"):
            collection_links.append(
                {
                    "rel": "https://www.opengis.net/def/rel/ogc/1.0/queryables",
                    "type": MimeTypes.json,
                    "href": urljoin(
                        base_url,
                        f"collections/{self.meta.id}/queryables",
                    ),
                }
            )

        return collection_links
