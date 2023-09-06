# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "14 Jun 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

from itertools import chain

from elasticsearch import NotFoundError

# Third-party imports
from importlib_metadata import entry_points

DEFAULT_MODELS = {
    "ASSET_INDEX": "asset",
    "ITEM_INDEX": "item",
    "COLLECTION_INDEX": "collection",
}


class SearchMiddleware:
    MODEL_KEY: str

    def __init__(self, catalogs: dict, extensions: list = [], **kwargs) -> None:
        # for each database type create a instance
        # create a CATALOGS mapping
        self.catalogs_map = {}
        self.database_entry_points = {}
        self.database_models = {}

        for entry_point in entry_points(group="stac_fastapi.elasticsearch.database"):
            self.database_entry_points[entry_point.name] = entry_point.load()

        self.load_catalogs(catalogs, extensions, **kwargs)

    def load_catalogs(self, conf: dict, extensions: list, **kwargs):
        for key, conf in reversed(list(conf.items())):
            if key in DEFAULT_MODELS and key == self.MODEL_KEY:
                self.load_catalog("default", conf, extensions, **kwargs)
            elif isinstance(conf, dict):
                for cat_key, cat_conf in conf.items():
                    if cat_key == self.MODEL_KEY:
                        self.load_catalog(key, cat_conf, extensions, **kwargs)

    def load_catalog(self, catalog: str, conf: dict, extensions: list, **kwargs):
        index = conf["name"] if isinstance(conf, dict) else conf
        model = (
            conf["model"]
            if isinstance(conf, dict) and "model" in conf
            else DEFAULT_MODELS[self.MODEL_KEY]
        )

        self.catalogs_map[catalog] = model

        if model in self.database_models:
            self.database_models[model].add(catalog, index)

        else:
            self.database_models[model] = self.database_entry_points[model](
                catalog=catalog, index=index, extensions=extensions, **kwargs
            )

    def search(self, **kwargs):
        # check CATALOGS mapping
        if "catalog" in kwargs and kwargs["catalog"] in self.catalogs_map:
            database = self.database_models[self.catalogs_map[kwargs["catalog"]]]

            return database.search(**kwargs)

        else:
            limit = (
                int(kwargs["limit"]) if "limit" in kwargs and kwargs["limit"] else 10000
            )
            total_count = 0
            total_items = iter(())
            for database in self.database_models.values():
                count_kwargs = kwargs
                count_kwargs.update({"database": database})
                count = self.count(**count_kwargs)

                if count > 0:
                    kwargs["limit"] = limit
                    total_items = chain(total_items, database.search(**kwargs))
                    limit -= count
                    total_count += count

                if total_count >= limit:
                    break

            return total_items

    def count(self, **kwargs):
        if "database" in kwargs:
            return kwargs["database"].count(**kwargs)

        if "catalog" in kwargs and kwargs["catalog"] in self.catalogs_map:
            database = self.database_models[self.catalogs_map[kwargs["catalog"]]]
            return database.count(**kwargs)

        else:
            total_count = 0
            for database in self.database_models.values():
                count = database.count(**kwargs)
                total_count += count

            return total_count

    def get(self, **kwargs):
        # check CATALOGS mapping
        # call search for each

        if "catalog" in kwargs and kwargs["catalog"] in self.catalogs_map:
            try:
                database_key = self.catalogs_map[kwargs["catalog"]]
                database = self.database_models[database_key]

                return database.get(**kwargs)

            except NotFoundError as exc:
                raise NotFoundError from exc

        else:
            for catalog, database_key in self.catalogs_map.items():
                try:
                    kwargs["catalog"] = catalog
                    database = self.database_models[database_key]
                    result = database.get(**kwargs)

                    if result:
                        return result
                except:
                    pass

            raise NotFoundError


class CollectionSearchMiddleware(SearchMiddleware):
    MODEL_KEY: str = "COLLECTION_INDEX"


class ItemSearchMiddleware(SearchMiddleware):
    MODEL_KEY: str = "ITEM_INDEX"


class AssetSearchMiddleware(SearchMiddleware):
    MODEL_KEY: str = "ASSET_INDEX"
