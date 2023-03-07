# encoding: utf-8
"""

"""
__author__ = "Richard Smith"
__date__ = "12 Nov 2021"
__copyright__ = "Copyright 2018 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"
__contact__ = "richard.d.smith@stfc.ac.uk"

import argparse
import json
import os
from pathlib import Path

from elasticsearch import Elasticsearch

workingdir = Path(__file__).parent.absolute()
data_dir = workingdir.parent / "stac_fastapi" / "test_data"


def parse_args():
    parser = argparse.ArgumentParser(description="Load test data into elasticsearch")
    parser.add_argument(
        "--host", help="Elasticsearch host and port", default="database:9200"
    )

    return parser.parse_args()


def read_json(path, object_type):
    with open(os.path.join(path, f"{object_type}s.json"), encoding="utf-8") as reader:
        return json.load(reader)


def load_mappings(path, es_host, object_types):

    for object_type in object_types:
        map = read_json(path, f"{object_type}s.json")

        index_name = f"stac-{object_type}s"
        if not es_host.indices.exists(index_name):
            es_host.indices.create(index_name, body=map)


def load_data(path, es_host, object_types):

    for object_type in object_types:
        data = read_json(path, f"{object_type}s.json")

        for item in data:
            es_host.index(
                index=f"stac-{object_type}s", id=item["_id"], body=item["_source"]
            )


def main():

    args = parse_args()

    object_types = ["asset", "item", "collection"]
    es = Elasticsearch(args.host)
    load_mappings(os.path.join(data_dir, "mappings"), es, object_types)

    collections = os.listdir(os.path.join(data_dir, "collections"))
    for collection in collections:
        path = os.path.join(data_dir, "collections", collection)
        load_data(path, es, object_types)


if __name__ == "__main__":
    main()
