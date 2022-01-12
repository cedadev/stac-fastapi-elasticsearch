# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '12 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import argparse
import os
from pathlib import Path
import json
from elasticsearch import Elasticsearch


workingdir = Path(__file__).parent.absolute()
data_dir = workingdir.parent / "stac_fastapi" / "test_data"


def parse_args():
    parser = argparse.ArgumentParser(description='Load test data into elasticsearch')
    parser.add_argument('--host', help='Elasticsearch host and port', default='database:9200')

    return parser.parse_args()


def load_mappings(path, host):

    for object_type in ['asset', 'item', 'collection']:
        with open(os.path.join(path, f'{object_type}_mapping.json')) as reader:
            map = json.load(reader)
            es = Elasticsearch(host)

            index_name = f'stac-{object_type}s'
            if not es.indices.exists(index_name):
                es.indices.create(index_name, body=map)


def load_data(collection, object_type, host):
    path = os.path.join(collection, f'{object_type}s.json')
    with open(path) as reader:
        data = json.load(reader)

    es = Elasticsearch(host)

    for item in data:
        id = item['_id']
        source = item['_source']
        es.index(index=f'stac-{object_type}s', id=id, body=source)


def main():

    args = parse_args()

    load_mappings(os.path.join(data_dir,'mappings'), args.host)

    collections = os.listdir(os.path.join(data_dir, 'collections'))
    for collection in collections:
        path = os.path.join(data_dir, 'collections', collection)
        load_data(path, 'asset', args.host)
        load_data(path, 'item', args.host)
        load_data(path, 'collection', args.host)




if __name__ == '__main__':
    main()