
import argparse
import json
import os
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch_dsl import connections, Search
import yaml


workingdir = Path(__file__).parent.absolute()
data_dir = workingdir.parent / "stac_fastapi" / "test_data"

def get_paths_list():
    paths = []

    with open('paths.txt') as paths_file:
        paths = [p.strip() for p in paths_file]

    return paths

def get_config():
    with open('.fastapi.yml', encoding='utf-8') as reader:
        conf = yaml.safe_load(reader)
    
    es_conf = conf.get("ELASTICSEARCH")

    return es_conf

def get_query_from_path(path):
    return Search(index='ceda-assets-atod').using('es').query('prefix', properties__uri={'value': path})

def get_query_from_item_id(item_id):
    return Search(index='ceda-items-atod').using('es').query('match', item_id=item_id)

def get_query_from_col_id(col_id):
    return Search(index='ceda-collections-atod').using('es').query('match', _id=col_id)

def get_results_list(query_method, ids):
    results = []

    for i in ids:
        s = query_method(i)
        response = s.execute()
        results += [h.to_dict() for h in response.hits]

    return results



def main():
    es_conf = get_config()
    connections.create_connection(alias='es', **es_conf.get("SESSION_KWARGS"))
 
    paths = get_paths_list()
    assets = get_results_list(get_query_from_path, paths)
    with open(data_dir / 'assets.json', 'w') as f:
        json.dump(assets, f)
    
    item_ids = set([a['item_id'] for a in assets])
    items = get_results_list(get_query_from_item_id, item_ids)
    with open(data_dir / 'items.json', 'w') as f:
        json.dump(items, f)

    collection_ids = set([i['collection_id'] for i in items])
    collections = get_results_list(get_query_from_col_id, collection_ids)
    with open(data_dir / 'collections.json', 'w') as f:
        json.dump(collections, f)
    
    



if __name__ == "__main__":
    main()