# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '12 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

import os

ELASTICSEARCH_CONNECTION = {
    'hosts': ['database:9200']
}

COLLECTION_INDEX = 'stac-collections'
ITEM_INDEX = 'stac-items'
ASSET_INDEX = 'stac-assets'

STAC_DESCRIPTION='STAC API Elasticsearch'
STAC_TITLE='STAC API Elasticsearch'

APP_HOST = os.environ.get('APP_HOST', '0.0.0.0')
APP_PORT = int(os.environ.get('APP_PORT',8080))
enable_response_models = True