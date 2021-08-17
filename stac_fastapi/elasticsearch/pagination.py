# encoding: utf-8
"""
Pagination classes and functions.
"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from typing import List, Dict
from urllib.parse import urljoin
from stac_pydantic.links import Relations


def generate_pagination_links(request) -> List[Dict]:
    """Generate page base pagination links."""

    link_url = urljoin(str(request.base_url), request.url.path) + '?'
    page = int(request.query_params.get('page', 1))

    for key, value in request.query_params.items():
        if key == 'page':
            continue
        link_url += f"{key}={value}&"

    links = [
        {
            'rel': Relations.next,
            'href': f'{link_url}page={page + 1}'
        },
        {
            'rel': Relations.self,
            'href': f'{link_url}page={page}'
        }
    ]
    if page != 1:
        links.append(
            {
                'rel': Relations.previous,
                'href': f'{link_url}page={page - 1}'
            }
        )
    return links