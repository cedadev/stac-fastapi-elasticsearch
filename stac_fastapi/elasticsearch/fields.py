# encoding: utf-8
"""
Pagination classes and functions.
"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from typing import Dict
from urllib.parse import urljoin
from stac_pydantic.links import Relations


def generate_fields_dict(fields) -> Dict:
    """Generate page base pagination links."""

    fields_dict = {
            "includes": [],
            "excludes": []
        }

    if isinstance(fields, list):
        for field in fields:
            if field.startswith('-'):
                fields_dict['excludes'].append(field.lstrip('-'))
            else:
                fields_dict['includes'].append(field.lstrip('+'))

    if isinstance(fields, dict):
        fields_dict['includes'] = list(fields['include'])
        fields_dict['excludes'] = list(fields['exclude'])

    return fields_dict
