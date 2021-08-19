# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '19 Aug 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'


def generate_context(limit, result_count, page):
    return {
        'returned': limit if page * limit <= result_count - 1 else (result_count - 1) - (page - 1) * limit,
        'limit': limit,
        'result_count': result_count,
    }
