# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '19 Aug 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from stac_fastapi.elasticsearch.types.context import ResultContext
from typing import Optional


def generate_context(limit: int, result_count: int, page: Optional[int]) -> ResultContext:
    """Generate context"""

    # Default page to 1
    page = page or 1

    returned = limit if page * limit <= result_count else result_count - (page - 1) * limit

    return ResultContext(
        returned=int(returned),
        limit=int(limit),
        matched=int(result_count),
    )
