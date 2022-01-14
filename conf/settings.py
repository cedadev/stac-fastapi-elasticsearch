# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '12 Nov 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from pathlib import Path

from flexi_settings import include, include_dir


base_dir = Path(__file__).resolve().parent

# First, include the defaults
include(base_dir / "defaults.py")
include_dir(base_dir / 'settings.d')
