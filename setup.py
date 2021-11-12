# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '11 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

from setuptools import setup, find_namespace_packages

with open("README.md") as readme_file:
    _long_description = readme_file.read()

setup(
    name='stac_fastapi.elasticsearch',
    description='Elasticsearch backend for stac-fastapi',
    author='Richard Smith',
    url='https://github.com/cedadev/stac-fastapi-elasticsearch/',
    long_description=_long_description,
    long_description_content_type='text/markdown',
    license='BSD - See asset_extractor/LICENSE file for details',
    packages=find_namespace_packages(),
    python_requires='>=3.5',
    package_data={
        'stac_fastapi_elasticsearch': [
            'LICENSE'
        ]
    },
    install_requires=[
        'attrs',
        'fastapi',
        'stac-fastapi.api',
        'stac-fastapi.types',
        'stac-fastapi.extensions',
        'stac-pydantic',
        'elasticsearch-dsl'
    ],
    extras_require={
        'server': ["uvicorn[standard]>=0.12.0,<0.14.0"]
    },
    entry_points={
    }
)