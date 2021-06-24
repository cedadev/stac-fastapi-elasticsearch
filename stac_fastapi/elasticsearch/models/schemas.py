# encoding: utf-8
"""

"""
__author__ = 'Richard Smith'
__date__ = '16 Jun 2021'
__copyright__ = 'Copyright 2018 United Kingdom Research and Innovation'
__license__ = 'BSD - see LICENSE file in top-level package directory'
__contact__ = 'richard.d.smith@stfc.ac.uk'

"""API pydantic models."""

from typing import List, Optional, Union

from pydantic import BaseModel
from stac_pydantic import Collection as CollectionBase
from stac_pydantic import Item as ItemBase
from stac_pydantic.links import Link

from stac_fastapi.elasticsearch.models.decompose import CollectionGetter, ItemGetter

# Be careful: https://github.com/samuelcolvin/pydantic/issues/1423#issuecomment-642797287
NumType = Union[float, int]


class Collection(CollectionBase):
    """Collection model."""

    links: Optional[List[Link]]

    class Config:
        """Model config."""

        orm_mode = True
        use_enum_values = True
        getter_dict = CollectionGetter


class Item(ItemBase):
    """Item model."""

    class Config:
        """Model config."""

        orm_mode = True
        use_enum_values = True
        getter_dict = ItemGetter


class Items(BaseModel):
    """Items model."""

    items: List[Item]
