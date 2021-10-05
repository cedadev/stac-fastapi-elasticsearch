from pydantic import BaseModel, AnyUrl, Field, constr
from typing import List, Dict, Optional, Union, Any
from stac_pydantic.version import STAC_VERSION
from stac_pydantic.collection import Extent, Range
from stac_pydantic.item import ItemProperties
from stac_pydantic.shared import BBox, Asset, Provider


class ItemValidator(BaseModel):
    """
    https://github.com/radiantearth/stac-spec/blob/v1.0.0/item-spec/item-spec.md
    """

    type: constr(min_length=1) = Field("Feature", const=True)
    stac_version: constr(min_length=1) = Field(STAC_VERSION, const=True)
    stac_extensions: Optional[List[AnyUrl]]
    properties: ItemProperties
    collection: str
    assets: Optional[Dict[str, Asset]]
    bbox: Optional[BBox]


class CollectionValidator(BaseModel):
    """
    https://github.com/radiantearth/stac-spec/blob/v1.0.0/collection-spec/collection-spec.md
    """

    type: constr(min_length=1) = Field("Collection", const=True)
    stac_version: constr(min_length=1) = Field(STAC_VERSION, const=True)
    stac_extensions: Optional[List[AnyUrl]]
    description: constr(min_length=1)
    title: Optional[str]
    keywords: Optional[List[str]]
    providers: Optional[List[Provider]]
    summaries: Optional[Dict[str, Union[Range, List[Any], Dict[str, Any]]]]
