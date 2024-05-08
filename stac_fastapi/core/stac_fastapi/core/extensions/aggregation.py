"""Request model for the Aggregation extension."""

from typing import Optional, Union, List
from stac_fastapi.types.search import APIRequest
import attr
from stac_pydantic.shared import BBox
from stac_fastapi.types.search import APIRequest, str2bbox
from stac_fastapi.types.rfc3339 import DateTimeType, str_to_interval

def str2list(x: str) -> Optional[List]:
    """Convert string to list base on , delimiter."""
    if x:
        return x.split(",")


def str2bbox(x: str) -> Optional[BBox]:
    """Convert string to BBox based on , delimiter."""
    if x:
        t = tuple(float(v) for v in str2list(x))
        assert len(t) == 4
        return t

@attr.s
class AggregationExtensionGetRequest(APIRequest):
    """Query Extension GET request model."""

    aggregations: Optional[Union[str, List[str]]] = attr.ib(default=None)
    collections: Optional[str] = attr.ib(default=None, converter=str2list)
    ids: Optional[str] = attr.ib(default=None, converter=str2list)
    bbox: Optional[BBox] = attr.ib(default=None, converter=str2bbox)
    intersects: Optional[str] = attr.ib(default=None)
    datetime: Optional[DateTimeType] = attr.ib(default=None, converter=str_to_interval)
    limit: Optional[int] = attr.ib(default=10)
    filter: Optional[str] = attr.ib(default=None)


# class AggregationExtensionPostRequest(BaseModel):
#     """Query Extension POST request model."""

#     bbox: Optional[BBox]
#     datetime: Optional[DateTimeType]
#     limit: Optional[Limit] = Field(default=10)
#     collections: Optional[Any] = attr.ib(default=None)
#     ids: Optional[Any] = attr.ib(default=None)
#     intersects: Optional[str] = attr.ib(default=None)
#     query: Optional[str] = attr.ib(default=None)
