"""Request model for the Aggregation extension."""

from typing import Optional, Union, List
from stac_fastapi.types.search import APIRequest
import attr
from stac_fastapi.types.search import BaseSearchGetRequest, BaseSearchPostRequest
from stac_fastapi.extensions.core.filter.request import FilterExtensionGetRequest, FilterExtensionPostRequest


@attr.s
class AggregationExtensionGetRequest(BaseSearchGetRequest, FilterExtensionGetRequest):
    """Query Extension GET request model."""

    aggregations: Optional[Union[str, List[str]]] = attr.ib(default=None)

class AggregationExtensionPostRequest(BaseSearchPostRequest, FilterExtensionPostRequest):
    """Query Extension POST request model."""

    aggregations: Optional[str] = attr.ib(default=None)
    


