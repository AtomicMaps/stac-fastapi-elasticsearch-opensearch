"""Filter Extension."""
from enum import Enum
from typing import List, Type, Union, Dict, Any, Optional

import attr
from fastapi import APIRouter, FastAPI
from starlette.responses import Response

from stac_fastapi.api.models import CollectionUri, EmptyRequest, JSONSchemaResponse
from stac_fastapi.api.routes import create_async_endpoint
# from stac_fastapi.types.core import AsyncBaseFiltersClient, BaseFiltersClient
from stac_fastapi.types.extension import ApiExtension

from .request import AggregationExtensionGetRequest #AggregationExtensionPostRequest
import abc

    
@attr.s
class BaseAggregationClient(abc.ABC):
    """Defines a pattern for implementing the STAC filter extension."""

    def get_aggregations(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the available aggregations over all
        collections.
        """
        return {
            "aggregations": [
                {
                    "name": "total_count",
                    "data_type": "integer"
                }
            ],
            "links": [
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": "https://example.org/v1"
                },
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": "https://example.org/v1/aggregations"
                }
            ]
        }
    
    def aggregate(
            self, collection_id: Optional[str] = None, **kwargs
        ) -> Dict[str, Any]:
        return {
            "aggregations": [],
            "links": [
                {
                "rel": "self",
                "type": "application/json",
                "href": "https://example.org/v1/aggregate"
                },
                {
                "rel": "root",
                "type": "application/json",
                "href": "https://example.org/v1"
                }
            ]
        }

from geojson_pydantic.geometries import Geometry
from stac_pydantic.shared import BBox, MimeTypes
from stac_fastapi.types.rfc3339 import DateTimeType
@attr.s
class AsyncBaseAggregationClient(abc.ABC):
    """Defines a pattern for implementing the STAC aggregation extension."""

    async def get_aggregations(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the aggregations available for the given collection_id.

        If collection_id is None, returns the available aggregations over all
        collections.
        """
        return {
            "aggregations": [
                {
                    "name": "total_count",
                    "data_type": "integer"
                }
            ],
            "links": [
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": "https://example.org"
                },
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": "https://example.org/aggregations"
                }
            ]
        }
    
    async def aggregate(
            self, 
            collection_id: Optional[str] = None,
            aggregations: Optional[Union[str, List[str]]] = None,
            collections: Optional[List[str]] = None,
            ids: Optional[List[str]] = None,
            bbox: Optional[BBox] = None,
            intersects: Optional[Geometry] = None,
            datetime: Optional[DateTimeType] = None,
            limit: Optional[int] = 10,
              **kwargs
        ) -> Dict[str, Any]:
        return {
            "aggregations": [],
            "links": [
                {
                "rel": "self",
                "type": "application/json",
                "href": "https://example.org/aggregate"
                },
                {
                "rel": "root",
                "type": "application/json",
                "href": "https://example.org"
                }
            ]
        }
    
class AggregationConformanceClasses(str, Enum):
    """Conformance classes for the Aggregation extension.

    See
    https://github.com/stac-api-extensions/aggregation
    """

    AGGREGATION = "https://api.stacspec.org/v0.3.0/aggregation"


@attr.s
class AggregationExtension(ApiExtension):
    """Aggregation Extension.

    The purpose of the Aggregation Extension is to provide an endpoint similar to 
    the Search endpoint (/search), but which will provide aggregated information 
    on matching Items rather than the Items themselves. This is highly influenced 
    by the Elasticsearch and OpenSearch aggregation endpoint, but with a more 
    regular structure for responses.

    The Aggregation extension adds several endpoints which allow the retrieval of
    available aggregation fields and aggregation buckets based on a seearch query:
        GET /aggregation
        GET /aggregate
        GET /collections/{collection_id}/aggregations
        GET /collections/{collection_id}/aggregate

    https://github.com/stac-api-extensions/aggregation/blob/main/README.md

    Attributes:
        conformance_classes: Conformance classes provided by the extension
    """

    GET = AggregationExtensionGetRequest
    # POST = AggregationExtensionPostRequest

    client: Union[AsyncBaseAggregationClient, BaseAggregationClient] = attr.ib(
        factory=BaseAggregationClient
    )

    conformance_classes: List[str] = attr.ib(
        default=[
            AggregationConformanceClasses.AGGREGATION
        ]
    )
    router: APIRouter = attr.ib(factory=APIRouter)
    response_class: Type[Response] = attr.ib(default=JSONSchemaResponse)

    def register(self, app: FastAPI) -> None:
        """Register the extension with a FastAPI application.

        Args:
            app: target FastAPI application.

        Returns:
            None
        """
        self.router.prefix = app.state.router_prefix
        self.router.add_api_route(
            name="Aggregations",
            path="/aggregations",
            methods=["GET", "POST"],
            endpoint=create_async_endpoint(
                self.client.get_aggregations, EmptyRequest, self.GET
            ),
        )
        self.router.add_api_route(
            name="Collection Aggregations",
            path="/collections/{collection_id}/aggregations",
            methods=["GET", "POST"],
            endpoint=create_async_endpoint(
                self.client.get_aggregations, CollectionUri, self.GET
            ),
        )
        self.router.add_api_route(
            name="Aggregate",
            path="/aggregate",
            methods=["GET", "POST"],
            endpoint=create_async_endpoint(
                self.client.aggregate, self.GET, self.GET
            ),
        )
        self.router.add_api_route(
            name="Collection Aggregate",
            path="/collections/{collection_id}/aggregate",
            methods=["GET", "POST"],
            endpoint=create_async_endpoint(
                self.client.aggregate, self.GET, self.GET
            ),
        )
        app.include_router(self.router, tags=["Aggregation Extension"])