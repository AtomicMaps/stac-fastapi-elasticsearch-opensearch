"""FastAPI application."""

import os

from stac_fastapi.api.app import StacApi
from stac_fastapi.api.models import create_get_request_model, create_post_request_model
from stac_fastapi.core.basic_auth import apply_basic_auth
from stac_fastapi.core.core import (
    BulkTransactionsClient,
    CoreClient,
    EsAsyncBaseFiltersClient,
    TransactionsClient,
    EsAsyncAggregationClient
)
from stac_fastapi.core.extensions import QueryExtension
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.core import (
    FieldsExtension,
    FilterExtension,
    SortExtension,
    TokenPaginationExtension,
    TransactionExtension,
)
from stac_fastapi.extensions.third_party import BulkTransactionExtension
from stac_fastapi.opensearch.config import OpensearchSettings
from stac_fastapi.opensearch.database_logic import (
    DatabaseLogic,
    create_collection_index,
    create_index_templates,
)

from .aggregation.aggregation import AggregationExtension
from .aggregation.request import AggregationExtensionGetRequest #, AggregationExtensionPostRequest
import attr
from typing import Optional, Any, Dict, Optional, Union, List, Tuple

@attr.s
class OpenSearchAggregationExtensionGetRequest(AggregationExtensionGetRequest):
    """Add implementation specific query parameters to AggregationExtensionGetRequest for aggrgeation precision."""
    grid_geohex_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geohash_frequency_precision: Optional[int] = attr.ib(default=None)
    grid_geotile_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geohex_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    centroid_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geohash_grid_frequency_precision: Optional[int] = attr.ib(default=None)
    geometry_geotile_grid_frequency_precision: Optional[int] = attr.ib(default=None)


settings = OpensearchSettings()
session = Session.create_from_settings(settings)

filter_extension = FilterExtension(client=EsAsyncBaseFiltersClient())
filter_extension.conformance_classes.append(
    "http://www.opengis.net/spec/cql2/1.0/conf/advanced-comparison-operators"
)

database_logic = DatabaseLogic()

aggregation_extension = AggregationExtension(
        client=EsAsyncAggregationClient(
            database=database_logic, session=session, settings=settings
            )
        )

aggregation_extension.GET = OpenSearchAggregationExtensionGetRequest
# aggregation_extension.POST = OpenSearchAggregationExtensionPostRequest

search_extensions = [
    TransactionExtension(
        client=TransactionsClient(
            database=database_logic, session=session, settings=settings
        ),
        settings=settings,
    ),
    BulkTransactionExtension(
        client=BulkTransactionsClient(
            database=database_logic,
            session=session,
            settings=settings,
        )
    ),
    FieldsExtension(),
    QueryExtension(),
    SortExtension(),
    TokenPaginationExtension(),
    filter_extension
]

api_extensions = [
    aggregation_extension
] + search_extensions

 
# print("EXTENSIONS: ", api_extensions)
post_request_model = create_post_request_model(search_extensions)

api = StacApi(
    title=os.getenv("STAC_FASTAPI_TITLE", "stac-fastapi-opensearch"),
    description=os.getenv("STAC_FASTAPI_DESCRIPTION", "stac-fastapi-opensearch"),
    api_version=os.getenv("STAC_FASTAPI_VERSION", "2.1"),
    settings=settings,
    extensions=api_extensions,
    client=CoreClient(
        database=database_logic, session=session, post_request_model=post_request_model
    ),
    search_get_request_model=create_get_request_model(search_extensions),
    search_post_request_model=post_request_model,
)
app = api.app
app.root_path = os.getenv("STAC_FASTAPI_ROOT_PATH", "")

apply_basic_auth(api)


@app.on_event("startup")
async def _startup_event() -> None:
    await create_index_templates()
    await create_collection_index()


def run() -> None:
    """Run app from command line using uvicorn if available."""
    try:
        import uvicorn

        uvicorn.run(
            "stac_fastapi.opensearch.app:app",
            host=settings.app_host,
            port=settings.app_port,
            log_level="info",
            reload=settings.reload,
        )
    except ImportError:
        raise RuntimeError("Uvicorn must be installed in order to use command")


if __name__ == "__main__":
    run()


def create_handler(app):
    """Create a handler to use with AWS Lambda if mangum available."""
    try:
        from mangum import Mangum

        return Mangum(app)
    except ImportError:
        return None


handler = create_handler(app)
