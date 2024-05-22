"""Core client."""
import logging
import re
import json
from datetime import datetime as datetime_type
from datetime import timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Type, Union
from urllib.parse import unquote_plus, urljoin

import attr
import orjson
import stac_pydantic
from fastapi import HTTPException, Request
from overrides import overrides
from pydantic import ValidationError
from pygeofilter.backends.cql2_json import to_cql2
from pygeofilter.parsers.cql2_text import parse as parse_cql2_text
from stac_pydantic import Collection, Item, ItemCollection
from stac_pydantic.links import Relations
from stac_pydantic.shared import BBox, MimeTypes
from stac_pydantic.version import STAC_VERSION

from stac_fastapi.core.base_database_logic import BaseDatabaseLogic
from stac_fastapi.core.base_settings import ApiBaseSettings
from stac_fastapi.core.models.links import PagingLinks
from stac_fastapi.core.serializers import CollectionSerializer, ItemSerializer
from stac_fastapi.core.session import Session
from stac_fastapi.extensions.third_party.bulk_transactions import (
    BaseBulkTransactionsClient,
    BulkTransactionMethod,
    Items,
)
from stac_fastapi.types import stac as stac_types
from stac_fastapi.types.config import Settings
from stac_fastapi.types.conformance import BASE_CONFORMANCE_CLASSES
from stac_fastapi.types.core import (
    AsyncBaseCoreClient,
    AsyncBaseFiltersClient,
    AsyncBaseTransactionsClient,
)
from stac_fastapi.types.extension import ApiExtension
from stac_fastapi.types.requests import get_base_url
from stac_fastapi.types.rfc3339 import DateTimeType
from stac_fastapi.types.search import BaseSearchPostRequest

logger = logging.getLogger(__name__)

NumType = Union[float, int]


@attr.s
class CoreClient(AsyncBaseCoreClient):
    """Client for core endpoints defined by the STAC specification.

    This class is a implementation of `AsyncBaseCoreClient` that implements the core endpoints
    defined by the STAC specification. It uses the `DatabaseLogic` class to interact with the
    database, and `ItemSerializer` and `CollectionSerializer` to convert between STAC objects and
    database records.

    Attributes:
        session (Session): A requests session instance to be used for all HTTP requests.
        item_serializer (Type[serializers.ItemSerializer]): A serializer class to be used to convert
            between STAC items and database records.
        collection_serializer (Type[serializers.CollectionSerializer]): A serializer class to be
            used to convert between STAC collections and database records.
        database (DatabaseLogic): An instance of the `DatabaseLogic` class that is used to interact
            with the database.
    """

    database: BaseDatabaseLogic = attr.ib()
    base_conformance_classes: List[str] = attr.ib(
        factory=lambda: BASE_CONFORMANCE_CLASSES
    )
    extensions: List[ApiExtension] = attr.ib(default=attr.Factory(list))

    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))
    item_serializer: Type[ItemSerializer] = attr.ib(default=ItemSerializer)
    collection_serializer: Type[CollectionSerializer] = attr.ib(
        default=CollectionSerializer
    )
    post_request_model = attr.ib(default=BaseSearchPostRequest)
    stac_version: str = attr.ib(default=STAC_VERSION)
    landing_page_id: str = attr.ib(default="stac-fastapi")
    title: str = attr.ib(default="stac-fastapi")
    description: str = attr.ib(default="stac-fastapi")

    def _landing_page(
        self,
        base_url: str,
        conformance_classes: List[str],
        extension_schemas: List[str],
    ) -> stac_types.LandingPage:
        landing_page = stac_types.LandingPage(
            type="Catalog",
            id=self.landing_page_id,
            title=self.title,
            description=self.description,
            stac_version=self.stac_version,
            conformsTo=conformance_classes,
            links=[
                {
                    "rel": Relations.self.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": Relations.root.value,
                    "type": MimeTypes.json,
                    "href": base_url,
                },
                {
                    "rel": "data",
                    "type": MimeTypes.json,
                    "href": urljoin(base_url, "collections"),
                },
                {
                    "rel": Relations.conformance.value,
                    "type": MimeTypes.json,
                    "title": "STAC/WFS3 conformance classes implemented by this server",
                    "href": urljoin(base_url, "conformance"),
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "GET",
                },
                {
                    "rel": Relations.search.value,
                    "type": MimeTypes.geojson,
                    "title": "STAC search",
                    "href": urljoin(base_url, "search"),
                    "method": "POST",
                }
            ],
            stac_extensions=extension_schemas,
        )
        return landing_page

    async def landing_page(self, **kwargs) -> stac_types.LandingPage:
        """Landing page.

        Called with `GET /`.

        Returns:
            API landing page, serving as an entry point to the API.
        """
        request: Request = kwargs["request"]
        base_url = get_base_url(request)
        landing_page = self._landing_page(
            base_url=base_url,
            conformance_classes=self.conformance_classes(),
            extension_schemas=[],
        )

        if self.extension_is_enabled("FilterExtension"):
            landing_page["links"].append(
                {
                    # TODO: replace this with Relations.queryables.value,
                    "rel": "http://www.opengis.net/def/rel/ogc/1.0/queryables",
                    # TODO: replace this with MimeTypes.jsonschema,
                    "type": "application/schema+json",
                    "title": "Queryables",
                    "href": urljoin(base_url, "queryables")
                }
            )

        # Add Aggregation links
        if self.extension_is_enabled("AggregationExtension"):
            landing_page["links"].extend(
                [
                    {
                        "rel": "aggregate",
                        "type": "application/json",
                        "title": "Aggregate",
                        "href": urljoin(base_url, "aggregate"),
                    },
                    {
                        "rel": "aggregations",
                        "type": "application/json",
                        "title": "Aggregations",
                        "href": urljoin(base_url, "aggregations"),
                    },
                ]
            )
        collections = await self.all_collections(request=kwargs["request"])
        for collection in collections["collections"]:
            landing_page["links"].append(
                {
                    "rel": Relations.child.value,
                    "type": MimeTypes.json.value,
                    "title": collection.get("title") or collection.get("id"),
                    "href": urljoin(base_url, f"collections/{collection['id']}"),
                }
            )

        # Add OpenAPI URL
        landing_page["links"].append(
            {
                "rel": "service-desc",
                "type": "application/vnd.oai.openapi+json;version=3.0",
                "title": "OpenAPI service description",
                "href": urljoin(
                    str(request.base_url), request.app.openapi_url.lstrip("/")
                ),
            }
        )

        # Add human readable service-doc
        landing_page["links"].append(
            {
                "rel": "service-doc",
                "type": "text/html",
                "title": "OpenAPI service documentation",
                "href": urljoin(
                    str(request.base_url), request.app.docs_url.lstrip("/")
                ),
            }
        )

        return landing_page

    async def all_collections(self, **kwargs) -> stac_types.Collections:
        """Read all collections from the database.

        Args:
            **kwargs: Keyword arguments from the request.

        Returns:
            A Collections object containing all the collections in the database and links to various resources.
        """
        request = kwargs["request"]
        base_url = str(request.base_url)
        limit = int(request.query_params.get("limit", 10))
        token = request.query_params.get("token")

        collections, next_token = await self.database.get_all_collections(
            token=token, limit=limit, base_url=base_url
        )

        links = [
            {"rel": Relations.root.value, "type": MimeTypes.json, "href": base_url},
            {"rel": Relations.parent.value, "type": MimeTypes.json, "href": base_url},
            {
                "rel": Relations.self.value,
                "type": MimeTypes.json,
                "href": urljoin(base_url, "collections"),
            },
        ]

        if next_token:
            next_link = PagingLinks(next=next_token, request=request).link_next()
            links.append(next_link)

        return stac_types.Collections(collections=collections, links=links)

    async def get_collection(
        self, collection_id: str, **kwargs
    ) -> stac_types.Collection:
        """Get a collection from the database by its id.

        Args:
            collection_id (str): The id of the collection to retrieve.
            kwargs: Additional keyword arguments passed to the API call.

        Returns:
            Collection: A `Collection` object representing the requested collection.

        Raises:
            NotFoundError: If the collection with the given id cannot be found in the database.
        """
        base_url = str(kwargs["request"].base_url)
        collection = await self.database.find_collection(collection_id=collection_id)
        return self.collection_serializer.db_to_stac(
            collection=collection, base_url=base_url
        )

    async def item_collection(
        self,
        collection_id: str,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: int = 10,
        token: str = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Read items from a specific collection in the database.

        Args:
            collection_id (str): The identifier of the collection to read items from.
            bbox (Optional[BBox]): The bounding box to filter items by.
            datetime (Optional[DateTimeType]): The datetime range to filter items by.
            limit (int): The maximum number of items to return. The default value is 10.
            token (str): A token used for pagination.
            request (Request): The incoming request.

        Returns:
            ItemCollection: An `ItemCollection` object containing the items from the specified collection that meet
                the filter criteria and links to various resources.

        Raises:
            HTTPException: If the specified collection is not found.
            Exception: If any error occurs while reading the items from the database.
        """
        request: Request = kwargs["request"]
        base_url = str(request.base_url)

        collection = await self.get_collection(
            collection_id=collection_id, request=request
        )
        collection_id = collection.get("id")
        if collection_id is None:
            raise HTTPException(status_code=404, detail="Collection not found")

        search = self.database.make_search()
        search = self.database.apply_collections_filter(
            search=search, collection_ids=[collection_id]
        )

        if datetime:
            datetime_search = self._return_date(datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if bbox:
            bbox = [float(x) for x in bbox]
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            sort=None,
            token=token,  # type: ignore
            collection_ids=[collection_id],
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        links = await PagingLinks(request=request, next=next_token).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=maybe_count,
        )

    async def get_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> stac_types.Item:
        """Get an item from the database based on its id and collection id.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be retrieved.

        Returns:
            Item: An `Item` object representing the requested item.

        Raises:
            Exception: If any error occurs while getting the item from the database.
            NotFoundError: If the item does not exist in the specified collection.
        """
        base_url = str(kwargs["request"].base_url)
        item = await self.database.get_one_item(
            item_id=item_id, collection_id=collection_id
        )
        return self.item_serializer.db_to_stac(item, base_url)

    @staticmethod
    def _return_date(
        interval: Optional[Union[DateTimeType, str]]
    ) -> Dict[str, Optional[str]]:
        """
        Convert a date interval.

        (which may be a datetime, a tuple of one or two datetimes a string
        representing a datetime or range, or None) into a dictionary for filtering
        search results with Elasticsearch.

        This function ensures the output dictionary contains 'gte' and 'lte' keys,
        even if they are set to None, to prevent KeyError in the consuming logic.

        Args:
            interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
                a tuple with one or two datetimes, a string, or None.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results,
                always containing 'gte' and 'lte' keys.
        """
        result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

        if interval is None:
            return result

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = parts[0] if parts[0] != ".." else None
                result["lte"] = (
                    parts[1] if len(parts) > 1 and parts[1] != ".." else None
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            datetime_iso = interval.isoformat()
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                result["gte"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if end:
                result["lte"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        return result

    def _format_datetime_range(self, date_tuple: DateTimeType) -> str:
        """
        Convert a tuple of datetime objects or None into a formatted string for API requests.

        Args:
            date_tuple (tuple): A tuple containing two elements, each can be a datetime object or None.

        Returns:
            str: A string formatted as 'YYYY-MM-DDTHH:MM:SS.sssZ/YYYY-MM-DDTHH:MM:SS.sssZ', with '..' used if any element is None.
        """

        def format_datetime(dt):
            """Format a single datetime object to the ISO8601 extended format with 'Z'."""
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" if dt else ".."

        start, end = date_tuple
        return f"{format_datetime(start)}/{format_datetime(end)}"

    async def get_search(
        self,
        request: Request,
        collections: Optional[List[str]] = None,
        ids: Optional[List[str]] = None,
        bbox: Optional[BBox] = None,
        datetime: Optional[DateTimeType] = None,
        limit: Optional[int] = 10,
        query: Optional[str] = None,
        token: Optional[str] = None,
        fields: Optional[List[str]] = None,
        sortby: Optional[str] = None,
        intersects: Optional[str] = None,
        filter: Optional[str] = None,
        filter_lang: Optional[str] = None,
        **kwargs,
    ) -> stac_types.ItemCollection:
        """Get search results from the database.

        Args:
            collections (Optional[List[str]]): List of collection IDs to search in.
            ids (Optional[List[str]]): List of item IDs to search for.
            bbox (Optional[BBox]): Bounding box to search in.
            datetime (Optional[DateTimeType]): Filter items based on the datetime field.
            limit (Optional[int]): Maximum number of results to return.
            query (Optional[str]): Query string to filter the results.
            token (Optional[str]): Access token to use when searching the catalog.
            fields (Optional[List[str]]): Fields to include or exclude from the results.
            sortby (Optional[str]): Sorting options for the results.
            intersects (Optional[str]): GeoJSON geometry to search in.
            kwargs: Additional parameters to be passed to the API.

        Returns:
            ItemCollection: Collection of `Item` objects representing the search results.

        Raises:
            HTTPException: If any error occurs while searching the catalog.
        """
        base_args = {
            "collections": collections,
            "ids": ids,
            "bbox": bbox,
            "limit": limit,
            "token": token,
            "query": orjson.loads(query) if query else query,
        }

        # this is borrowed from stac-fastapi-pgstac
        # Kludgy fix because using factory does not allow alias for filter-lan
        query_params = str(request.query_params)
        if filter_lang is None:
            match = re.search(r"filter-lang=([a-z0-9-]+)", query_params, re.IGNORECASE)
            if match:
                filter_lang = match.group(1)

        if datetime:
            base_args["datetime"] = self._format_datetime_range(datetime)

        if intersects:
            base_args["intersects"] = orjson.loads(unquote_plus(intersects))

        if sortby:
            sort_param = []
            for sort in sortby:
                sort_param.append(
                    {
                        "field": sort[1:],
                        "direction": "desc" if sort[0] == "-" else "asc",
                    }
                )
            base_args["sortby"] = sort_param

        if filter:
            if filter_lang == "cql2-json":
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(unquote_plus(filter))
            else:
                base_args["filter-lang"] = "cql2-json"
                base_args["filter"] = orjson.loads(to_cql2(parse_cql2_text(filter)))

        if fields:
            includes = set()
            excludes = set()
            for field in fields:
                if field[0] == "-":
                    excludes.add(field[1:])
                elif field[0] == "+":
                    includes.add(field[1:])
                else:
                    includes.add(field)
            base_args["fields"] = {"include": includes, "exclude": excludes}

        # Do the request
        try:
            search_request = self.post_request_model(**base_args)
        except ValidationError as e:
            raise HTTPException(status_code=400, detail=f"Invalid parameters provided: {e}")
        resp = await self.post_search(search_request=search_request, request=request)

        return resp

    async def post_search(
        self, search_request: BaseSearchPostRequest, request: Request
    ) -> stac_types.ItemCollection:
        """
        Perform a POST search on the catalog.

        Args:
            search_request (BaseSearchPostRequest): Request object that includes the parameters for the search.
            kwargs: Keyword arguments passed to the function.

        Returns:
            ItemCollection: A collection of items matching the search criteria.

        Raises:
            HTTPException: If there is an error with the cql2_json filter.
        """
        base_url = str(request.base_url)

        search = self.database.make_search()

        if search_request.ids:
            search = self.database.apply_ids_filter(
                search=search, item_ids=search_request.ids
            )

        if search_request.collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=search_request.collections
            )

        if search_request.datetime:
            datetime_search = self._return_date(search_request.datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if search_request.bbox:
            bbox = search_request.bbox
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        if search_request.intersects:
            search = self.database.apply_intersects_filter(
                search=search, intersects=search_request.intersects
            )

        if search_request.query:
            for field_name, expr in search_request.query.items():
                field = "properties__" + field_name
                for op, value in expr.items():
                    # Convert enum to string
                    operator = op.value if isinstance(op, Enum) else op
                    search = self.database.apply_stacql_filter(
                        search=search, op=operator, field=field, value=value
                    )

        # only cql2_json is supported here
        if hasattr(search_request, "filter"):
            cql2_filter = getattr(search_request, "filter", None)
            try:
                search = self.database.apply_cql2_filter(search, cql2_filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        sort = None
        if search_request.sortby:
            sort = self.database.populate_sort(search_request.sortby)

        limit = 10
        if search_request.limit:
            limit = search_request.limit

        items, maybe_count, next_token = await self.database.execute_search(
            search=search,
            limit=limit,
            token=search_request.token,  # type: ignore
            sort=sort,
            collection_ids=search_request.collections,
        )

        items = [
            self.item_serializer.db_to_stac(item, base_url=base_url) for item in items
        ]

        if self.extension_is_enabled("FieldsExtension"):
            if search_request.query is not None:
                query_include: Set[str] = set(
                    [
                        k if k in Settings.get().indexed_fields else f"properties.{k}"
                        for k in search_request.query.keys()
                    ]
                )
                if not search_request.fields.include:
                    search_request.fields.include = query_include
                else:
                    search_request.fields.include.union(query_include)

            filter_kwargs = search_request.fields.filter_fields

            items = [
                orjson.loads(
                    stac_pydantic.Item(**feat).json(**filter_kwargs, exclude_unset=True)
                )
                for feat in items
            ]

        links = await PagingLinks(request=request, next=next_token).get_links()

        return stac_types.ItemCollection(
            type="FeatureCollection",
            features=items,
            links=links,
            numReturned=len(items),
            numMatched=maybe_count,
        )


@attr.s
class TransactionsClient(AsyncBaseTransactionsClient):
    """Transactions extension specific CRUD operations."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    @overrides
    async def create_item(
        self, collection_id: str, item: Union[Item, ItemCollection], **kwargs
    ) -> Optional[stac_types.Item]:
        """Create an item in the collection.

        Args:
            collection_id (str): The id of the collection to add the item to.
            item (stac_types.Item): The item to be added to the collection.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Item: The created item.

        Raises:
            NotFound: If the specified collection is not found in the database.
            ConflictError: If the item in the specified collection already exists.

        """
        item = item.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)

        # If a feature collection is posted
        if item["type"] == "FeatureCollection":
            bulk_client = BulkTransactionsClient(
                database=self.database, settings=self.settings
            )
            processed_items = [
                bulk_client.preprocess_item(item, base_url, BulkTransactionMethod.INSERT) for item in item["features"]  # type: ignore
            ]

            await self.database.bulk_async(
                collection_id, processed_items, refresh=kwargs.get("refresh", False)
            )

            return None
        else:
            item = await self.database.prep_create_item(item=item, base_url=base_url)
            await self.database.create_item(item, refresh=kwargs.get("refresh", False))
            return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def update_item(
        self, collection_id: str, item_id: str, item: Item, **kwargs
    ) -> stac_types.Item:
        """Update an item in the collection.

        Args:
            collection_id (str): The ID of the collection the item belongs to.
            item_id (str): The ID of the item to be updated.
            item (stac_types.Item): The new item data.
            kwargs: Other optional arguments, including the request object.

        Returns:
            stac_types.Item: The updated item object.

        Raises:
            NotFound: If the specified collection is not found in the database.

        """
        item = item.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)
        now = datetime_type.now(timezone.utc).isoformat().replace("+00:00", "Z")
        item["properties"]["updated"] = now

        await self.database.check_collection_exists(collection_id)
        await self.delete_item(item_id=item_id, collection_id=collection_id)
        await self.create_item(collection_id=collection_id, item=Item(**item), **kwargs)

        return ItemSerializer.db_to_stac(item, base_url)

    @overrides
    async def delete_item(
        self, item_id: str, collection_id: str, **kwargs
    ) -> Optional[stac_types.Item]:
        """Delete an item from a collection.

        Args:
            item_id (str): The identifier of the item to delete.
            collection_id (str): The identifier of the collection that contains the item.

        Returns:
            Optional[stac_types.Item]: The deleted item, or `None` if the item was successfully deleted.
        """
        await self.database.delete_item(item_id=item_id, collection_id=collection_id)
        return None

    @overrides
    async def create_collection(
        self, collection: Collection, **kwargs
    ) -> stac_types.Collection:
        """Create a new collection in the database.

        Args:
            collection (stac_types.Collection): The collection to be created.
            kwargs: Additional keyword arguments.

        Returns:
            stac_types.Collection: The created collection object.

        Raises:
            ConflictError: If the collection already exists.
        """
        collection = collection.model_dump(mode="json")
        base_url = str(kwargs["request"].base_url)
        collection = self.database.collection_serializer.stac_to_db(
            collection, base_url
        )
        await self.database.create_collection(collection=collection)
        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def update_collection(
        self, collection_id: str, collection: Collection, **kwargs
    ) -> stac_types.Collection:
        """
        Update a collection.

        This method updates an existing collection in the database by first finding
        the collection by the id given in the keyword argument `collection_id`.
        If no `collection_id` is given the id of the given collection object is used.
        If the object and keyword collection ids don't match the sub items
        collection id is updated else the items are left unchanged.
        The updated collection is then returned.

        Args:
            collection_id: id of the existing collection to be updated
            collection: A STAC collection that needs to be updated.
            kwargs: Additional keyword arguments.

        Returns:
            A STAC collection that has been updated in the database.

        """
        collection = collection.model_dump(mode="json")

        base_url = str(kwargs["request"].base_url)

        collection = self.database.collection_serializer.stac_to_db(
            collection, base_url
        )
        await self.database.update_collection(
            collection_id=collection_id, collection=collection
        )

        return CollectionSerializer.db_to_stac(collection, base_url)

    @overrides
    async def delete_collection(
        self, collection_id: str, **kwargs
    ) -> Optional[stac_types.Collection]:
        """
        Delete a collection.

        This method deletes an existing collection in the database.

        Args:
            collection_id (str): The identifier of the collection that contains the item.
            kwargs: Additional keyword arguments.

        Returns:
            None.

        Raises:
            NotFoundError: If the collection doesn't exist.
        """
        await self.database.delete_collection(collection_id=collection_id)
        return None


@attr.s
class BulkTransactionsClient(BaseBulkTransactionsClient):
    """A client for posting bulk transactions to a Postgres database.

    Attributes:
        session: An instance of `Session` to use for database connection.
        database: An instance of `DatabaseLogic` to perform database operations.
    """

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    def __attrs_post_init__(self):
        """Create es engine."""
        self.client = self.settings.create_client

    def preprocess_item(
        self, item: stac_types.Item, base_url, method: BulkTransactionMethod
    ) -> stac_types.Item:
        """Preprocess an item to match the data model.

        Args:
            item: The item to preprocess.
            base_url: The base URL of the request.
            method: The bulk transaction method.

        Returns:
            The preprocessed item.
        """
        exist_ok = method == BulkTransactionMethod.UPSERT
        return self.database.sync_prep_create_item(
            item=item, base_url=base_url, exist_ok=exist_ok
        )

    @overrides
    def bulk_item_insert(
        self, items: Items, chunk_size: Optional[int] = None, **kwargs
    ) -> str:
        """Perform a bulk insertion of items into the database using Elasticsearch.

        Args:
            items: The items to insert.
            chunk_size: The size of each chunk for bulk processing.
            **kwargs: Additional keyword arguments, such as `request` and `refresh`.

        Returns:
            A string indicating the number of items successfully added.
        """
        request = kwargs.get("request")
        if request:
            base_url = str(request.base_url)
        else:
            base_url = ""

        processed_items = [
            self.preprocess_item(item, base_url, items.method)
            for item in items.items.values()
        ]

        # not a great way to get the collection_id-- should be part of the method signature
        collection_id = processed_items[0]["collection"]

        self.database.bulk_sync(
            collection_id, processed_items, refresh=kwargs.get("refresh", False)
        )

        return f"Successfully added {len(processed_items)} Items."


@attr.s
class EsAsyncBaseFiltersClient(AsyncBaseFiltersClient):
    """Defines a pattern for implementing the STAC filter extension."""

    # todo: use the ES _mapping endpoint to dynamically find what fields exist
    async def get_queryables(
        self, collection_id: Optional[str] = None, **kwargs
    ) -> Dict[str, Any]:
        """Get the queryables available for the given collection_id.

        If collection_id is None, returns the intersection of all
        queryables over all collections.

        This base implementation returns a blank queryable schema. This is not allowed
        under OGC CQL but it is allowed by the STAC API Filter Extension

        https://github.com/radiantearth/stac-api-spec/tree/master/fragments/filter#queryables

        Args:
            collection_id (str, optional): The id of the collection to get queryables for.
            **kwargs: additional keyword arguments

        Returns:
            Dict[str, Any]: A dictionary containing the queryables for the given collection.
        """
        return {
            "$schema": "https://json-schema.org/draft/2019-09/schema",
            "$id": "https://stac-api.example.com/queryables",
            "type": "object",
            "title": "Queryables for Example STAC API",
            "description": "Queryable names for the example STAC API Item Search filter.",
            "properties": {
                "id": {
                    "description": "ID",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/properties/id",
                },
                "collection": {
                    "description": "Collection",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/2/then/properties/collection",
                },
                "geometry": {
                    "description": "Geometry",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/item.json#/definitions/core/allOf/1/oneOf/0/properties/geometry",
                },
                "datetime": {
                    "description": "Acquisition Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/datetime",
                },
                "created": {
                    "description": "Creation Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/created",
                },
                "updated": {
                    "description": "Creation Timestamp",
                    "$ref": "https://schemas.stacspec.org/v1.0.0/item-spec/json-schema/datetime.json#/properties/updated",
                },
                "cloud_cover": {
                    "description": "Cloud Cover",
                    "$ref": "https://stac-extensions.github.io/eo/v1.0.0/schema.json#/definitions/fields/properties/eo:cloud_cover",
                },
                "cloud_shadow_percentage": {
                    "description": "Cloud Shadow Percentage",
                    "title": "Cloud Shadow Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
                "nodata_pixel_percentage": {
                    "description": "No Data Pixel Percentage",
                    "title": "No Data Pixel Percentage",
                    "type": "number",
                    "minimum": 0,
                    "maximum": 100,
                },
            },
            "additionalProperties": True,
        }
    
import abc

import sys
if sys.version_info < (3, 9, 2):
    from typing_extensions import TypedDict
else:
    from typing import TypedDict
from typing import List, Type, Union, Dict, Any, Optional, Literal
from pydantic import Field


class Bucket(TypedDict, total=False):
    """A STAC aggregation bucket."""

    key: str
    data_type: str
    frequency: Optional[Dict] = None
    _from: Optional[Union[int, float]] = Field(alias="filter-crs", default=None)
    to: Optional[Optional[Union[int, float]]] = None

class Aggregation(TypedDict, total=False):
    """A STAC aggregation."""

    name: str
    data_type: str
    buckets: Optional[Dict] = None
    overflow: Optional[int] = None
    value: Optional[Union[str, int, DateTimeType]] = None


class AggregationCollection(TypedDict, total=False):
    """STAC Item Aggregation Collection."""

    type: Literal["FeatureCollection"]
    aggregations: List[Aggregation]
    links: List[Dict[str, Any]]


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
        return AggregationCollection(
            type="AggregationCollection",
            aggregations=[
                Aggregation(
                    name="total_count",
                    data_type="integer"
                )
            ],
            links=[
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
        )
    
    def aggregate(
            self, collection_id: Optional[str] = None, **kwargs
        ) -> AggregationCollection:
        return AggregationCollection(
            type="AggregationCollection",
            aggregations=[],
            links=[
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
        )

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
        return AggregationCollection(
            type="AggregationCollection",
            aggregations=[
                Aggregation(
                    name="total_count",
                    data_type="integer"
                )
            ],
            links=[
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
        )

@attr.s
class EsAsyncAggregationClient(AsyncBaseAggregationClient):
    """Defines a pattern for implementing the STAC aggregation extension."""

    database: BaseDatabaseLogic = attr.ib()
    settings: ApiBaseSettings = attr.ib()
    session: Session = attr.ib(default=attr.Factory(Session.create_from_env))

    from typing import Optional, List, Union, Dict
    from fastapi import HTTPException
    from datetime import datetime

    # HOW TO PASS THE ENDPOINT TO THIS?? LOOK AT stac-fastapi
    async def get_aggregations(self,
                                collection_id: Optional[str] = None,
                                **kwargs
                                ):
        
        request: Request = kwargs["request"]
        base_url = str(request.base_url)

        DEFAULT_AGGREGATIONS = [
            {
                "name": 'total_count',
                "data_type": 'integer'
            },
            {
                "name": 'datetime_max',
                "data_type": 'datetime'
            },
            {
                "name": 'datetime_min',
                "data_type": 'datetime'
            },
            {
                "name": 'datetime_frequency',
                "data_type": 'frequency_distribution',
                "frequency_distribution_data_type": 'datetime'
            },
        ]

        links = [
            {
                "rel": "root",
                "type": "application/json",
                "href": base_url
            }
        ]

        if collection_id:
            collection_endpoint = urljoin(base_url, "collections", collection_id)
            links.extend(
                [
                    {
                        "rel": "collection",
                        "type": "application/json",
                        "href": collection_endpoint
                    },
                    {
                        "rel": "self",
                        "type": "application/json",
                        "href": urljoin(collection_endpoint, "aggregations")
                    }
                ]
            )
            if self.database.check_collection_exists(collection_id):
                collection = await self.database.find_collection(collection_id)
                aggregations = collection.get("aggregations", DEFAULT_AGGREGATIONS.copy())
            else:
                raise IndexError("Collection does not exist")
        else:
            links.append(
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": urljoin(base_url, "aggregations")
                }
            )
        
            aggregations = DEFAULT_AGGREGATIONS.copy()
        return AggregationCollection(
            type="AggregationCollection",
            aggregations=aggregations,
            links=links
        )


    # TEST WITH SINGLE COLLECTION
    # TEST WITH MUTLIPLE
    def extract_collection_ids(self, collection_ids: Union[str, List[str]]) -> List[str]:
        import json
        if collection_ids:
            try:
                if isinstance(collection_ids, str):
                    if ',' in collection_ids:
                        ids_rules = collection_ids.split(',')
                    else:
                        ids_rules = [collection_ids]
                else:
                    ids_rules = list(collection_ids)
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(status_code=400, detail='Invalid collections value')

            return ids_rules
        return []
    

    def extract_aggregations(self, aggregations_value: Union[str, List[str]]) -> List[str]:
        import json
        if aggregations_value:
            try:
                if isinstance(aggregations_value, str):
                    if ',' in aggregations_value:
                        aggs = aggregations_value.split(',')
                    else:
                        aggs = [aggregations_value]
                else:
                    aggs = list(aggregations_value)
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(status_code=400, detail='Invalid aggregations value')

            return aggs
        return []

    
    # UNCLEAR WHAT THIS DOES
    def extract_ids(self, ids_value: Union[str, List[str]]) -> List[str]:
        import json
        if ids_value:
            try:
                if isinstance(ids_value, str):
                    if ',' in ids_value:
                        ids_rules = ids_value.split(',')
                    else:
                        ids_rules = [ids_value]
                else:
                    ids_rules = list(ids_value)
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(status_code=400, detail='Invalid ids value')

            return ids_rules
        else:
            return []


    # TEST WITH BAD STRING
    # TEST WITH BAD JSON
    # TEST WITH FEATURECOLLECTION AND WITH FEATURE
    # TEST WITH INVALID GEOMETRY
    def extract_intersects(self, intersects_value: Union[str, Dict]) -> Union[Dict, None]:
        import json
        if intersects_value:
            try:
                if isinstance(intersects_value, str):
                    geojson = json.loads(intersects_value)
                else:
                    geojson = dict(intersects_value)

                if geojson.get('type') == 'FeatureCollection' or geojson.get('type') == 'Feature':
                    raise HTTPException(status_code=400, detail='Expected GeoJSON geometry, not Feature or FeatureCollection')

                return geojson
            except (json.JSONDecodeError, ValueError):
                raise HTTPException(status_code=400, detail='Invalid GeoJSON geometry')
        else:
            return None
    
    
    # TEST NUMBER OUTSIDE OF RANGE
    # TEST NONE PRECISION
    def extract_precision(self, precision: int, min_value: int, max_value: int) -> Optional[int]:

        if precision is not None:
            if precision < min_value or precision > max_value:
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid precision. Must be a number between {min_value} and {max_value} inclusive",
                )
            return precision
        else:
            return min_value
        
    
    def is_finite(self, value: float) -> bool:
        import math
        return not math.isnan(value) and math.isfinite(value)
        
    # TEST GET AND POST WITH TEXT AND LISTS
    # TEST THE WRONG NUMBER OF POINTS
    # TEST LATITUDE
    def extract_bbox(self, bbox_value: Union[str, List[float]], http_method: str = 'GET') -> Optional[List[float]]:
        if bbox_value is not None:
            if http_method.upper() == 'GET' and isinstance(bbox_value, str):
                try:
                    bbox_array = [float(x) for x in bbox_value.split(',') if self.is_finite(float(x))]
                except ValueError:
                    raise HTTPException(status_code=400, detail='Invalid bbox')
            elif http_method.upper() == 'POST' and isinstance(bbox_value, list):
                bbox_array = bbox_value
            else:
                raise HTTPException(status_code=400, detail='Invalid bbox')

            if len(bbox_array) not in [4, 6]:
                raise HTTPException(status_code=400, detail='Invalid bbox, must have 4 or 6 points')

            if (len(bbox_array) == 4 and bbox_array[1] > bbox_array[3]) or \
            (len(bbox_array) == 6 and bbox_array[1] > bbox_array[4]):
                raise HTTPException(status_code=400, detail='Invalid bbox, SW latitude must be less than NE latitude')

            return bbox_array
        else:
            return None
        
    
    def _return_date(
        self,
        interval: Optional[Union[DateTimeType, str]]
    ) -> Dict[str, Optional[str]]:
        """
        Convert a date interval.

        (which may be a datetime, a tuple of one or two datetimes a string
        representing a datetime or range, or None) into a dictionary for filtering
        search results with Elasticsearch.

        This function ensures the output dictionary contains 'gte' and 'lte' keys,
        even if they are set to None, to prevent KeyError in the consuming logic.

        Args:
            interval (Optional[Union[DateTimeType, str]]): The date interval, which might be a single datetime,
                a tuple with one or two datetimes, a string, or None.

        Returns:
            dict: A dictionary representing the date interval for use in filtering search results,
                always containing 'gte' and 'lte' keys.
        """
        result: Dict[str, Optional[str]] = {"gte": None, "lte": None}

        if interval is None:
            return result

        if isinstance(interval, str):
            if "/" in interval:
                parts = interval.split("/")
                result["gte"] = parts[0] if parts[0] != ".." else None
                result["lte"] = (
                    parts[1] if len(parts) > 1 and parts[1] != ".." else None
                )
            else:
                converted_time = interval if interval != ".." else None
                result["gte"] = result["lte"] = converted_time
            return result

        if isinstance(interval, datetime_type):
            datetime_iso = interval.isoformat()
            result["gte"] = result["lte"] = datetime_iso
        elif isinstance(interval, tuple):
            start, end = interval
            # Ensure datetimes are converted to UTC and formatted with 'Z'
            if start:
                result["gte"] = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
            if end:
                result["lte"] = end.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

        return result
        

    def agg(self, es_aggs, name, data_type):
        buckets = []
        for bucket in (es_aggs.get(name, {}).get('buckets', [])):
            bucket_data = {
                'key': bucket.get('key_as_string') or bucket.get('key'),
                'data_type': data_type,
                'frequency': bucket.get('doc_count'),
                'to': bucket.get('to'),
                'from': bucket.get('from'),
            }
            buckets.append(bucket_data)
        return Aggregation(name=name,
                           data_type="frequency_distribution",
                           overflow=es_aggs.get(name, {}).get('sum_other_doc_count', 0),
                           buckets=buckets
                           )


    async def aggregate(
        self,
        datetime,
        collections,
        aggregations, 
        filter,
        ids,  
        bbox,  
        intersects,
        grid_geohex_frequency_precision,
        grid_geohash_frequency_precision,
        grid_geotile_frequency_precision,
        centroid_geohash_grid_frequency_precision,
        centroid_geohex_grid_frequency_precision,
        centroid_geotile_grid_frequency_precision,
        geometry_geohash_grid_frequency_precision,
        geometry_geotile_grid_frequency_precision,
        collection_id: Optional[str]=None,
        **kwargs
    ) -> Union[Dict, Exception]:

        request: Request = kwargs["request"]
        base_url = str(request.base_url)

        if bbox and intersects:
            raise ValueError('Expected bbox OR intersects, not both')
        
        search = self.database.make_search()

        if collection_id:
            collection_endpoint = urljoin(base_url, "collections", collection_id)

            if self.database.check_collection_exists(collection_id):
                collection = await self.database.find_collection(collection_id)
                search = self.database.apply_collections_filter(
                    search=search, collection_ids=[collection_id]
                )
            if isinstance(collection, Exception):
                return collection
            
        elif collections:
            search = self.database.apply_collections_filter(
                search=search, collection_ids=self.extract_collection_ids(collections)
            )
            collection_endpoint = None
            collection: Collection = None
        else:
            collection_endpoint = None
            collection: Collection = None

        if datetime:
            datetime_search = self._return_date(datetime)
            search = self.database.apply_datetime_filter(
                search=search, datetime_search=datetime_search
            )

        if bbox:
            if len(bbox) == 6:
                bbox = [bbox[0], bbox[1], bbox[3], bbox[4]]

            search = self.database.apply_bbox_filter(search=search, bbox=bbox)

        elif intersects:
            intersects_geometry = self.extract_intersects(intersects)
            search = self.database.apply_intersects_filter(search=search, intersects=intersects_geometry)

        if ids:
            ids = self.extract_ids(ids)
            search = self.database.apply_ids_filter(search=search, item_ids=ids)

        # only cql2_json is supported here
        if filter:
            if isinstance(filter, str):
                filter = json.loads(filter)
            try:
                search = self.database.apply_cql2_filter(search, filter)
            except Exception as e:
                raise HTTPException(
                    status_code=400, detail=f"Error with cql2_json filter: {e}"
                )

        aggregations_requested = self.extract_aggregations(aggregations)

        DEFAULT_AGGREGATIONS = [
            {
                "name": 'total_count',
                "data_type": 'integer'
            },
            {
                "name": 'datetime_max',
                "data_type": 'datetime'
            },
            {
                "name": 'datetime_min',
                "data_type": 'datetime'
            },
            {
                "name": 'datetime_frequency',
                "data_type": 'frequency_distribution',
                "frequency_distribution_data_type": 'datetime'
            },
        ]

        ALL_AGGREGATION_NAMES = [
            agg['name'] for agg in DEFAULT_AGGREGATIONS
        ] + [
            'collection_frequency',
            'grid_code_frequency',
            'grid_geohash_frequency',
            'grid_geohex_frequency',
            'grid_geotile_frequency',
            'centroid_geohash_grid_frequency',
            'centroid_geohex_grid_frequency',
            'centroid_geotile_grid_frequency',
            'geometry_geohash_grid_frequency',
            # 'geometry_geohex_grid_frequency',
            'geometry_geotile_grid_frequency',
            'platform_frequency',
            'sun_elevation_frequency',
            'sun_azimuth_frequency',
            'off_nadir_frequency',
            'cloud_cover_frequency',
        ]

        # validate that aggregations are supported by collection
        # if aggregations are not defined for a collection, any aggregation may be requested
        if collection and collection.get("aggregations"):
            supported_aggregations = [x.name for x in collection.get("aggregations")]
            for agg_name in aggregations_requested:
                if agg_name not in supported_aggregations:
                    raise HTTPException(status_code=415, detail=f"Aggregation {agg_name} not supported by collection {collection_id}")
        else:
            for agg_name in aggregations_requested:
                if agg_name not in ALL_AGGREGATION_NAMES:
                    raise HTTPException(status_code=415, detail=f"Aggregation {agg_name} not supported at catalog level")

        max_geohash_precision = 12
        max_geohex_precision = 15
        max_geotile_precision = 29

        geohash_precision = self.extract_precision(
            grid_geohash_frequency_precision,
            1,
            max_geohash_precision       )

        geohex_precision = self.extract_precision(
            grid_geohex_frequency_precision,
            0,
            max_geohex_precision
        )

        geotile_precision = self.extract_precision(
            grid_geotile_frequency_precision,
            0,
            max_geotile_precision
        )

        centroid_geohash_grid_precision = self.extract_precision(
            centroid_geohash_grid_frequency_precision,
            1,
            max_geohash_precision
        )

        centroid_geohex_grid_precision = self.extract_precision(
            centroid_geohex_grid_frequency_precision,
            0,
            max_geohex_precision
        )

        centroid_geotile_grid_precision = self.extract_precision(
            centroid_geotile_grid_frequency_precision,
            0,
            max_geotile_precision
        )

        geometry_geohash_grid_precision = self.extract_precision(
            geometry_geohash_grid_frequency_precision,
            1,
            max_geohash_precision
        )

        # geometry_geohex_grid_frequency_precision = self.extract_precision(
        #     geometry_geohex_grid_frequency_precision,
        #     0,
        #     max_geohex_precision
        # )

        geometry_geotile_grid_precision = self.extract_precision(
            geometry_geotile_grid_frequency_precision,
            0,
            max_geotile_precision
        )

        try:
            db_response = await self.database.aggregate(
                collection_id,
                aggregations_requested,
                search,
                geohash_precision,
                geohex_precision,
                geotile_precision,
                centroid_geohash_grid_precision,
                centroid_geohex_grid_precision,
                centroid_geotile_grid_precision,
                geometry_geohash_grid_precision,
                # geometry_geohex_grid_precision,
                geometry_geotile_grid_precision
            )
        except Exception as error:
            if not isinstance(error, IndexError):
                raise error
        
        aggregations: List[Dict] = []

        if db_response:
            result_aggs = db_response.get('aggregations')

            if 'total_count' in aggregations_requested:
                aggregations.append(
                    Aggregation(
                        name='total_count',
                        data_type="integer",
                        value=result_aggs.get('total_count', {}).get('value', None)
                        )
                    )

            if 'datetime_max' in aggregations_requested:
                aggregations.append(
                    Aggregation(
                        name='datetime_max',
                        data_type="datetime",
                        value=result_aggs.get('datetime_max', {}).get('value_as_string', None)
                        )
                    )

            if 'datetime_min' in aggregations_requested:
                aggregations.append(
                    Aggregation(
                        name='datetime_min',
                        data_type="datetime",
                        value=result_aggs.get('datetime_min', {}).get('value_as_string', None)
                        )
                    )

            other_aggregations = {
                'collection_frequency': 'string',
                'grid_code_frequency': 'string',
                'grid_geohash_frequency': 'string',
                'grid_geohex_frequency': 'string',
                'grid_geotile_frequency': 'string',
                'centroid_geohash_grid_frequency': 'string',
                'centroid_geohex_grid_frequency': 'string',
                'centroid_geotile_grid_frequency': 'string',
                'geometry_geohash_grid_frequency': 'string',
                'geometry_geotile_grid_frequency': 'string',
                'platform_frequency': 'string',
                'sun_elevation_frequency': 'string',
                'sun_azimuth_frequency': 'string',
                'off_nadir_frequency': 'string',
                'datetime_frequency': 'datetime',
                'cloud_cover_frequency': 'numeric',
            }

            for agg_name, data_type in other_aggregations.items():
                if agg_name in aggregations_requested:
                    aggregations.append(self.agg(result_aggs, agg_name, data_type))
        links = [
                {
                    'rel': 'self',
                    'type': 'application/json',
                    'href': urljoin(base_url, "aggregate")
                },
                {
                    'rel': 'root',
                    'type': 'application/json',
                    'href': base_url
                }
            ]
        if collection_endpoint:
            results['links'].append({
                'rel': 'collection',
                'type': 'application/json',
                'href': collection_endpoint
            })
        results = AggregationCollection(
            type="AggregationCollection",
            aggregations=aggregations,
            links=links
        )

        return results
