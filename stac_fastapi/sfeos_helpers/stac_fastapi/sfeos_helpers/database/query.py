"""Query building functions for Elasticsearch/OpenSearch.

This module provides functions for building and manipulating Elasticsearch/OpenSearch queries.
"""

import re
from typing import Any, Dict, List, Optional

from stac_fastapi.sfeos_helpers.mappings import Geometry


def is_numeric(val: str) -> bool:
    try:
        float(val)
        return True
    except ValueError:
        return False


def is_date(val: str) -> bool:
    # Basic ISO8601 date match: YYYY-MM-DD or YYYY-MM-DDTHH:MM:SSZ
    iso_date_pattern = re.compile(r"^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}Z?)?$")
    return bool(iso_date_pattern.match(val))


def process_ftq(q):
    q = q.strip()
    if not q:
        return
    if is_numeric(q) or is_date(q):
        return q
    else:
        return f"({q}* OR {q.lower()}* OR {q.upper()}*)"


def apply_free_text_filter_shared(
    search: Any,
    free_text_queries: Optional[List[str]],
    fields: Optional[List[str]] = [],
) -> Any:
    if free_text_queries:
        processed_queries = [
            process_ftq(q.strip()) for q in free_text_queries if q.strip()
        ]

        if processed_queries:
            free_text_query_string = " AND ".join(processed_queries)

            search = search.query(
                "query_string", query=free_text_query_string, fields=fields
            )

    return search


def apply_intersects_filter_shared(
    intersects: Geometry,
) -> Dict[str, Dict]:
    """Create a geo_shape filter for intersecting geometry.

    Args:
        intersects (Geometry): The intersecting geometry, represented as a GeoJSON-like object.

    Returns:
        Dict[str, Dict]: A dictionary containing the geo_shape filter configuration
            that can be used with Elasticsearch/OpenSearch Q objects.

    Notes:
        This function creates a geo_shape filter configuration to find documents that intersect
        with the specified geometry. The returned dictionary should be wrapped in a Q object
        when applied to a search.
    """
    return {
        "geo_shape": {
            "geometry": {
                "shape": {
                    "type": intersects.type.lower(),
                    "coordinates": intersects.coordinates,
                },
                "relation": "intersects",
            }
        }
    }


def populate_sort_shared(sortby: List) -> Optional[Dict[str, Dict[str, str]]]:
    """Create a sort configuration for Elasticsearch/OpenSearch queries.

    Args:
        sortby (List): A list of sort specifications, each containing a field and direction.

    Returns:
        Optional[Dict[str, Dict[str, str]]]: A dictionary mapping field names to sort direction
            configurations, or None if no sort was specified.

    Notes:
        This function transforms a list of sort specifications into the format required by
        Elasticsearch/OpenSearch for sorting query results. The returned dictionary can be
        directly used in search requests.
    """
    if sortby:
        return {s.field: {"order": s.direction} for s in sortby}
    else:
        return None
