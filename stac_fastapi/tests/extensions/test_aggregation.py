import json
import os
from os import listdir
from os.path import isfile, join

import pytest

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

@pytest.mark.asyncio
async def test_catalog_aggregations(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get(
        '/aggregations'
    )

    assert resp.status_code == 200
    assert len(resp.json()["aggregations"]) == 12

@pytest.mark.asyncio
async def test_aggregate_filter_extension_gte_get(app_client, ctx):
    # there's one item that can match, so one of these queries should match it and the other shouldn't
    resp = await app_client.get(
        '/aggregate?aggregations=grid_geohex_frequency,total_count&grid_geohex_frequency_precision=2&filter={"op":"<=","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 1

    resp = await app_client.get(
        '/aggregate?aggregations=grid_geohex_frequency,total_count&grid_geohex_frequency_precision=2&filter={"op":">","args":[{"property": "properties.proj:epsg"},32756]}'
    )

    assert resp.status_code == 200
    assert resp.json()["aggregations"][0]["value"] == 0