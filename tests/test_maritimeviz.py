#!/usr/bin/env python

"""Tests for `maritimeviz` package."""
import os
import pytest
from src.maritimeviz.maritimeviz import GFW_api

#designed to provide a reusable instance of the GFW_api class
@pytest.fixture
def gfw_api():
    token = os.getenv("GFW_API_TOKEN")
    if not token:
        pytest.fail("API token not set.")
    return GFW_api(token=token)


def test_search_vessel(gfw_api):
    vessel_id = "7831410" 
    result = gfw_api.search_vessel(vessel_id)  # Call method from fixture instance

    assert result is not None, "Expected valid vessel data, but got None"
    assert isinstance(result, list), "Expected result to be a list"
    assert result, "Expected non-empty response list"
    assert "vessel_id" in result[0], "Missing 'vessel_id' in response"


def test_get_fishing_events(gfw_api):
    vessel_id = "7831410"
    start_date = "2022-01-01"
    end_date = "2022-01-02"

    result = gfw_api.get_fishing_events(vessel_id, start_date, end_date)

    assert result is not None, "Expected fishing event data."
    assert isinstance(result, list), "Expected a list of fishing events"
    assert result, "Expected non-empty list of fishing events"


def test_get_fishing_stats(gfw_api): 
    start_date = "2022-01-01"
    end_date = "2022-02-01"

    result = gfw_api.get_fishing_stats(start_date, end_date)

    assert result is not None, "Expected fishing stats data, but got None"
    assert isinstance(result, dict), "Expected a dictionary of statistics"
    assert result, "Expected non-empty dictionary of statistics"
