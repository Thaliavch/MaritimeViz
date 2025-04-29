#!/usr/bin/env python

"""Tests for the Map class in the visualization module."""

import geopandas as gpd
import pytest
from shapely.geometry import Point

from src.maritimeviz.viz import Map


@pytest.fixture
def sample_gdf():
    data = {
        "latitude": [10.0, 10.5, 11.0],
        "longitude": [20.0, 20.5, 21.0],
        "speed": [1, 5, 12],
        "mmsi": [123456789, 123456789, 123456789],
        "timestamp": ["2022-01-01", "2022-01-01", "2022-01-01"],
    }
    gdf = gpd.GeoDataFrame(data)
    gdf["geometry"] = [Point(xy) for xy in zip(gdf.longitude, gdf.latitude)]
    return gdf


@pytest.fixture
def wkt_polygon():
    return "POLYGON((19 9, 22 9, 22 12, 19 12, 19 9))"


def test_ship_map_by_polygon(sample_gdf, wkt_polygon):
    map_obj = Map()
    map_obj_out = map_obj.ship_map_by_polygon(wkt_polygon, sample_gdf)
    assert map_obj_out is not None, "Expected map object to be returned"


def test_ships_route_with_invalid_mmsi(sample_gdf, tmp_path):
    geojson_path = tmp_path / "route.geojson"
    sample_gdf.to_file(geojson_path, driver="GeoJSON")
    map_obj = Map()
    result = map_obj.ships_route(str(geojson_path), mmsi=999999)
    assert result == "No ship found with that mmsi", "Expected message for invalid MMSI"
