#!/usr/bin/env python

"""Tests for the Map class in the visualization module."""
import pytest
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point, Polygon
from src.maritimeviz.viz import Map

@pytest.fixture
def sample_gdf():
    data = {
        "latitude": [10.0, 10.5, 11.0],
        "longitude": [20.0, 20.5, 21.0],
        "speed": [1, 5, 12],
        "mmsi": [123456789, 123456789, 123456789],
        "timestamp": ["2022-01-01", "2022-01-01", "2022-01-01"]
    }
    gdf = gpd.GeoDataFrame(data)
    gdf["geometry"] = [Point(xy) for xy in zip(gdf.longitude, gdf.latitude)]
    return gdf

@pytest.fixture
def wkt_polygon():
    return "POLYGON((19 9, 22 9, 22 12, 19 12, 19 9))"

def test_filter_ships_by_polygon(sample_gdf, wkt_polygon):
    map_obj = Map()
    polygon = wkt.loads(wkt_polygon)
    filtered = map_obj.filter_ships_by_polygon(wkt_polygon, sample_gdf)
    assert not filtered.empty, "Expected some ships to be inside the polygon"
    assert all(filtered.geometry.within(polygon)), "Some points are outside the polygon"

def test_ship_map_by_polygon(sample_gdf, wkt_polygon):
    map_obj = Map()
    map_obj_out = map_obj.ship_map_by_polygon(wkt_polygon, sample_gdf)
    assert map_obj_out is not None, "Expected map object to be returned"

def test_ships_route_all(sample_gdf, tmp_path):
    geojson_path = tmp_path / "route.geojson"
    sample_gdf.to_file(geojson_path, driver="GeoJSON")
    map_obj = Map()
    result = map_obj.ships_route(str(geojson_path))
    assert hasattr(result, "_parent"), "Expected a valid folium map object"

def test_ships_route_with_invalid_mmsi(sample_gdf, tmp_path):
    geojson_path = tmp_path / "route.geojson"
    sample_gdf.to_file(geojson_path, driver="GeoJSON")
    map_obj = Map()
    result = map_obj.ships_route(str(geojson_path), mmsi=999999)
    assert result == "No ship found with that mmsi", "Expected message for invalid MMSI"

def test_plot_ship_heatmap(sample_gdf, tmp_path):
    geojson_path = tmp_path / "heatmap.geojson"
    sample_gdf.to_file(geojson_path, driver="GeoJSON")
    map_obj = Map()
    result = map_obj.plot_ship_heatmap(str(geojson_path))
    assert hasattr(result, "_parent"), "Expected a valid folium map object"

def test_filter_ships_by_invalid_polygon(sample_gdf):
    map_obj = Map()
    invalid_wkt = "POLY(0 0, 1 1, 1 0)"  # WKT invalid
    with pytest.raises(ValueError, match="Invalid WKT polygon format"):
        map_obj.filter_ships_by_polygon(invalid_wkt, sample_gdf)

def test_ship_by_mmsi(sample_gdf):
    map_obj = Map()
    result = map_obj.ship_by_mmsi( mmsi = 123456789, geojson_data=sample_gdf)
    assert hasattr(result, "_parent"), "Expected a valid folium map object"
