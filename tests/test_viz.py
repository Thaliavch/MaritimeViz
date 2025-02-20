import leafmap
import pytest
#from src/maritimeviz/ import AISDatabase
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
import shutil
from leafmap import *
from folium import *
from MaritimeViz.src.maritimeviz.ais_db import AISDatabase
from . import logger

file_path = "tests/ais_2016_07_28_aa"
db_path ="test_db.duckdb"


# def test_map_all(setup_db):
#     db = setup_db  # This is your database setup fixture
#
#     # Create a small test GeoJSON
#     test_geojson = {
#         "type": "FeatureCollection",
#         "features": [
#             {
#                 "type": "Feature",
#                 "geometry": {
#                     "type": "Point",
#                     "coordinates": [-80.191790, 25.761680]
#                 },
#                 "properties": {
#                     "mmsi": 123456789,
#                     "speed": 10.5
#                 }
#             },
#             {
#                 "type": "Feature",
#                 "geometry": {
#                     "type": "Point",
#                     "coordinates": [-81.694360, 26.142036]
#                 },
#                 "properties": {
#                     "mmsi": 987654321,
#                     "speed": 15.2
#                 }
#             }
#         ]
#     }
#
#     # Run the function to generate the map
#     try:
#         result_map = db.map_all(test_geojson)
#
#         # Ensure that a Folium map object is created
#         assert isinstance(result_map, folium.Map), "map_all did not return a valid Folium Map object"
#
#         # Check that markers exist in the map
#         map_html = result_map.get_root().render()
#         assert "marker" in map_html.lower(), "No markers were added to the map"
#
#         print("Test passed: Map was successfully created with markers.")
#
#     except Exception as e:
#         pytest.fail(f"map_all function raised an exception: {e}")
#
#
# def test_map_all_missing_features(setup_db):
#     db = setup_db
#     m = leafmap.Map()
#     assert isinstance(m)
#     assert db

