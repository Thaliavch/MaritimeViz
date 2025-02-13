import pytest
#from src/maritimeviz/ import AISDatabase
import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point

from src.maritimeviz.ais_db import AISDatabase
from . import logger

file_path = "tests/ais_2016_07_28_aa"
db_path ="test_db.duckdb"


# TODO(Thalia): Move to utility
def check_file_exists():
    print(f"Database file exists: {os.path.exists('test_db.duckdb')}")

def test_initialize_existing_database_works():
   db = AISDatabase("test_db.duckdb")
   conn = db.connection()
   # TODO(Thalia): wrap in method and move to utilities
   # tables = conn.execute(
   #     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
   # print(tables)
   result = conn.execute("SELECT * FROM ais_msg_123 LIMIT 10").fetchdf()
   print(result)

   assert conn is not None
   assert len(result) > 0

   db.close()

def test_search_works():
    db = AISDatabase("test_db.duckdb")
    conn = db.connection()
    result_all = db.search()
    print("Result (No filters):", result_all)
    assert isinstance(result_all, gpd.GeoDataFrame)
    assert not result_all.empty

    # Search by valid MMSI → Should return 24 rows
    result_mmsi = db.search(mmsi=9111254)
    print("Result (MMSI 9111254):", result_mmsi)
    assert isinstance(result_mmsi, gpd.GeoDataFrame)
    assert not result_mmsi.empty
    assert len(result_mmsi) == 24  # Expecting one row

    # Search by non-existing MMSI → Should return an empty GeoDataFrame
    result_invalid_mmsi = db.search(mmsi=9999999)
    print("Result (Invalid MMSI):", result_invalid_mmsi)
    assert isinstance(result_invalid_mmsi, gpd.GeoDataFrame)
    assert result_invalid_mmsi.empty

    # Search by date range (should match at least one row)
    result_date_range = db.search(start_date="2016-07-27",
                                  end_date="2016-07-29")
    print("Result (Date Range 2016-07-28 to 2016-07-29):", result_date_range)
    assert isinstance(result_date_range, gpd.GeoDataFrame)
    assert not result_date_range.empty
    assert len(result_date_range) >= 1  # Should have at least one row

    # # Search by polygon bounds (bounding box containing a known point)
    # polygon_bounds = "POLYGON((-93 29, -93 33, -89 33, -89 29, -93 29))"
    # result_polygon = db.search(polygon_bounds=polygon_bounds)
    # print("Result (Polygon Bounds):", result_polygon)
    # assert isinstance(result_polygon, gpd.GeoDataFrame)
    # assert not result_polygon.empty
    # assert any(result_polygon.geometry.within(Point(30.0, -90.0)))  # Check if known point is inside

    db.clear_cache()
    db.close()

'''
def test_process_file():
    db = AISDatabase()
    db.process_file(file_path)

    # Query the database to check row counts
    row_count_123 = \
    db.connection.execute("SELECT COUNT(*) FROM ais_msg_123").fetchone()[0]
    row_count_5 = \
    db.connection.execute("SELECT COUNT(*) FROM ais_msg_5").fetchone()[0]

    # Assert that the tables are not empty
    assert row_count_123 > 0, "Table ais_msg_123 should not be empty after processing."
    #assert row_count_5 > 0, "Table ais_msg_5 should not be empty after processing."

    db.close()
        '''
