# import pytest
# #from src/maritimeviz/ import AISDatabase
# import pandas as pd
# import geopandas as gpd
# import os
# from shapely.geometry import Point
# import shutil
#
# from src.maritimeviz.ais_db import AISDatabase
# from . import logger
#
# file_path = "tests/ais_2016_07_28_aa"
# db_path ="test_db.duckdb"
#
#
# @pytest.fixture(scope="function")
# def setup_db():
#     """Fixture to create and clean up the test database."""
#     db = AISDatabase(db_path)
#     yield db
#
#     db.clear_cache()
#     db.close()
#
#     # Clean up exported files
#     for file in ["test_data.csv", "test_data.parquet", "test_data.kml", "test_data.xlsx"]:
#         if os.path.exists(file):
#             os.remove(file)
#
#
# # TODO(Thalia): Move to utility
# def check_file_exists():
#     print(f"Database file exists: {os.path.exists('test_db.duckdb')}")
#
# def test_initialize_existing_database_works(setup_db):
#    db = setup_db
#    conn = db.connection()
#    # TODO(Thalia): wrap in method and move to utilities
#    # tables = conn.execute(
#    #     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
#    # print(tables)
#    result = conn.execute("SELECT * FROM ais_msg_123 LIMIT 10").fetchdf()
#    print(result)
#
#    assert conn is not None
#    assert len(result) > 0
#
#
# def test_search_works(setup_db):
#     db = setup_db
#     conn = db.connection()
#     result_all = db.search()
#     print("Result (No filters):", result_all)
#     assert isinstance(result_all, gpd.GeoDataFrame)
#     assert not result_all.empty
#
#     # Search by valid MMSI → Should return 24 rows
#     result_mmsi = db.search(mmsi=9111254)
#     print("Result (MMSI 9111254):", result_mmsi)
#     assert isinstance(result_mmsi, gpd.GeoDataFrame)
#     assert not result_mmsi.empty
#     assert len(result_mmsi) == 24  # Expecting one row
#
#     # Search by non-existing MMSI → Should return an empty GeoDataFrame
#     result_invalid_mmsi = db.search(mmsi=9999999)
#     print("Result (Invalid MMSI):", result_invalid_mmsi)
#     assert isinstance(result_invalid_mmsi, gpd.GeoDataFrame)
#     assert result_invalid_mmsi.empty
#
#     # Search by date range (should match at least one row)
#     result_date_range = db.search(start_date="2016-07-27",
#                                   end_date="2016-07-29")
#     print("Result (Date Range 2016-07-28 to 2016-07-29):", result_date_range)
#     assert isinstance(result_date_range, gpd.GeoDataFrame)
#     assert not result_date_range.empty
#     assert len(result_date_range) >= 1  # Should have at least one row
#
#     # # Search by polygon bounds (bounding box containing a known point)
#     # polygon_bounds = "POLYGON((-93 29, -93 33, -89 33, -89 29, -93 29))"
#     # result_polygon = db.search(polygon_bounds=polygon_bounds)
#     # print("Result (Polygon Bounds):", result_polygon)
#     # assert isinstance(result_polygon, gpd.GeoDataFrame)
#     # assert not result_polygon.empty
#     # assert any(result_polygon.geometry.within(Point(30.0, -90.0)))  # Check if known point is inside
#
#
#
# def test_get_csv(setup_db):
#     """Test exporting AIS data to CSV format."""
#     db = setup_db
#     file_path = "test_data.csv"
#
#     result = db.get_csv(mmsi=9111254, file_path=file_path)
#
#     assert os.path.exists(file_path), "CSV file should be created"
#     assert "CSV saved at" in result, "CSV export function should return success message"
#
# def test_get_parquet(setup_db):
#     """Test exporting AIS data to Parquet format."""
#     db = setup_db
#     file_path = "test_data.parquet"
#
#     result = db.get_parquet(mmsi=9111254, file_path=file_path)
#
#     assert os.path.exists(file_path), "Parquet file should be created"
#     assert "Parquet file saved at" in result, "Parquet export function should return success message"
#
# def test_get_json(setup_db):
#     """Test exporting AIS data to JSON format."""
#     db = setup_db
#     file_path = "test_data.json"
#
#     result = db.get_json(mmsi=9111254, file_path=file_path)
#
#     assert os.path.exists(file_path)
#     assert len(result) >= 1
#
#
#
# def test_get_shapefile(setup_db):
#     db = setup_db
#     file_path = "test_shapefile"
#
#     try:
#         # Run the function to generate the shapefile
#         db.get_shapefile(file_path=file_path, mmsi=9111254)
#
#         # Ensure the folder and required files exist
#         assert os.path.exists(file_path), "Shapefile folder was not created"
#
#         # Check that the required shapefile components exist
#         expected_files = ["shp", "shx", "dbf", "prj"]
#         for ext in expected_files:
#             file_inside_folder = os.path.join(file_path,
#                                               f"{os.path.basename(file_path)}.{ext}")
#             assert os.path.exists(
#                 file_inside_folder), f"Missing {ext} file inside the shapefile folder"
#
#             # Attempt to load the shapefile
#             gdf = gpd.read_file(
#                 os.path.join(file_path, f"{os.path.basename(file_path)}.shp"))
#             assert not gdf.empty, "Generated shapefile is empty"
#
#     finally:
#         # Cleanup: Recursively delete the shapefile directory
#         if os.path.exists(file_path):
#             shutil.rmtree(file_path)
#
# def test_get_kml(setup_db):
#     db = setup_db
#     file_path = "test_data.kml"
#
#     result = db.get_kml(file_path=file_path, mmsi=9111254)
#
#     # Check if KML file is created
#     assert os.path.exists(file_path), "KML file was not created."
#
#     gdf = gpd.read_file(file_path)
#     assert not gdf.empty, "KML file should not be empty."
#
# def test_get_excel(setup_db):
#     db = setup_db
#     file_path = "test_data.xlsx"
#
#     result = db.get_excel(file_path=file_path, mmsi=9111254)
#
#     # Check if Excel file is created
#     assert os.path.exists(file_path), "Excel file was not created."
#
#     df = pd.read_excel(file_path)
#     assert not df.empty, "Excel file should not be empty."
#
# def test_get_wkt(setup_db):
#     db = setup_db
#
#     wkt_list = db.get_wkt(mmsi=9111254)
#
#     # Check that WKT list is not empty
#     assert isinstance(wkt_list, list), "WKT output should be a list."
#     assert len(wkt_list) > 0, "WKT list should not be empty."
#     assert "POINT" in wkt_list[0], "WKT should contain 'POINT'."
#
# '''
# def test_process_file():
#     db = AISDatabase()
#     db.process_file(file_path)
#
#     # Query the database to check row counts
#     row_count_123 = \
#     db.connection.execute("SELECT COUNT(*) FROM ais_msg_123").fetchone()[0]
#     row_count_5 = \
#     db.connection.execute("SELECT COUNT(*) FROM ais_msg_5").fetchone()[0]
#
#     # Assert that the tables are not empty
#     assert row_count_123 > 0, "Table ais_msg_123 should not be empty after processing."
#     #assert row_count_5 > 0, "Table ais_msg_5 should not be empty after processing."
#
#     db.close()
#         '''
import os
import re
import pytest
import duckdb
import pandas as pd
import geopandas as gpd
import shutil

from src.maritimeviz.ais_db import AISDatabase
from src.maritimeviz.constants import *

# Database and AIS files for testing
TEST_DB_PATH = "test_db.duckdb"
AIS_FILE_PATH = "tests/ais_2016_07_28_aa"



@pytest.fixture(scope="function")
def setup_existing_db():
    """
    Fixture to create and clean up a test AISDatabase instance
    with an existing db.
    """
    # Initialize the database (this will create tables and views)
    db = AISDatabase(TEST_DB_PATH) # existing db
    yield db
    # Clear cache if needed and close connection
    db.clear_cache()
    db.close()

    # Remove any exported files
    for fname in ["test_data.csv", "test_data.parquet", "test_data.json",
                  "ais_shapefile", "test_data.kml", "test_data.xlsx"]:
        if os.path.exists(fname):
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            else:
                os.remove(fname)

@pytest.fixture(scope="function")
def setup_new_db():
    """
    Fixture to create and clean up a test AISDatabase instance instantiated
    with default db path.
    """
    # Initialize empty database
    db = AISDatabase()  # existing db
    yield db
    # Clear cache if needed and close connection
    db.clear_cache()
    db.close()
    # Remove the test database file after tests run
    if os.path.exists("ais_data_1.duckdb"):
        os.remove("ais_data_1.duckdb")
    # Remove any exported files
    for fname in ["test_data.csv", "test_data.parquet", "test_data.json",
                  "ais_shapefile", "test_data.kml", "test_data.xlsx"]:
        if os.path.exists(fname):
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            else:
                os.remove(fname)

def test_db_default_name():
    # Reset the class-level counter
    AISDatabase._default_db_counter = 0

    name1 = AISDatabase._get_default_db_path()
    print(name1)
    name2 = AISDatabase._get_default_db_path()
    print(name2)

    # Check that the names follow the expected pattern.
    pattern = r"^ais_data_\d+\.duckdb$"
    assert re.match(pattern, name1), f"Default db name {name1} does not match the pattern."
    assert re.match(pattern, name2), f"Default db name {name2} does not match the pattern."

    # Check that the two names are different.
    assert name1 != name2, "Consecutive default db names should be different."

    # Check that the counter has increased by 2.
    assert AISDatabase._default_db_counter == 2, "Global counter should be incremented by 2 after two calls."

def test_global_views_exist(setup_new_db):
    """Test that the global views are created and return a DataFrame (even if empty)."""
    db = setup_new_db
    conn = db.connection()
    # Check for one of the views; adjust table/view names if needed.
    try:
        df_dynamic = conn.execute(
            "SELECT * FROM global_ais_dynamic LIMIT 1").fetchdf()
        df_static = conn.execute(
            "SELECT * FROM global_ais_static LIMIT 1").fetchdf()
        df_all = conn.execute(
            "SELECT * FROM global_ais_data LIMIT 1").fetchdf()
    except Exception as e:
        pytest.fail(f"Global view query failed: {e}")

    # They might be empty if no data is inserted, but the queries should succeed.
    assert isinstance(df_dynamic, pd.DataFrame)
    assert isinstance(df_static, pd.DataFrame)
    assert isinstance(df_all, pd.DataFrame)

class TestGlobalExports:
    """
    Testing global export methods in AISDatabase
    """

    def test_get_global_geojson(setup_existing_db):
        db = setup_existing_db
        # Call global geojson export with default parameters
        result = db.get_geojson(data="all")
        # Expect a dictionary (even if empty)
        assert isinstance(result, dict)


    def test_get_global_csv(setup_db):
        db = setup_db
        file_path = "test_data.csv"
        result = db.get_csv(file_path=file_path, data="all")
        # If no data exists, function should return a string indicating no data.
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping CSV file test.")
        # Otherwise, check the file exists.
        assert os.path.exists(file_path)
        # Load CSV and check type.
        df = pd.read_csv(file_path)
        assert isinstance(df, pd.DataFrame)


    def test_get_global_parquet(setup_db):
        db = setup_db
        file_path = "test_data.parquet"
        result = db.get_parquet(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping Parquet test.")
        assert os.path.exists(file_path)
        df = pd.read_parquet(file_path)
        assert isinstance(df, pd.DataFrame)


    def test_get_global_json(setup_db):
        db = setup_db
        file_path = "test_data.json"
        result = db.get_json(file_path=file_path, data="all")
        if isinstance(result, str) and result.startswith("No data"):
            pytest.skip("No data available to export; skipping JSON test.")
        # Check that the result is a dict and file exists
        assert isinstance(result, dict)
        assert os.path.exists(file_path)


    def test_get_global_shapefile(setup_db):
        db = setup_db
        folder_path = "ais_shapefile"
        result = db.get_shapefile(file_path=folder_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping Shapefile test.")
        # Check that the folder exists and contains a .shp file.
        assert os.path.exists(folder_path)
        shp_files = [f for f in os.listdir(folder_path) if f.endswith(".shp")]
        assert len(shp_files) > 0, "No shapefile found in the folder."
        # Attempt to read the shapefile.
        gdf = gpd.read_file(os.path.join(folder_path, shp_files[0]))
        assert isinstance(gdf, gpd.GeoDataFrame)


    def test_get_global_kml(setup_db):
        db = setup_db
        file_path = "test_data.kml"
        result = db.get_kml(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping KML test.")
        assert os.path.exists(file_path)
        gdf = gpd.read_file(file_path)
        assert isinstance(gdf, gpd.GeoDataFrame)


    def test_get_global_excel(setup_db):
        db = setup_db
        file_path = "test_data.xlsx"
        result = db.get_excel(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping Excel test.")
        assert os.path.exists(file_path)
        df = pd.read_excel(file_path)
        assert isinstance(df, pd.DataFrame)


    def test_get_global_wkt(setup_db):
        db = setup_db
        result = db.get_wkt(data="all")
        if isinstance(result, str) and result.startswith("No data"):
            pytest.skip("No data available to export; skipping WKT test.")
        assert isinstance(result, list)
        # Optionally, check that at least one WKT string contains "POINT"
        assert any("POINT" in wkt for wkt in result)

class TestClassAMessages:
    def test_class_a_search(self, setup_existing_db):
        db = setup_existing_db
        processor = db.typeA()
        df = processor.search(mmsi=9111254)
        # Check that the returned data is a GeoDataFrame (or DataFrame) and not empty
        assert isinstance(df, gpd.GeoDataFrame)
        # can also check expected number of rows but know the test dataset

class TestClassBMessages:
    def test_class_b_search_dynamic(self, setup_existing_db):
        db = setup_existing_db
        processor = db.typeB()
        df = processor.search(mmsi=9111254, start_date="2016-07-27", end_date="2016-07-29")
        assert isinstance(df, pd.DataFrame)


    def test_class_b_static_info(self, setup_existing_db):
        db = setup_existing_db
        processor = db.typeB()
        df = processor.static_info(mmsi=9111254)
        assert isinstance(df, pd.DataFrame)



