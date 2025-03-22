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
TEST_DB_PATH = "ais_data .duckdb"
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
    for fname in ["test_data.geojson", "test_data.csv", "test_data.parquet", "test_data.json",
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

def test_initialize_existing_database_works(setup_existing_db):
   db = setup_existing_db
   conn = db.connection()
   # TODO(Thalia): wrap in method and move to utilities
   # tables = conn.execute(
   #     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
   # print(tables)
   result = conn.execute("SELECT * FROM ais_msg_123 LIMIT 10").fetchdf()
   print(result)

   assert conn is not None
   assert len(result) > 0

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
    def test_get_global_geojson(self, setup_existing_db):
        db = setup_existing_db
        result = db.get_geojson(data="all")
        # Check that result is a dictionary
        assert isinstance(result, dict)
        # Verify it has a FeatureCollection structure
        assert result.get("type") == "FeatureCollection"
        assert isinstance(result.get("features"), list)

    def test_get_global_csv(self, setup_existing_db):
        db = setup_existing_db
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


    def test_get_global_parquet(self, setup_existing_db):
        db = setup_existing_db
        file_path = "test_data.parquet"
        result = db.get_parquet(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping Parquet test.")
        assert os.path.exists(file_path)
        df = pd.read_parquet(file_path)
        assert isinstance(df, pd.DataFrame)


    def test_get_global_json(self, setup_existing_db):
        db = setup_existing_db
        file_path = "test_data.json"
        result = db.get_json(file_path=file_path, data="all")
        if isinstance(result, str) and result.startswith("No data"):
            pytest.skip("No data available to export; skipping JSON test.")
        # Check that the result is a dict and file exists
        assert isinstance(result, dict)
        assert os.path.exists(file_path)


    def test_get_global_shapefile(self, setup_existing_db):
        db = setup_existing_db
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


    def test_get_global_kml(self, setup_existing_db):
        db = setup_existing_db
        file_path = "test_data.kml"
        result = db.get_kml(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping KML test.")
        assert os.path.exists(file_path)
        gdf = gpd.read_file(file_path)
        assert isinstance(gdf, gpd.GeoDataFrame)


    def test_get_global_excel(self, setup_existing_db):
        db = setup_existing_db
        file_path = "test_data.xlsx"
        result = db.get_excel(file_path=file_path, data="all")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping Excel test.")
        assert os.path.exists(file_path)
        df = pd.read_excel(file_path)
        assert isinstance(df, pd.DataFrame)


    def test_get_global_wkt(self, setup_existing_db):
        db = setup_existing_db
        result = db.get_wkt(data="all")
        if isinstance(result, str) and result.startswith("No data"):
            pytest.skip("No data available to export; skipping WKT test.")
        assert isinstance(result, list)
        # Optionally, check that at least one WKT string contains "POINT"
        assert any("POINT" in wkt for wkt in result)

def test_dynamic_table_has_data(setup_existing_db):
    db = setup_existing_db
    conn = db.connection()
    df = conn.execute("SELECT mmsi, id FROM ais_msg_123 LIMIT 3").fetchdf()
    print("ais_msg_123 sample:", df)
    assert not df.empty, "Expected ais_msg_123 to have data."


class TestClassAMessages:
    def test_search_works(self, setup_existing_db):
        db = setup_existing_db
        db.clear_cache()
        processor = db.typeA()

        # 1. Test search with no filters.
        result_all = processor.search()
        print("Type A (No filters):", result_all)
        assert isinstance(result_all, gpd.GeoDataFrame), "Expected a GeoDataFrame ."
        assert not result_all.empty, "Expected non-empty GeoDataFrame."

        # 2. Search by a valid MMSI (e.g., 9111254).
        result_mmsi = processor.search(mmsi=9111254)
        print("Type A (MMSI 9111254):", result_mmsi)
        assert isinstance(result_mmsi, gpd.GeoDataFrame), "Expected a GeoDataFrame for a valid MMSI search."
        assert not result_mmsi.empty, "Expected non-empty result for MMSI 9111254."
        # Adjust expected row count as appropriate (example: expecting 24 rows)
        assert len(result_mmsi) == 6, f"Expected 6 rows for MMSI 9111254, got {len(result_mmsi)}."

        # 3. Search by non-existing MMSI should return an empty GeoDataFrame.
        result_invalid_mmsi = processor.search(mmsi=9999999)
        print("Type A (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, gpd.GeoDataFrame), "Expected a GeoDataFrame even for an invalid MMSI."
        assert result_invalid_mmsi.empty, "Expected an empty GeoDataFrame for an invalid MMSI."

        # 4. Search by date range (should return at least one row).
        result_date_range = processor.search(start_date="2016-07-27", end_date="2016-07-29")
        print("Type A (Date Range):", result_date_range)
        assert isinstance(result_date_range, gpd.GeoDataFrame), "Expected a GeoDataFrame for a date range search."
        assert not result_date_range.empty, "Expected non-empty GeoDataFrame for the given date range."
        assert len(result_date_range) >= 1, "Expected at least one row for the given date range."

        # 5. Optionally: Search by polygon bounds.
        # Uncomment and adjust if your processor supports filtering by spatial bounds.
        # polygon_bounds = "POLYGON((-93 29, -93 33, -89 33, -89 29, -93 29))"
        # result_polygon = processor.search(polygon_bounds=polygon_bounds)
        # print("Type A (Polygon Bounds):", result_polygon)
        # assert isinstance(result_polygon, gpd.GeoDataFrame), "Expected a GeoDataFrame for polygon bounds search."
        # assert not result_polygon.empty, "Expected non-empty result for the given polygon bounds."
        # Example check: verify a known point is within at least one feature (adjust as needed)
        # known_point = Point(-90.0, 30.0)
        # assert any(result_polygon.geometry.apply(lambda geom: geom.within(known_point))),
        #        "Expected at least one geometry to contain the known point."


class TestClassBMessages:
    def test_search_works_dynamic(self, setup_existing_db):
        db = setup_existing_db
        processor = db.typeB()

        # 1. Test search with no filters.
        result_all = processor.search()
        print("Type B (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), "Expected a DataFrame when no filters are provided for Type B."
        assert not result_all.empty, "Expected non-empty DataFrame when no filters are applied for Type B."

        # 2. Search by valid MMSI (with a date range).
        result_mmsi = processor.search(mmsi=9111254, start_date="2016-07-27", end_date="2016-07-29")
        print("Type B (MMSI 9111254, Date Range):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for a valid MMSI search in Type B."
        assert not result_mmsi.empty, "Expected non-empty result for MMSI 9111254 in Type B."

        # 3. Search by non-existing MMSI should return an empty DataFrame.
        result_invalid_mmsi = processor.search(mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29")
        print("Type B (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, pd.DataFrame), "Expected a DataFrame even for an invalid MMSI in Type B."
        assert result_invalid_mmsi.empty, "Expected an empty DataFrame for an invalid MMSI in Type B."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-27", end_date="2016-07-29")
        print("Type B (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for a date range search in Type B."
        assert not result_date_range.empty, "Expected non-empty DataFrame for the given date range in Type B."



