import os
import re
import pytest
import duckdb
import pandas as pd
import geopandas as gpd
import shutil
from typing import Optional
import json

from src.maritimeviz.ais_db import AISDatabase
from src.maritimeviz.constants import *

# Database and AIS files for testing
TEST_DB_PATH = "ais_data .duckdb"
AIS_FILE_PATH = "ais_2016_07_28_aa"



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
def setup_new_db(request):
    """
    Fixture to create and clean up a test AISDatabase instance instantiated
    with default db path.
    """
    # Initialize empty database
    db_path = request.param if hasattr(request, "param") else None
    if db_path:
        db = AISDatabase(db_path)
    else:
        db = AISDatabase()

    yield db
    # Clear cache if needed and close connection
    db.clear_cache()
    db.close()
    # Remove the test database files after tests run
    for fname in ["ais_data_1.duckdb", "ais_class_A_only.duckdb", "ais_class_B_only.duckdb"]:
        if os.path.exists(fname):
            os.remove(fname)
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
        processor = db.class_a()

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

        # Search by polygon bounds.
        # polygon_bounds = "POLYGON((-93 29, -93 33, -89 33, -89 29, -93 29))"
        # result_polygon = processor.search(polygon_bounds=polygon_bounds)
        # print("Type A (Polygon Bounds):", result_polygon)
        # assert isinstance(result_polygon, gpd.GeoDataFrame), "Expected a GeoDataFrame for polygon bounds search."
        # assert not result_polygon.empty, "Expected non-empty result for the given polygon bounds."
        # Example check: verify a known point is within at least one feature (adjust as needed)
        # known_point = Point(-90.0, 30.0)
        # assert any(result_polygon.geometry.apply(lambda geom: geom.within(known_point))),
        #        "Expected at least one geometry to contain the known point."

    # TODO(Thalia) in the class ensure user enters duckdb file extension otherwise add the extension
    @pytest.mark.parametrize("setup_new_db", ["ais_class_A_only.duckdb"],
                             indirect=True)
    def test_process_classA(self, setup_new_db):
        db = setup_new_db
        processorA = db.class_a()
        processorA.process(AIS_FILE_PATH)

        conn = db.connection()
        count_123 = \
        conn.execute("SELECT COUNT(*) FROM ais_msg_123").fetchone()[0]
        # Static table for Class A (ais_msg_5) has no data for the current testing file
        count_5 = conn.execute("SELECT COUNT(*) FROM ais_msg_5").fetchone()[0]

        print("Rows in ais_msg_123:", count_123)
        print("Rows in ais_msg_5:", count_5)

        assert count_123 > 0, "Expected ais_msg_123 to have data after processing Class A messages."
        assert count_5 == 0, "Expected ais_msg_5 to have no data after processing Class A messages."


class TestClassBMessages:
    def test_search_works(self, setup_existing_db):
        db = setup_existing_db
        processor = db.class_b()

        # 1. Test search with no filters.
        result_all = processor.search()
        print("Type B (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), "Expected a DataFrame when no filters are provided for Type B."
        assert not result_all.empty, "Expected non-empty DataFrame when no filters are applied for Type B."

        # 2. Search by valid MMSI
        result_mmsi = processor.search(mmsi=338097623)
        print("Type B (MMSI 338097623, Date Range):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for a valid MMSI search in Type B."
        assert not result_mmsi.empty, "Expected non-empty result for MMSI 338097623 in Type B."

        # 3. Search by non-existing MMSI should return an empty DataFrame.
        result_invalid_mmsi = processor.search(mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29")
        print("Type B (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, pd.DataFrame), "Expected a DataFrame even for an invalid MMSI in Type B."
        assert result_invalid_mmsi.empty, "Expected an empty DataFrame for an invalid MMSI in Type B."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-26", end_date="2016-07-30")
        print("Type B (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for a date range search in Type B."
        assert not result_date_range.empty, "Expected non-empty DataFrame for the given date range in Type B."

    @pytest.mark.parametrize("setup_new_db", ["ais_class_B_only.duckdb"],
                             indirect=True)
    def test_process_classB(self, setup_new_db):
        db = setup_new_db
        db.clear_cache()
        processorB = db.class_b()
        # Process the sample file with the Class B processor.
        processorB.process(AIS_FILE_PATH)

        conn = db.connection()
        # Check that the dynamic table for Class B (ais_msg_18_19) has data.
        count_18_19 = \
        conn.execute("SELECT COUNT(*) FROM ais_msg_18_19").fetchone()[0]
        # Check that the static table for Class B (ais_msg_24) has data.
        count_24 = conn.execute("SELECT COUNT(*) FROM ais_msg_24").fetchone()[
            0]

        print("Rows in ais_msg_18_19:", count_18_19)
        print("Rows in ais_msg_24:", count_24)

        assert count_18_19 > 0, "Expected ais_msg_18_19 to have data after processing Class B messages."
        assert count_24 > 0, "Expected ais_msg_24 to have data after processing Class B messages."

class TestLongRangeMessages:
    def test_private_insert_longrange_message(self, setup_new_db):
        """
        Test inserting a Message 27 (Long-range AIS broadcast) into ais_msg_27,
        then verify that the row is present.
        """
        db = setup_new_db
        processor = db.long_range()

        sample_msg_27 = {
            "id": 27,
            "repeat_indicator": 3,
            "mmsi": 123456789,
            "position_accuracy": 1,
            "raim": True,
            "nav_status": 0,  # underway using engine
            "x": -70.1234,
            "y": 40.9876,
            "sog": 18,
            "cog": 90,
            "gnss": True,
            "spare": 0,
            # We don't have vessel_type in the raw data; it's determined by guess_vessel_type
            # The rest are TagBlock fields:
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 9999},
            "tagblock_line_count": 444,
            "tagblock_station": "SAT-GOM",
            "tagblock_timestamp": 1699999999
        }

        # Insert sample
        processor._insert_message(sample_msg_27)

        # Verify insertion
        df = db.connection().execute("SELECT * FROM ais_msg_27").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_27, got {len(df)}."
        assert df.loc[0, "id"] == 27, "Message ID should be 27."
        # Optionally check vessel_type was computed (assuming guess_vessel_type is returning something).
        assert df.loc[0, "vessel_type"] is not None, "vessel_type should not be None."

    def test_process_works(self, setup_new_db):
        db = setup_new_db
        processor = db.long_range()

        processor.process(AIS_FILE_PATH)

        conn = db.connection()

        count_27 = \
            conn.execute("SELECT COUNT(*) FROM ais_msg_27").fetchone()[0]

        print("Rows in ais_msg_27:", count_27)

        assert count_27 > 0, "Expected ais_msg27 to have data after processing long range messages."

    def test_search_works(self, setup_existing_db):
        # mmsi 577305000
        db = setup_existing_db
        processor = db.long_range()

        # 1. Test search with no filters.
        result_all = processor.search()
        print("Long Range (No filters):", result_all)
        assert isinstance(result_all,
                          pd.DataFrame), "Expected a DataFrame when no filters are provided for Long Range."
        assert not result_all.empty, "Expected non-empty DataFrame when no filters are applied for Long Range."

        # 2. Search by valid MMSI
        result_mmsi = processor.search(mmsi=577305000)
        print("Long Range (MMSI 577305000, Date Range):", result_mmsi)
        assert isinstance(result_mmsi,
                          pd.DataFrame), "Expected a DataFrame for a valid MMSI."
        assert not result_mmsi.empty, "Expected non-empty result."

        # 3. Search by non-existing MMSI should return an empty DataFrame.
        result_invalid_mmsi = processor.search(mmsi=9999999,
                                               start_date="2016-07-27",
                                               end_date="2016-07-29")
        print("Type B (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi,
                          pd.DataFrame), "Expected a DataFrame even for an invalid MMSI ."
        assert result_invalid_mmsi.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-26",
                                             end_date="2016-07-30")
        print("Type B (Date Range):", result_date_range)
        assert isinstance(result_date_range,
                          pd.DataFrame), "Expected a DataFrame for a date range."
        assert not result_date_range.empty, "Expected non-empty DataFrame for the given date range."


# Test for Addressed Binary Messages (Type 6)
class TestAddressedBinaryHandler:
    def test_insert_msg(self, setup_new_db):
        """
        Test inserting an Addressed Binary Message into ais_msg_6,
        then verify that the row is present.
        """
        db = setup_new_db
        processor = db.addressed_binary()

        sample_msg = {
            "id": 6,
            "repeat_indicator": 0,
            "mmsi": 123456789,
            "spare": 0,
            "spare2": 1,
            "dac": 1,
            "fi": 2,  # using 'fi' in place of fid
            "x": -70.1234,
            "y": 40.9876,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 9999},
            "tagblock_line_count": 444,
            "tagblock_station": "SAT-ATL",
            "tagblock_timestamp": 1699999999,
            "payload": "binary_payload_example"  # additional data stored as JSON
        }

        # Insert the sample message
        processor._insert_message(sample_msg)

        # Verify insertion by directly querying the table
        df = db.connection().execute("SELECT * FROM ais_msg_6").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_6, got {len(df)}."
        assert df.loc[0, "id"] == 6, "Message ID should be 6."

    # todo(Thalia) Current file does not have any message of type 6, find a new one
    # def test_process_works(self, setup_new_db):
    #     """
    #     Test that processing a file inserts messages into ais_msg_6.
    #     """
    #     db = setup_new_db
    #     processor = db.addressed_binary()
    #
    #     processor.process(AIS_FILE_PATH)
    #
    #     conn = db.connection()
    #     count = conn.execute("SELECT COUNT(*) FROM ais_msg_6").fetchone()[0]
    #     print("Rows in ais_msg_6:", count)
    #     assert count > 0, "Expected ais_msg_6 table to have data after processing."
    #
    # def test_search_works(self, setup_existing_db):
    #     """
    #     Test search functionality for Addressed Binary messages.
    #     """
    #     db = setup_existing_db
    #     processor = db.addressed_binary()
    #
    #     # 1. Search with no filters.
    #     result_all = processor.search()
    #     print("Addressed Binary (No filters):", result_all)
    #     assert isinstance(result_all, pd.DataFrame), "Expected a DataFrame with no filters."
    #
    #     # 2. Search by a valid MMSI.
    #     result_mmsi = processor.search(mmsi=123456789)
    #     print("Addressed Binary (MMSI 123456789):", result_mmsi)
    #     assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for valid MMSI."
    #
    #     # 3. Searching for a non-existent MMSI should return an empty DataFrame.
    #     result_invalid = processor.search(mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29")
    #     print("Addressed Binary (Invalid MMSI):", result_invalid)
    #     assert isinstance(result_invalid, pd.DataFrame), "Expected a DataFrame for an invalid MMSI."
    #     assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."
    #
    #     # 4. Search by date range.
    #     result_date_range = processor.search(start_date="2023-10-01", end_date="2023-12-31")
    #     print("Addressed Binary (Date Range):", result_date_range)
    #     assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for the date range."


# Test for Broadcast Text Messages (Type 8)
class TestBroadcastTextHandler:
    def test_insert_msg(self, setup_new_db):
        """
        Test inserting a Broadcast Text Message (type 8) using a real message extracted from a file.
        This verifies that the core columns are inserted and that leftover/application-specific data
        is stored correctly in the application_data column.
        """
        db = setup_new_db
        processor = db.broadcast_text()

        # Real extracted message from a file:
        sample_msg = {
            "id": 8,
            "repeat_indicator": 0,
            "mmsi": 993161005,
            "spare": 0,
            "dac": 1,
            "fi": 11,  # will be used to determine 'fid'
            "x": -125.62686666666667,
            "y": 48.8853,
            "wind_ave": 15,
            "wind_gust": 18,
            "wind_dir": 261,
            "wind_gust_dir": 18,
            "air_temp": 42.29999923706055,
            "rel_humid": 127,
            "dew_point": 31.100000381469727,
            "air_pres": 1311.0,
            "air_pres_trend": 3,
            "horz_vis": 25.5,
            "water_level": 41.099998474121094,
            "water_level_trend": 3,
            "surf_cur_speed": 0.20000000298023224,
            "surf_cur_dir": 237,
            "cur_speed_2": 0.10000000149011612,
            "cur_dir_2": 110,
            "cur_depth_2": 15,
            "cur_speed_3": 0.30000001192092896,
            "cur_dir_3": 159,
            "cur_depth_3": 30,
            "wave_height": 1.100000023841858,
            "wave_period": 7,
            "wave_dir": 263,
            "swell_height": 2.0,
            "swell_period": 63,
            "swell_dir": 511,
            "sea_state": 15,
            "water_temp": 41.099998474121094,
            "precip_type": 7,
            "ice": 3,
            "ext_water_level": 63,
            "spare2": 63,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 3213},
            "tagblock_line_count": 3915,
            "tagblock_station": "D13MN-PS-BAHBS1",
            "tagblock_timestamp": 1469665805
        }

        # Insert the sample message
        processor._insert_message(sample_msg)

        # Verify insertion by querying the table for type 8 messages
        df = db.connection().execute("SELECT * FROM ais_msg_8").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_8, got {len(df)}."
        # Check core columns
        assert df.loc[0, "id"] == 8, "Message ID should be 8."
        assert df.loc[0, "repeat_indicator"] == 0, "repeat_indicator should be 0."
        assert df.loc[0, "mmsi"] == 993161005, "mmsi should be 993161005."
        # 'fid' is determined from 'fi'
        assert df.loc[0, "fid"] == 11, "fid should be 11."
        # Verify coordinates
        assert abs(df.loc[0, "x"] - (-125.62686666666667)) < 1e-8, "x coordinate is incorrect."
        assert abs(df.loc[0, "y"] - 48.8853) < 1e-8, "y coordinate is incorrect."

        # Check that application_data contains the leftover fields (e.g., wind_ave, wind_gust, etc.)
        app_data = json.loads(df.loc[0, "application_data"])
        assert "wind_ave" in app_data, "Expected 'wind_ave' in application_data."
        assert app_data["wind_ave"] == 15, "wind_ave should be 15."
        assert "air_temp" in app_data, "Expected 'air_temp' in application_data."
        assert abs(app_data["air_temp"] - 42.29999923706055) < 1e-8, "air_temp is incorrect."
        # Optionally, check for other keys as needed:
        for key in ["wind_gust", "wind_dir", "water_level", "wave_height"]:
            assert key in app_data, f"Expected '{key}' in application_data."

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_8.
        """
        db = setup_new_db
        processor = db.broadcast_text()

        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_8").fetchone()[0]
        print("Rows in ais_msg_8:", count)
        assert count > 0, "Expected ais_msg_8 table to have data after processing."

    def test_search_works(self, setup_existing_db):
        """
        Test search functionality for Broadcast Text messages.
        mmsi: 366853070
        """
        db = setup_existing_db
        processor = db.broadcast_text()

        # 1. Search with no filters.
        result_all = processor.search()
        print("Broadcast Text (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), "Expected a DataFrame with no filters."
        assert not result_all.empty, "Expected non-empty result."

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=366853070)
        print("Broadcast Text (MMSI 366853070):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for valid MMSI."
        assert not result_mmsi.empty, "Expecting messages with mmsi: 366853070"

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(mmsi=111111111, start_date="2016-07-27", end_date="2016-07-29")
        print("Broadcast Text (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), "Expected a DataFrame for an invalid MMSI."
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-27", end_date="2016-07-29")
        print("Broadcast Text (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for the date range."
        assert not result_date_range.empty, "Expecting data for given date ..."


# Test for Short Binary Messages (Types 25/26)
class TestShortBinaryHandler:
    def test_insert_msg(self, setup_new_db):
        """
        Test inserting a Short Binary Message into ais_msg_25_26,
        then verify that the row is present.
        """
        db = setup_new_db
        processor = db.short_binary()

        sample_msg = {
            "id": 25,  # or 26
            "repeat_indicator": 2,
            "mmsi": 444444444,
            "dest_mmsi": 333333333,
            "sync_state": 1,
            "x": -60.9876,
            "y": 45.6789,
            "tagblock_group": {"sentence": 3, "groupsize": 1, "id": 7777},
            "tagblock_line_count": 222,
            "tagblock_station": "SAT-LAX",
            "tagblock_timestamp": 1697777777,
            "binary_data": "101010"  # additional binary payload data
        }

        # Insert sample message
        processor._insert_message(sample_msg)

        # Verify insertion by directly querying the table
        df = db.connection().execute("SELECT * FROM ais_msg_25_26").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_25_26, got {len(df)}."
        assert df.loc[0, "id"] == 25, "Message ID should be 25."

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_25_26.
        """
        db = setup_new_db
        processor = db.short_binary()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_25_26").fetchone()[0]
        print("Rows in ais_msg_25_26:", count)
        assert count > 0, "Expected ais_msg_25_26 table to have data after processing."

    def test_search_works(self, setup_existing_db):
        """
        Test search functionality for Short Binary messages.
        Note: The search() method here uses a query on 'ais_msg_21' which may be a discrepancy.
        Adjust expectations if the implementation is corrected.
        mmsi: 367080550
        """
        db = setup_existing_db
        processor = db.short_binary()

        # 1. Search with no filters.
        result_all = processor.search()
        print("Short Binary (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), "Expected a DataFrame with no filters."
        assert not result_all.empty, "Expecting non empty dataframe"

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=367080550)
        print("Short Binary (MMSI 367080550):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for valid MMSI."
        assert not result_mmsi.empty, "Expected one row for mmsi 367080550"

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29")
        print("Short Binary (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), "Expected a DataFrame for an invalid MMSI."
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-27", end_date="2016-07-29")
        print("Short Binary (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for the date range."
        assert not result_date_range.empty, "Expected data for given dates"


class TestAidToNavigationMessages:
    def test_insert_aton_message(self, setup_new_db):
        """
        Test inserting a Message 21 (AtoN) into ais_msg_21,
        then verifying the row is present.
        """
        db = setup_new_db
        processor = db.aton()

        sample_msg_21 = {
            "id": 21,
            "repeat_indicator": 0,
            "mmsi": 993123456,  # Aton MMSIs typically start with 993...
            "spare": 0,
            "aton_type": 9,  # e.g., "Beacon, Cardinal N"
            "name": "TEST BUOY",
            "position_accuracy": 1,
            "x": -80.1111,
            "y": 26.2222,
            "dim_a": 5,
            "dim_b": 5,
            "dim_c": 2,
            "dim_d": 2,
            "fix_type": 1,
            "timestamp": 55,
            "off_pos": False,
            "aton_status": 0,
            "raim": False,
            "virtual_aton": False,
            "assigned_mode": False,
            "tagblock_group": {"sentence": 1, "id": 101},
            "tagblock_line_count": 123,
            "tagblock_station": "COAST-ATON",
            "tagblock_timestamp": 1600010000
        }

        processor._insert_message(sample_msg_21)

        df = db.connection().execute("SELECT * FROM ais_msg_21").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row, got {len(df)}."
        assert df.loc[0, "mmsi"] == 993123456, "AtoN MMSI mismatch."
        assert df.loc[0, "name"] == "TEST BUOY"

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_21.
        """
        db = setup_new_db
        processor = db.aton()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT count(*) FROM ais_msg_21").fetchone()[0]
        print("Rows in ais_msg_21:", count)
        assert count > 0, "Expected ais_msg_21 table to have data after processing."

    # todo(thalia): add to search based of aton identifier
    def test_search_works(self, setup_existing_db):
        """
        Test search functionality for AtoN messages.
        Using mmsi: 993672272 from the inserted test message.
        """
        db = setup_existing_db
        processor = db.aton()

        # 1. Search with no filters.
        result_all = processor.search()
        print("AtoN (No filters):", result_all)
        assert isinstance(result_all,
                          pd.DataFrame), "Expected a DataFrame with no filters."
        assert not result_all.empty, "Expected non-empty DataFrame when no filters are applied."

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=993672272)
        print("AtoN (MMSI 993672272):", result_mmsi)
        assert isinstance(result_mmsi,
                          pd.DataFrame), "Expected a DataFrame for valid MMSI."
        assert not result_mmsi.empty, "Expected at least one row for mmsi 993123456."

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(mmsi=9999999,
                                          start_date="2016-07-27",
                                          end_date="2016-07-29")
        print("AtoN (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid,
                          pd.DataFrame), "Expected a DataFrame for an invalid MMSI."
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-27",
                                             end_date="2016-07-29")
        print("AtoN (Date Range):", result_date_range)
        assert isinstance(result_date_range,
                          pd.DataFrame), "Expected a DataFrame for the date range."
        assert not result_date_range.empty, "Expected data for the given dates."



class TestBaseStationMessages:
    def test_insert_base_station_message(self, setup_new_db):
        """
        Test inserting a Message 4 (Base Station Report) into ais_msg_4,
        then verify the row is present.
        Base station MMSI typically 00MIDxxxxx
        """
        db = setup_new_db
        processor = db.base_station()

        sample_msg_4 = {
            "id": 4,
            "repeat_indicator": 0,
            "mmsi": 3669707,
            "year": 2023,
            "month": 8,
            "day": 15,
            "hour": 14,
            "minute": 2,
            "second": 10,
            "position_accuracy": 1,
            "x": -122.1234,
            "y": 37.8765,
            "fix_type": 1,
            "transmission_ctl": 0,
            "spare": 0,
            "raim": True,
            "sync_state": 0,
            "slot_timeout": 7,
            "slot_offset": 15,
            "slot_number": None,
            "received_stations": None,
            "tagblock_group": {"sentence": 1, "id": 202},
            "tagblock_line_count": 456,
            "tagblock_station": "BASE-STN-TEST",
            "tagblock_timestamp": 1600020000
        }

        processor._insert_message(sample_msg_4)

        df = db.connection().execute("SELECT * FROM ais_msg_4").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row, got {len(df)}."
        assert df.loc[0, "mmsi"] == 3669707 or df.loc[0, "mmsi"] == 3669707, \
            "MMSI mismatch, check your test value."
        assert df.loc[0, "year"] == 2023, "Year mismatch."

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_4.
        """
        db = setup_new_db
        processor = db.base_station()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_4").fetchone()[0]
        print("Rows in ais_msg_4:", count)
        assert count > 0, "Expected ais_msg_4 table to have data after processing."

    def test_search_works(self, setup_existing_db):
        """
        Test search functionality for Base Station messages.
        For example, using MMSI 3660619 (as in the insert test).
        """
        db = setup_existing_db
        processor = db.base_station()

        # 1. Search with no filters.
        result_all = processor.search()
        print("Base Station (No filters):", result_all)
        assert isinstance(result_all,
                          pd.DataFrame), "Expected a DataFrame with no filters."
        assert not result_all.empty, "Expected non-empty DataFrame when no filters are applied."

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=3660619)
        print("Base Station (MMSI 3660619):", result_mmsi)
        assert isinstance(result_mmsi,
                          pd.DataFrame), "Expected a DataFrame for valid MMSI."
        assert not result_mmsi.empty, "Expected at least one row for MMSI 3669707."

        # 3. Search by invalid MMSI.
        result_invalid = processor.search(mmsi=9999999,
                                          start_date="2016-07-27",
                                          end_date="2016-07-29")
        print("Base Station (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid,
                          pd.DataFrame), "Expected a DataFrame for an invalid MMSI."
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(start_date="2016-07-27",
                                             end_date="2016-07-29")
        print("Base Station (Date Range):", result_date_range)
        assert isinstance(result_date_range,
                          pd.DataFrame), "Expected a DataFrame for the date range."
        assert not result_date_range.empty, "Expected data for the given date range."

# class TestSafetyAndAcknowledgementMessages:
#     def test_insert_safety_and_ack_messages(self, setup_new_db):
#         """
#         Test inserting message 7/13 (ack) and 12/14 (safety) into ais_msg_7_13 / ais_msg_12_14.
#         """
#         db = setup_new_db
#         processor = db.safety_and_ack()
#
#         # Sample ack (7)
#         sample_ack_7 = {
#             "id": 7,
#             "repeat_indicator": 0,
#             "mmsi": 777777777,
#             "ack_count": 2,
#             "ack_slot": 500,
#             "tagblock_group": {"sentence": 1, "id": 777},
#             "tagblock_line_count": 700,
#             "tagblock_station": "ACK-STATION",
#             "tagblock_timestamp": 1600030000
#         }
#
#         # Sample safety (14)
#         sample_safety_14 = {
#             "id": 14,
#             "repeat_indicator": 0,
#             "mmsi": 888888888,
#             "message_text": "SECURITY ALERT",
#             "tagblock_group": {"sentence": 1, "id": 888},
#             "tagblock_line_count": 800,
#             "tagblock_station": "SAFETY-STATION",
#             "tagblock_timestamp": 1600030050
#         }
#
#         processor._insert_message(sample_ack_7)
#         processor._insert_message(sample_safety_14)
#
#         # Check ais_msg_7_13
#         df_7_13 = db.connection().execute("SELECT * FROM ais_msg_7_13").fetchdf()
#         print("ais_msg_7_13:", df_7_13)
#         assert len(df_7_13) == 1, "Expected 1 row in ais_msg_7_13."
#         assert df_7_13.loc[0, "id"] == 7, "Expected id=7 in ais_msg_7_13."
#
#         # Check ais_msg_12_14
#         df_12_14 = db.connection().execute("SELECT * FROM ais_msg_12_14").fetchdf()
#         print("ais_msg_12_14:", df_12_14)
#         assert len(df_12_14) == 1, "Expected 1 row in ais_msg_12_14."
#         assert df_12_14.loc[0, "id"] == 14, "Expected id=14 in ais_msg_12_14."
#         assert df_12_14.loc[0, "message_text"] == "SECURITY ALERT"

class TestAcknowledgementMessages:
    def test_insert_message(self, setup_new_db):
        """
        Test inserting an Acknowledgement Message (Types 7 or 13) into ais_msg_7_13,
        then verify the row is present.
        """
        db = setup_new_db
        # Instantiate the new acknowledgement processor.
        processor = db.ack()

        sample_ack_7 = {
            "id": 7,
            "repeat_indicator": 0,
            "mmsi": 777777777,
            "ack_count": 2,
            "ack_slot": 500,
            "tagblock_group": {"sentence": 1, "id": 777},
            "tagblock_line_count": 700,
            "tagblock_station": "ACK-STATION",
            "tagblock_timestamp": 1600030000
        }
        processor._insert_message(sample_ack_7)

        df = db.connection().execute("SELECT * FROM ais_msg_7_13").fetchdf()
        print("Inserted Acknowledgement Message:", df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_7_13, got {len(df)}."
        assert df.loc[0, "id"] == 7, "Expected message ID 7 in ais_msg_7_13."

    # todo(thalia) the file does not have msg 7 and 13
    # def test_process_works(self, setup_new_db):
    #     """
    #     Test that processing a file inserts acknowledgement messages into ais_msg_7_13.
    #     """
    #     db = setup_new_db
    #     processor = db.ack()
    #     processor.process(AIS_FILE_PATH)
    #     conn = db.connection()
    #     count = conn.execute("SELECT COUNT(*) FROM ais_msg_7_13").fetchone()[0]
    #     print("Rows in ais_msg_7_13 after processing:", count)
    #     assert count > 0, "Expected ais_msg_7_13 table to have data after processing."
    #
    # def test_search_works(self, setup_existing_db):
    #     """
    #     Test search functionality for Acknowledgement messages.
    #     """
    #     db = setup_existing_db
    #     processor = db.ack()
    #
    #     # 1. Search with no filters.
    #     result_all = processor.search()
    #     print("Acknowledgement search (no filters):", result_all)
    #     assert isinstance(result_all,
    #                       pd.DataFrame), "Expected a DataFrame with no filters."
    #     # 2. Search by valid MMSI.
    #     result_mmsi = processor.search(mmsi=777777777)
    #     print("Acknowledgement search (MMSI 777777777):", result_mmsi)
    #     assert isinstance(result_mmsi,
    #                       pd.DataFrame), "Expected a DataFrame for valid MMSI."
    #     assert not result_mmsi.empty, "Expected non-empty result for MMSI 777777777."
    #     # 3. Search by an invalid MMSI.
    #     result_invalid = processor.search(mmsi=9999999,
    #                                       start_date="2016-07-27",
    #                                       end_date="2016-07-29")
    #     print("Acknowledgement search (invalid MMSI):", result_invalid)
    #     assert isinstance(result_invalid,
    #                       pd.DataFrame), "Expected a DataFrame for invalid MMSI."
    #     assert result_invalid.empty, "Expected empty result for invalid MMSI."


class TestSafetyMessages:
    def test_insert_message(self, setup_new_db):
        """
        Test inserting a Safety Message (Types 12 or 14) into ais_msg_12_14,
        then verify the row is present.
        """
        db = setup_new_db
        # Instantiate the new safety processor.
        processor = db.safety()

        sample_safety_14 = {
            "id": 14,
            "repeat_indicator": 0,
            "mmsi": 888888888,
            "message_text": "SECURITY ALERT",
            "tagblock_group": {"sentence": 1, "id": 888},
            "tagblock_line_count": 800,
            "tagblock_station": "SAFETY-STATION",
            "tagblock_timestamp": 1600030050
        }
        processor._insert_message(sample_safety_14)

        df = db.connection().execute("SELECT * FROM ais_msg_12_14").fetchdf()
        print("Inserted Safety Message:", df)
        assert len(df) == 1, f"Expected 1 row in ais_msg_12_14, got {len(df)}."
        assert df.loc[
                   0, "id"] == 14, "Expected message ID 14 in ais_msg_12_14."
        assert df.loc[
                   0, "message_text"] == "SECURITY ALERT", "Message text mismatch."

    # todo(thalia): file does not have msg 12 and 14
    # def test_process_works(self, setup_new_db):
    #     """
    #     Test that processing a file inserts safety messages into ais_msg_12_14.
    #     """
    #     db = setup_new_db
    #     processor = db.safety()
    #     processor.process(AIS_FILE_PATH)
    #     conn = db.connection()
    #     count = conn.execute("SELECT COUNT(*) FROM ais_msg_12_14").fetchone()[
    #         0]
    #     print("Rows in ais_msg_12_14 after processing:", count)
    #     assert count > 0, "Expected ais_msg_12_14 table to have data after processing."
    #
    # def test_search_works(self, setup_existing_db):
    #     """
    #     Test search functionality for Safety messages.
    #     """
    #     db = setup_existing_db
    #     processor = db.safety()
    #
    #     # 1. Search with no filters.
    #     result_all = processor.search()
    #     print("Safety search (no filters):", result_all)
    #     assert isinstance(result_all,
    #                       pd.DataFrame), "Expected a DataFrame with no filters."
    #     # 2. Search by valid MMSI.
    #     result_mmsi = processor.search(mmsi=888888888)
    #     print("Safety search (MMSI 888888888):", result_mmsi)
    #     assert isinstance(result_mmsi,
    #                       pd.DataFrame), "Expected a DataFrame for valid MMSI."
    #     assert not result_mmsi.empty, "Expected non-empty result for MMSI 888888888."
    #     # 3. Search by an invalid MMSI.
    #     result_invalid = processor.search(mmsi=9999999,
    #                                       start_date="2016-07-27",
    #                                       end_date="2016-07-29")
    #     print("Safety search (invalid MMSI):", result_invalid)
    #     assert isinstance(result_invalid,
    #                       pd.DataFrame), "Expected a DataFrame for invalid MMSI."
    #     assert result_invalid.empty, "Expected empty result for invalid MMSI."

class TestSarAircraftMessages:
    def test_insert_sar_aircraft_message(self, setup_new_db):
        """
        Test inserting Message 9 (SAR aircraft) into ais_msg_9,
        and verifying that it was written.
        """
        db = setup_new_db
        processor = db.sar_aircraft()

        sample_msg_9 = {
            "id": 9,
            "repeat_indicator": 0,
            "mmsi": 999999999,
            "altitude": 2500,
            "sog": 100.2,
            "position_accuracy": 1,
            "x": -100.0,
            "y": 32.0,
            "cog": 180,
            "timestamp": 10,
            "raim": False,
            "spare": 0,
            "application_data": {"emergency_code": "SAR"},
            "tagblock_group": {"sentence": 1, "id": 9},
            "tagblock_line_count": 100,
            "tagblock_station": "SAR-TST",
            "tagblock_timestamp": 1700000000
        }

        processor._insert_message(sample_msg_9)

        df = db.connection().execute("SELECT * FROM ais_msg_9").fetchdf()
        print(df)
        assert len(df) == 1, f"Expected 1 row, got {len(df)}."
        assert df.loc[0, "mmsi"] == 999999999
        assert df.loc[0, "altitude"] == 2500

    #todo(thalia) current file does not have msg 9
    # def test_process_works(self, setup_new_db):
    #     """
    #     If SarAircraftMessages has a file-based process() method to parse raw AIS data
    #     for message type 9, we can test it here. Otherwise, this is a placeholder.
    #     """
    #     db = setup_new_db
    #     processor = db.sar_aircraft()
    #
    #     processor.process(AIS_FILE_PATH)
    #     conn = db.connection()
    #     count = conn.execute("SELECT COUNT(*) FROM ais_msg_9").fetchone()[
    #         0]
    #     mmsi = conn.execute("SELECT * FROM ais_msg_9").fetchdf()["mmsi"]
    #     print(mmsi)
    #     print("Rows in ais_msg_9 after processing:", count)
    #     assert count > 0, "Expected ais_msg_9 table to have data after processing."
    #
    #
    # def test_search_works(self, setup_existing_db):
    #     """
    #     Test the search method of SarAircraftMessages. We assume .search() is implemented
    #     with optional filters like mmsi, start_date, end_date, polygon_bounds, etc.
    #     """
    #     db = setup_existing_db
    #     processor = db.sar_aircraft()
    #
    #     # 1. Test search with no filters (should return all message 9 data).
    #     result_all = processor.search()
    #     print("SAR Aircraft (No filters):", result_all)
    #     assert isinstance(result_all, pd.DataFrame), (
    #         "Expected a DataFrame when no filters are provided for SAR Aircraft."
    #     )
    #
    #     assert not result_all.empty, "Expected non-empty DataFrame with no filters for SAR."
    #
    #     # 2. Search by valid MMSI
    #     test_mmsi = 999999999
    #     result_mmsi = processor.search(mmsi=test_mmsi)
    #     print("SAR (MMSI filter):", result_mmsi)
    #     assert isinstance(result_mmsi, pd.DataFrame), "Expected a DataFrame for a valid MMSI."
    #     assert not result_mmsi.empty, f"Expected non-empty result for MMSI {test_mmsi}."
    #
    #     # 3. Search by invalid MMSI -> expect empty
    #     result_invalid_mmsi = processor.search(mmsi=123123123)
    #     print("SAR (Invalid MMSI):", result_invalid_mmsi)
    #     assert isinstance(result_invalid_mmsi, pd.DataFrame), "Expected a DataFrame even for invalid MMSI."
    #     assert result_invalid_mmsi.empty, "Expected empty DataFrame for an invalid MMSI."
    #
    #     #todo(thalia) cleaning: have a start date and end date constant for all test cases
    #     # 4. Search by date range (assuming you have data in that range)
    #     start_date = "2016-07-27"
    #     end_date="2016-07-29"
    #     result_date_range = processor.search(start_date=start_date, end_date=end_date)
    #     print("SAR (Date Range):", result_date_range)
    #     assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame for date range filters."
    #


class TestUtcDateMessages:
    def test_insert_utc_date_messages(self, setup_new_db):
        """
        Test inserting Messages 10 (UTC/Date inquiry) and 11 (UTC/Date response)
        into ais_msg_10_11, then verify they were written to the DB.
        """
        db = setup_new_db
        processor = db.utc_date()

        sample_msg_10 = {
            "id": 10,
            "repeat_indicator": 0,
            "mmsi": 538005182,
            "spare": 0,
            "dest_mmsi": 636013817,
            "spare2": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 9522},
            "tagblock_line_count": 10342,
            "tagblock_station": "D08MN-HG-CANBS1",
            "tagblock_timestamp": 1469666110
        }

        sample_msg_11 = {
            "id": 11,
            "repeat_indicator": 0,
            "mmsi": 477107200,
            "year": 2016,
            "month": 7,
            "day": 28,
            "hour": 0,
            "minute": 35,
            "second": 13,
            "position_accuracy": 0,
            "x": -120.51375,
            "y": 34.31921333333333,
            "fix_type": 1,
            "transmission_ctl": 0,
            "spare": 0,
            "raim": False,
            "sync_state": 0,
            "slot_timeout": 0,
            "slot_offset": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 2196},
            "tagblock_line_count": 9770,
            "tagblock_station": "D11MN-LA-LAGBS1",
            "tagblock_timestamp": 1469666112
        }

        # Insert both messages
        processor._insert_message(sample_msg_10)
        processor._insert_message(sample_msg_11)

        # Verify we have two rows in ais_msg_10_11
        df = db.connection().execute("SELECT * FROM ais_msg_10_11").fetchdf()
        print(df)  # Debugging

        assert len(df) == 2, f"Expected 2 rows, got {len(df)}."

        ids = set(df["id"])
        assert ids == {10, 11}, f"Expected message IDs 10 and 11, got {ids}"

    # todo(thalia) current file does not have utc msgs
    # def test_process_works(self, setup_new_db):
    #     """
    #     If UtcDateMessages has a file-based process(file_path) method,
    #     we can test it here. Otherwise, it's just a placeholder.
    #     """
    #     db = setup_new_db
    #     conn = db.connection()
    #     processor = db.utc_date()
    #     processor.process(AIS_FILE_PATH)
    #
    #     count = conn.execute("SELECT COUNT(*) FROM ais_msg_9").fetchone()[0]
    #     print("Rows in ais_msg_10_11 after process:",count)
    #     assert  count > 0, "Expected ais_msg_10_11 to have data after processing."
    #
    #
    # def test_search_works(self, setup_existing_db):
    #     """
    #     Test the search method of UtcDateMessages. We assume .search(...) is
    #     implemented with optional parameters like msg_id, mmsi, dest_mmsi, etc.
    #     """
    #     db = setup_existing_db
    #     processor = db.utc_date()
    #
    #     # 1. Search with no filters - expect all message 10 & 11 records
    #     all_df = processor.search()
    #     print("UTC/Date (No filters):", all_df)
    #     assert isinstance(all_df, pd.DataFrame), "Expected a DataFrame when no filters are given."
    #     assert not all_df.empty, "Expected non-empty DataFrame with no filters (assuming fixture has data)."
    #
    #     # 2. Search specifically for message 10
    #     msg10_df = processor.search(msg_id=10)
    #     print("UTC/Date (Msg 10):", msg10_df)
    #     assert isinstance(msg10_df, pd.DataFrame), "Expected a DataFrame for msg_id=10."
    #     # If your fixture includes a message 10, you can expect not empty:
    #     # assert not msg10_df.empty, "Expected non-empty result for message 10."
    #
    #     # 3. Search specifically for message 11
    #     msg11_df = processor.search(msg_id=11)
    #     print("UTC/Date (Msg 11):", msg11_df)
    #     assert isinstance(msg11_df, pd.DataFrame), "Expected a DataFrame for msg_id=11."
    #
    #     # 4. Search by invalid dest_mmsi to ensure we get no results
    #     invalid_dest_df = processor.search(dest_mmsi=999999999)
    #     print("UTC/Date (Invalid dest_mmsi):", invalid_dest_df)
    #     assert isinstance(invalid_dest_df, pd.DataFrame), "Expected a DataFrame for invalid dest_mmsi."
    #     assert invalid_dest_df.empty, "Expected empty result for invalid dest_mmsi."


class TestSystemManagementMessages:
    def test_insert_system_management_messages(self, setup_new_db):
        """
        Test inserting Messages 15, 16, 17, 20, 22, and 23
        into ais_msg_15_16_17_20_22_23, then verify the data.
        """
        db = setup_new_db
        processor = db.system_management()

        sample_msg_15 = {
            "id": 15,
            "repeat_indicator": 0,
            "mmsi": 3669706,
            "mmsi_1": 338576000,
            "msg_1_1": 5,
            "slot_offset_1_1": 0,
            "dest_msg_1_2": 0,
            "slot_offset_1_2": 0,
            "mmsi_2": 0,
            "msg_2": 0,
            "slot_offset_2": 0,
            "spare": 0,
            "spare2": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 222},
            "tagblock_line_count": 7230,
            "tagblock_station": "D08MN-HG-BAYBS1",
            "tagblock_timestamp": 1469664359
        }

        sample_msg_16 = {
            "id": 16,
            "repeat_indicator": 0,
            "mmsi": 3669977,
            "dest_mmsi_a": 636013630,
            "offset_a": 60,
            "inc_a": 0,
            "dest_mmsi_b": 0,
            "offset_b": 0,
            "inc_b": 0,
            "spare": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 3560},
            "tagblock_line_count": 2443,
            "tagblock_station": "D08MN-HG-HISBS1",
            "tagblock_timestamp": 1469664849
        }

        sample_msg_17 = {
            "id": 17,
            "repeat_indicator": 0,
            "mmsi": 367449650,
            "x": 0.555,
            "y": -94.75833333333334,
            "spare": 0,
            "spare2": 0,
            "tagblock_line_count": 547314,
            "tagblock_station": "b003665002",
            "tagblock_timestamp": 1469664700
        }

        sample_msg_20 = {
            "id": 20,
            "repeat_indicator": 0,
            "mmsi": 3669955,
            "reservations": [
                {"offset": 215, "num_slots": 5, "timeout": 7, "incr": 225},
                {"offset": 11, "num_slots": 2, "timeout": 7, "incr": 375}
            ],
            "spare": 0,
            "spare2": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 1141},
            "tagblock_line_count": 12654,
            "tagblock_station": "D08MN-NO-REGBS1",
            "tagblock_timestamp": 1469664480
        }

        sample_msg_22 = {
            "id": 22,
            "repeat_indicator": 0,
            "mmsi": 3160067,
            "chan_a": 2087,
            "chan_b": 2088,
            "txrx_mode": 0,
            "power_low": False,
            "x1": -79.83333333333333,
            "y1": 46.333333333333336,
            "x2": -84.33333333333333,
            "y2": 43.5,
            "chan_a_bandwidth": 0,
            "chan_b_bandwidth": 0,
            "zone_size": 2,
            "spare2": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 9827},
            "tagblock_line_count": 16094,
            "tagblock_station": "D09MN-DE-PASBS1",
            "tagblock_timestamp": 1469665318
        }

        sample_msg_23 = {
            "id": 23,
            "repeat_indicator": 3,
            "mmsi": 3160048,
            "spare": 0,
            "x1_23": -79.17333333333333,
            "y1_23": 43.275,
            "x2_23": -79.225,
            "y2_23": 43.111666666666665,
            "station_type": 0,
            "type_and_cargo": 0,
            "interval_raw": 11,
            "quiet": 0,
            "spare2": 0,
            "spare3": 0,
            "tagblock_group": {"sentence": 1, "groupsize": 2, "id": 249},
            "tagblock_line_count": 13592,
            "tagblock_station": "D09MN-BU-EDEBS1",
            "tagblock_timestamp": 1469665360
        }

        # Insert them all
        for msg in [sample_msg_15, sample_msg_16, sample_msg_17, sample_msg_20, sample_msg_22, sample_msg_23]:
            processor._insert_message(msg)

        # Now verify we have 6 rows in ais_msg_15_16_17_20_22_23
        df = db.connection().execute("SELECT * FROM ais_msg_15_16_17_20_22_23").fetchdf()
        print(df)  # For debugging

        assert len(df) == 6, f"Expected 6 rows, got {len(df)}."

        # Optionally check that we have each message type present.
        ids = sorted(df["id"].unique().tolist())
        assert ids == [15, 16, 17, 20, 22, 23], f"Expected IDs [15,16,17,20,22,23], got {ids}"


# Some messages for type B
"""
{'id': 24, 'repeat_indicator': 3, 'mmsi': 366970430, 'part_num': 1, 'type_and_cargo': 60, 'vendor_id': 'COMNAV@', 'callsign': 'WY6769@', 'dim_a': 3, 'dim_b': 16, 'dim_c': 3, 'dim_d': 3, 'spare': 0, 'tagblock_group': {'sentence': 1, 'groupsize': 2, 'id': 6539}, 'tagblock_line_count': 7284, 'tagblock_station': 'D13MN-CR-KELBS1', 'tagblock_timestamp': 1469663999}
{'id': 18, 'repeat_indicator': 0, 'mmsi': 338097623, 'spare': 0, 'sog': 0.10000000149011612, 'position_accuracy': 1, 'x': -70.19623333333334, 'y': 43.726173333333335, 'cog': 237.3000030517578, 'true_heading': 511, 'timestamp': 59, 'spare2': 0, 'unit_flag': 1, 'display_flag': 0, 'dsc_flag': 1, 'band_flag': 0, 'm22_flag': 1, 'mode_flag': 0, 'raim': False, 'commstate_flag': 1, 'commstate_cs_fill': 393222, 'tagblock_group': {'sentence': 1, 'groupsize': 2, 'id': 1700}, 'tagblock_line_count': 2431, 'tagblock_station': 'D01MN-NE-BRIBS1', 'tagblock_timestamp': 1469664000}
"""
# class TestApplicationSpecificMessages:
#     def test_insert_application_specific_messages(self, setup_new_db):
#         """
#         Test inserting messages 6, 8, 25, 26 into ais_msg_6_8 / ais_msg_25_26.
#         """
#         db = setup_new_db
#         processor = db.asm()  # application-specific messages
#
#         sample_msg_6 = {
#             "id": 6,
#             "repeat_indicator": 0,
#             "mmsi": 111111111,
#             "spare": 0,
#             "spare2": 0,
#             "dac": 1,
#             "fi": 27,
#             "x": -60.5,
#             "y": 25.25,
#             "some_extra_field": "HelloWorld",  # leftover data
#             "tagblock_group": {"sentence": 1, "groupsize": 2},
#             "tagblock_line_count": 300,
#             "tagblock_station": "SAT-TEST",
#             "tagblock_timestamp": 1600000000
#         }
#         sample_msg_25 = {
#             "id": 25,
#             "repeat_indicator": 0,
#             "mmsi": 222222222,
#             "dest_mmsi": 333333333,
#             "sync_state": 2,
#             "x": 10.123,
#             "y": 45.987,
#             "extra_key": "ABC-XYZ",
#             "tagblock_group": {"sentence": 2, "groupsize": 2},
#             "tagblock_line_count": 301,
#             "tagblock_station": "COAST-SB",
#             "tagblock_timestamp": 1600000050
#         }
#
#         # Insert each
#         processor._insert_message(sample_msg_6)
#         processor._insert_message(sample_msg_25)
#
#         # Query ais_msg_6_8
#         df_6_8 = db.connection().execute("SELECT * FROM ais_msg_6_8").fetchdf()
#         print("ais_msg_6_8:", df_6_8)
#         assert len(df_6_8) == 1, "Expected 1 row for message 6/8 table."
#         assert df_6_8.loc[0, "id"] == 6, "Expected ID=6 in ais_msg_6_8."
#
#         # Query ais_msg_25_26
#         df_25_26 = db.connection().execute("SELECT * FROM ais_msg_25_26").fetchdf()
#         print("ais_msg_25_26:", df_25_26)
#         assert len(df_25_26) == 1, "Expected 1 row for message 25/26 table."
#         assert df_25_26.loc[0, "id"] == 25, "Expected ID=25 in ais_msg_25_26."
