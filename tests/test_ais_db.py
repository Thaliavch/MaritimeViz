import os
import re
import shutil

import geopandas as gpd
import pandas as pd
import pytest

from src.maritimeviz.ais_db import AISDatabase

# Database and AIS files for testing
TEST_DB_PATH = os.path.join(os.path.dirname(__file__), "ais_data .duckdb")
# Use os.path.join and os.path.dirname(__file__) to get the path relative to the current file
AIS_FILE_PATH = os.path.join(os.path.dirname(__file__), "ais_2016_07_28_aa")


@pytest.fixture(scope="function")
def setup_existing_db():
    """
    Fixture to create and clean up a test AISDatabase instance
    with an existing db.
    """
    # Initialize the database (this will create tables and views)
    db = AISDatabase(TEST_DB_PATH)  # existing db
    yield db
    # Clear cache if needed and close connection
    db.clear_cache()
    db.close()

    # Remove any exported files
    for fname in [
        "test_data.geojson",
        "test_data.csv",
        "test_data.parquet",
        "test_data.json",
        "ais_shapefile",
        "test_data.kml",
        "test_data.xlsx",
    ]:
        if os.path.exists(fname):
            if os.path.isdir(fname):
                shutil.rmtree(fname)
            else:
                os.remove(fname)


@pytest.fixture(scope="function")
def setup_new_db(request, tmp_path):
    """
    Fixture to create and clean up a test AISDatabase instance instantiated
    with default db path.
    """
    # Initialize empty database
    if hasattr(request, "param") and request.param:
        db_file = tmp_path / request.param
        db = AISDatabase(str(db_file))
    else:
        # ask AISDatabase for its default name, but put it under tmp_path
        default_name = AISDatabase._get_default_db_path()
        db_file = tmp_path / default_name
        db = AISDatabase(str(db_file))

    yield db
    # Clear cache if needed and close connection
    db.clear_cache()
    db.close()
    # Remove the test database files after tests run
    for fname in [
        "ais_data_1.duckdb",
        "ais_class_A_only.duckdb",
        "ais_class_B_only.duckdb",
    ]:
        if os.path.exists(fname):
            os.remove(fname)
    # Remove any exported files
    for fname in [
        "test_data.csv",
        "test_data.parquet",
        "test_data.json",
        "ais_shapefile",
        "test_data.kml",
        "test_data.xlsx",
    ]:
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
    #     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';")\
    #     .fetchall()
    # print(tables)
    result = conn.execute("SELECT * FROM ais_msg_123 LIMIT 10").fetchdf()
    # print(result)

    assert conn is not None
    assert len(result) > 0


def test_db_default_name():
    # Reset the class-level counter
    AISDatabase._default_db_counter = 0

    name1 = AISDatabase._get_default_db_path()
    # print(name1)
    name2 = AISDatabase._get_default_db_path()
    # print(name2)

    # Check that the names follow the expected pattern.
    pattern = r"^ais_data_\d+\.duckdb$"
    assert re.match(pattern, name1), (
        f"Default db name {name1} does not match the pattern."
    )
    assert re.match(pattern, name2), (
        f"Default db name {name2} does not match the pattern."
    )

    # Check that the two names are different.
    assert name1 != name2, "Consecutive default db names should be different."

    # Check that the counter has increased by 2.
    assert AISDatabase._default_db_counter == 2, (
        "Global counter should be incremented by 2 after two calls."
    )


def test_global_views_exist(setup_new_db):
    """Test that the global views are created and return a DataFrame (even if empty)."""
    db = setup_new_db
    conn = db.connection()
    # Check for one of the views; adjust table/view names if needed.
    try:
        df_dynamic = conn.execute("SELECT * FROM global_ais_dynamic LIMIT 1").fetchdf()
        df_static = conn.execute("SELECT * FROM global_ais_static LIMIT 1").fetchdf()
        df_all = conn.execute("SELECT * FROM global_ais_data LIMIT 1").fetchdf()
    except Exception as e:
        pytest.fail(f"Global view query failed: {e}")

    # They might be empty if no data is inserted, but the queries should succeed.
    assert isinstance(df_dynamic, pd.DataFrame)
    assert isinstance(df_static, pd.DataFrame)
    assert isinstance(df_all, pd.DataFrame)


class TestGlobalFunctionality:
    """
    Functional tests for the global process, global search, and filter behavior
    of AISDatabase.
    """

    @pytest.mark.parametrize("setup_new_db", [None], indirect=True)
    def test_global_process_populates_views(self, setup_new_db):
        db: AISDatabase = setup_new_db
        db.process(AIS_FILE_PATH)

        # After processing, all three global views should return GeoDataFrames
        df_dynamic = db.search(report_type="position")
        df_static = db.search(report_type="static")

        assert isinstance(df_dynamic, gpd.GeoDataFrame), (
            "Expected GeoDataFrame for 'position report'"
        )
        assert isinstance(df_static, gpd.GeoDataFrame), (
            "Expected GeoDataFrame for 'static'"
        )

        # At least one of them should be non‑empty if the test file has data
        assert not (df_dynamic.empty and df_static.empty), (
            "All global views are empty after processing – expected at least one to contain data"
        )

    def test_global_search_without_and_with_filter(self, setup_existing_db):
        mmsi = 9111254
        db: AISDatabase = setup_existing_db

        # Get the full unfiltered 'all' dataset
        all_df = db.search(report_type="position")
        assert isinstance(all_df, gpd.GeoDataFrame)

        # Apply filter object and call search without explicit args
        db.set_filter({"mmsi": mmsi})
        filtered_df = db.search(report_type="position")
        # Call search directly with the same MMSI
        direct_df = db.search(report_type="position", mmsi=mmsi)

        # Both results should match in length and content
        assert isinstance(filtered_df, gpd.GeoDataFrame)
        assert isinstance(direct_df, gpd.GeoDataFrame)
        assert len(filtered_df) == len(direct_df), (
            "Filter object did not produce same result as passing mmsi directly"
        )
        # All rows in the filtered GeoDataFrame should have the expected MMSI
        assert all(filtered_df["mmsi"] == mmsi)

        # Clearing the filter should restore the full result
        db.clear_filter()
        cleared_df = db.search(report_type="position")
        assert len(cleared_df) == len(all_df), (
            "Clearing filter did not restore full dataset"
        )

    def test_filter_object_type_validation(self, setup_existing_db):
        db: AISDatabase = setup_existing_db

        # Passing a non-dict to set_filter should raise TypeError
        with pytest.raises(TypeError):
            db.set_filter(["not", "a", "dict"])

        # Passing invalid filter keys should raise TypeError
        with pytest.raises(TypeError):
            db.set_filter({"invalid_key": 123})

        # A valid empty filter is allowed
        db.set_filter({})
        assert db._filter == {}

        # Clean up
        db.clear_filter()
        assert db._filter is None


class TestGlobalExports:
    """
    Testing global export methods in AISDatabase
    """

    def test_get_global_geojson(self, setup_existing_db):
        db = setup_existing_db
        result = db.get_geojson(report_type="position")
        # Check that result is a dictionary
        assert isinstance(result, dict)
        # Verify it has a FeatureCollection structure
        assert result.get("type") == "FeatureCollection"
        assert isinstance(result.get("features"), list)

    def test_get_global_csv(self, setup_existing_db):
        db = setup_existing_db
        file_path = "test_data.csv"
        result = db.get_csv(file_path=file_path, report_type="position")
        # If no data exists, function should return a string indicating no data.
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping CSV file test.")
        # Otherwise, check the file exists.
        assert os.path.exists(file_path)
        # Load CSV and check type.
        df = pd.read_csv(file_path)
        assert isinstance(df, pd.DataFrame)

    def test_get_global_shapefile(self, setup_existing_db):
        db = setup_existing_db
        folder_path = "ais_shapefile"
        result = db.get_shapefile(file_path=folder_path, report_type="position")
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
        result = db.get_kml(file_path=file_path, report_type="position")
        if result.startswith("No data"):
            pytest.skip("No data available to export; skipping KML test.")
        assert os.path.exists(file_path)
        gdf = gpd.read_file(file_path)
        assert isinstance(gdf, gpd.GeoDataFrame)

    def test_get_global_wkt(self, setup_existing_db):
        db = setup_existing_db
        result = db.get_wkt(report_type="position")
        if isinstance(result, str) and result.startswith("No data"):
            pytest.skip("No data available to export; skipping WKT test.")
        assert isinstance(result, list)
        # Optionally, check that at least one WKT string contains "POINT"
        assert any("POINT" in wkt for wkt in result)


def test_dynamic_table_has_data(setup_existing_db):
    db = setup_existing_db
    conn = db.connection()
    df = conn.execute("SELECT mmsi, id FROM ais_msg_123 LIMIT 3").fetchdf()
    # print("ais_msg_123 sample:", df)
    assert not df.empty, "Expected ais_msg_123 to have data."


class TestClassAMessages:
    def test_search_works(self, setup_existing_db):
        db = setup_existing_db
        db.clear_cache()
        processor = db.class_a()

        # 1. Test search with no filters.
        result_all = processor.search()
        # print("Type A (No filters):", result_all)
        assert isinstance(result_all, gpd.GeoDataFrame), "Expected a GeoDataFrame ."
        assert not result_all.empty, "Expected non-empty GeoDataFrame."

        # 2. Search by a valid MMSI (e.g., 9111254).
        result_mmsi = processor.search(mmsi=9111254)
        # print("Type A (MMSI 9111254):", result_mmsi)
        assert isinstance(result_mmsi, gpd.GeoDataFrame), (
            "Expected a GeoDataFrame for a valid MMSI search."
        )
        assert not result_mmsi.empty, "Expected non-empty result for MMSI 9111254."
        # Adjust expected row count as appropriate (example: expecting 24 rows)
        assert len(result_mmsi) == 6, (
            f"Expected 6 rows for MMSI 9111254, got {len(result_mmsi)}."
        )

        # 3. Search by non-existing MMSI should return an empty GeoDataFrame.
        result_invalid_mmsi = processor.search(mmsi=9999999)
        # print("Type A (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, gpd.GeoDataFrame), (
            "Expected a GeoDataFrame even for an invalid MMSI."
        )
        assert result_invalid_mmsi.empty, (
            "Expected an empty GeoDataFrame for an invalid MMSI."
        )

        # 4. Search by date range (should return at least one row).
        result_date_range = processor.search(
            start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Type A (Date Range):", result_date_range)
        assert isinstance(result_date_range, gpd.GeoDataFrame), (
            "Expected a GeoDataFrame for a date range search."
        )
        assert not result_date_range.empty, (
            "Expected non-empty GeoDataFrame for the given date range."
        )
        assert len(result_date_range) >= 1, (
            "Expected at least one row for the given date range."
        )

        # Search by polygon bounds.
        # polygon_bounds = "POLYGON((-93 29, -93 33, -89 33, -89 29, -93 29))"
        # result_polygon = processor.search(polygon_bounds=polygon_bounds)
        # print("Type A (Polygon Bounds):", result_polygon)
        # assert isinstance(result_polygon, gpd.GeoDataFrame), "Expected a GeoDataFrame
        # for polygon bounds search."
        # assert not result_polygon.empty, "Expected non-empty result for the given polygon bounds."
        # Example check: verify a known point is within at least one feature (adjust as needed)
        # known_point = Point(-90.0, 30.0)
        # assert any(result_polygon.geometry.apply(lambda geom: geom.within(known_point))),
        #        "Expected at least one geometry to contain the known point."

    # TODO(Thalia) in the class ensure user enters duckdb file extension otherwise add the extension
    @pytest.mark.parametrize("setup_new_db", ["ais_class_A_only.duckdb"], indirect=True)
    def test_process_classA(self, setup_new_db):
        db = setup_new_db
        processorA = db.class_a()
        processorA.process(AIS_FILE_PATH)

        conn = db.connection()
        count_123 = conn.execute("SELECT COUNT(*) FROM ais_msg_123").fetchone()[0]
        # Static table for Class A (ais_msg_5) has no data for the current testing file
        count_5 = conn.execute("SELECT COUNT(*) FROM ais_msg_5").fetchone()[0]

        # print("Rows in ais_msg_123:", count_123)
        # print("Rows in ais_msg_5:", count_5)

        assert count_123 > 0, (
            "Expected ais_msg_123 to have data after processing Class A messages."
        )
        assert count_5 == 0, (
            "Expected ais_msg_5 to have no data after processing Class A messages."
        )


class TestClassBMessages:
    def test_search_works(self, setup_existing_db):
        db = setup_existing_db
        processor = db.class_b()

        # 1. Test search with no filters.
        result_all = processor.search()
        # print("Type B (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame when no filters are provided for Type B."
        )
        assert not result_all.empty, (
            "Expected non-empty DataFrame when no filters are applied for Type B."
        )

        # 2. Search by valid MMSI
        result_mmsi = processor.search(mmsi=338097623)
        # print("Type B (MMSI 338097623, Date Range):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for a valid MMSI search in Type B."
        )
        assert not result_mmsi.empty, (
            "Expected non-empty result for MMSI 338097623 in Type B."
        )

        # 3. Search by non-existing MMSI should return an empty DataFrame.
        result_invalid_mmsi = processor.search(
            mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Type B (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, pd.DataFrame), (
            "Expected a DataFrame even for an invalid MMSI in Type B."
        )
        assert result_invalid_mmsi.empty, (
            "Expected an empty DataFrame for an invalid MMSI in Type B."
        )

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-26", end_date="2016-07-30"
        )
        # print("Type B (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for a date range search in Type B."
        )
        assert not result_date_range.empty, (
            "Expected non-empty DataFrame for the given date range in Type B."
        )

    @pytest.mark.parametrize("setup_new_db", ["ais_class_B_only.duckdb"], indirect=True)
    def test_process_classB(self, setup_new_db):
        db = setup_new_db
        db.clear_cache()
        processorB = db.class_b()
        # Process the sample file with the Class B processor.
        processorB.process(AIS_FILE_PATH)

        conn = db.connection()
        # Check that the dynamic table for Class B (ais_msg_18_19) has data.
        count_18_19 = conn.execute("SELECT COUNT(*) FROM ais_msg_18_19").fetchone()[0]
        # Check that the static table for Class B (ais_msg_24) has data.
        count_24 = conn.execute("SELECT COUNT(*) FROM ais_msg_24").fetchone()[0]

        # print("Rows in ais_msg_18_19:", count_18_19)
        # print("Rows in ais_msg_24:", count_24)

        assert count_18_19 > 0, (
            "Expected ais_msg_18_19 to have data after processing Class B messages."
        )
        assert count_24 > 0, (
            "Expected ais_msg_24 to have data after processing Class B messages."
        )


class TestLongRangeMessages:
    def test_process_works(self, setup_new_db):
        db = setup_new_db
        processor = db.long_range()

        processor.process(AIS_FILE_PATH)

        conn = db.connection()

        count_27 = conn.execute("SELECT COUNT(*) FROM ais_msg_27").fetchone()[0]

        # print("Rows in ais_msg_27:", count_27)

        assert count_27 > 0, (
            "Expected ais_msg27 to have data after processing long range messages."
        )

    def test_search_works(self, setup_existing_db):
        # mmsi 577305000
        db = setup_existing_db
        processor = db.long_range()

        # 1. Test search with no filters.
        result_all = processor.search()
        # print("Long Range (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame when no filters are provided for Long Range."
        )
        assert not result_all.empty, (
            "Expected non-empty DataFrame when no filters are applied for Long Range."
        )

        # 2. Search by valid MMSI
        result_mmsi = processor.search(mmsi=577305000)
        # print("Long Range (MMSI 577305000, Date Range):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for a valid MMSI."
        )
        assert not result_mmsi.empty, "Expected non-empty result."

        # 3. Search by non-existing MMSI should return an empty DataFrame.
        result_invalid_mmsi = processor.search(
            mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Type B (Invalid MMSI):", result_invalid_mmsi)
        assert isinstance(result_invalid_mmsi, pd.DataFrame), (
            "Expected a DataFrame even for an invalid MMSI ."
        )
        assert result_invalid_mmsi.empty, (
            "Expected an empty DataFrame for an invalid MMSI."
        )

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-26", end_date="2016-07-30"
        )
        # print("Type B (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for a date range."
        )
        assert not result_date_range.empty, (
            "Expected non-empty DataFrame for the given date range."
        )


# Test for Addressed Binary Messages (Type 6)
class TestAddressedBinaryHandler:
    def test_insert_msg(self, setup_new_db):
        pass

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
    #     result_invalid = processor.search(mmsi=9999999, start_date="2016-07-27",
    #     end_date="2016-07-29")
    #     print("Addressed Binary (Invalid MMSI):", result_invalid)
    #     assert isinstance(result_invalid, pd.DataFrame), "Expected a DataFrame
    #     for an invalid MMSI."
    #     assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."
    #
    #     # 4. Search by date range.
    #     result_date_range = processor.search(start_date="2023-10-01", end_date="2023-12-31")
    #     print("Addressed Binary (Date Range):", result_date_range)
    #     assert isinstance(result_date_range, pd.DataFrame), "Expected a DataFrame
    #     for the date range."


# Test for Broadcast Text Messages (Type 8)
class TestBroadcastTextHandler:
    def test_insert_msg(self, setup_new_db):
        pass

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_8.
        """
        db = setup_new_db
        processor = db.broadcast_text()

        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_8").fetchone()[0]
        # print("Rows in ais_msg_8:", count)
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
        # print("Broadcast Text (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame with no filters."
        )
        assert not result_all.empty, "Expected non-empty result."

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=366853070)
        # print("Broadcast Text (MMSI 366853070):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for valid MMSI."
        )
        assert not result_mmsi.empty, "Expecting messages with mmsi: 366853070"

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(
            mmsi=111111111, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Broadcast Text (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), (
            "Expected a DataFrame for an invalid MMSI."
        )
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Broadcast Text (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for the date range."
        )
        assert not result_date_range.empty, "Expecting data for given date ..."


# Test for Short Binary Messages (Types 25/26)
class TestShortBinaryHandler:
    def test_insert_msg(self, setup_new_db):
        pass

    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_25_26.
        """
        db = setup_new_db
        processor = db.short_binary()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_25_26").fetchone()[0]
        # print("Rows in ais_msg_25_26:", count)
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
        # print("Short Binary (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame with no filters."
        )
        assert not result_all.empty, "Expecting non empty dataframe"

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=367080550)
        # print("Short Binary (MMSI 367080550):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for valid MMSI."
        )
        assert not result_mmsi.empty, "Expected one row for mmsi 367080550"

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(
            mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Short Binary (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), (
            "Expected a DataFrame for an invalid MMSI."
        )
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Short Binary (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for the date range."
        )
        assert not result_date_range.empty, "Expected data for given dates"


class TestAidToNavigationMessages:
    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_21.
        """
        db = setup_new_db
        processor = db.aton()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT count(*) FROM ais_msg_21").fetchone()[0]
        # print("Rows in ais_msg_21:", count)
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
        # print("AtoN (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame with no filters."
        )
        assert not result_all.empty, (
            "Expected non-empty DataFrame when no filters are applied."
        )

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=993672272)
        # print("AtoN (MMSI 993672272):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for valid MMSI."
        )
        assert not result_mmsi.empty, "Expected at least one row for mmsi 993123456."

        # 3. Search by non-existent MMSI should return an empty DataFrame.
        result_invalid = processor.search(
            mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("AtoN (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), (
            "Expected a DataFrame for an invalid MMSI."
        )
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("AtoN (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for the date range."
        )
        assert not result_date_range.empty, "Expected data for the given dates."


class TestBaseStationMessages:
    def test_process_works(self, setup_new_db):
        """
        Test that processing a file inserts messages into ais_msg_4.
        """
        db = setup_new_db
        processor = db.base_station()
        processor.process(AIS_FILE_PATH)
        conn = db.connection()

        count = conn.execute("SELECT COUNT(*) FROM ais_msg_4").fetchone()[0]
        # print("Rows in ais_msg_4:", count)
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
        # print("Base Station (No filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected a DataFrame with no filters."
        )
        assert not result_all.empty, (
            "Expected non-empty DataFrame when no filters are applied."
        )

        # 2. Search by valid MMSI.
        result_mmsi = processor.search(mmsi=3660619)
        # print("Base Station (MMSI 3660619):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected a DataFrame for valid MMSI."
        )
        assert not result_mmsi.empty, "Expected at least one row for MMSI 3669707."

        # 3. Search by invalid MMSI.
        result_invalid = processor.search(
            mmsi=9999999, start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Base Station (Invalid MMSI):", result_invalid)
        assert isinstance(result_invalid, pd.DataFrame), (
            "Expected a DataFrame for an invalid MMSI."
        )
        assert result_invalid.empty, "Expected an empty DataFrame for an invalid MMSI."

        # 4. Search by date range.
        result_date_range = processor.search(
            start_date="2016-07-27", end_date="2016-07-29"
        )
        # print("Base Station (Date Range):", result_date_range)
        assert isinstance(result_date_range, pd.DataFrame), (
            "Expected a DataFrame for the date range."
        )
        assert not result_date_range.empty, "Expected data for the given date range."


class TestAcknowledgementMessages:
    def test_insert_message(self, setup_new_db):
        pass

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
        pass

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
        pass

    # todo(thalia) current file does not have msg 9
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
    #     assert isinstance(result_invalid_mmsi, pd.DataFrame), "Expected a DataFrame \
    #     even for invalid MMSI."
    #     assert result_invalid_mmsi.empty, "Expected empty DataFrame for an invalid MMSI."
    #
    #     #todo(thalia) cleaning: have a start date and end date constant for all test cases
    #     # 4. Search by date range (assuming you have data in that range)
    #     start_date = "2016-07-27"
    #     end_date="2016-07-29"
    #     result_date_range = processor.search(start_date=start_date, end_date=end_date)
    #     print("SAR (Date Range):", result_date_range)
    #     assert isinstance(result_date_range, pd.DataFrame), "Expected a \
    #     DataFrame for date range filters."
    #


class TestUtcDateMessages:
    def test_insert_utc_date_messages(self, setup_new_db):
        pass

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
    #     assert isinstance(all_df, pd.DataFrame), "Expected a DataFrame when
    #     no filters are given."
    #     assert not all_df.empty, "Expected non-empty DataFrame with no filters
    #     (assuming fixture has data)."
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
    #     assert isinstance(invalid_dest_df, pd.DataFrame), "Expected a DataFrame \
    #     for invalid dest_mmsi."
    #     assert invalid_dest_df.empty, "Expected empty result for invalid dest_mmsi."


class TestSystemManagementMessages:
    def test_process_works(self, setup_new_db):
        """
        If SystemManagementMessages has a file-based process() method to parse
        raw AIS data for messages {15,16,17,20,22,23}, test it here.
        Otherwise, this is just a placeholder.
        """
        db = setup_new_db
        processor = db.system_management()
        processor.process(AIS_FILE_PATH)

        count = (
            db.connection()
            .execute("SELECT COUNT(*) FROM ais_msg_15_16_17_20_22_23")
            .fetchone()[0]
        )
        # print("Rows in ais_msg_15_16_17_20_22_23 after process:", count)
        assert count > 0, "Expected some data after processing system mgmt file."

    def test_search_works(self, setup_existing_db):
        """
        Test the search method in SystemManagementMessages with basic filters:
        - msg_id
        - mmsi: 3669980
        - start/end date
        """
        db = setup_existing_db
        processor = db.system_management()

        # 1. No filters => expect all rows in that table
        result_all = processor.search()
        # print("System mgmt (no filters):", result_all)
        assert isinstance(result_all, pd.DataFrame), (
            "Expected DataFrame from search() with no filters"
        )
        assert not result_all.empty, (
            "Expected at least one row for system mgmt messages."
        )

        # 2. Filter by a single message type (e.g. 20)
        result_20 = processor.search(msg_id=20)
        # print("System mgmt (message 20):", result_20)
        assert isinstance(result_20, pd.DataFrame), (
            "Expected DataFrame for message ID=17"
        )
        assert not result_20.empty, "Expected to find at least one message 17 row."

        # 3. Filter by MMSI
        test_mmsi = 3669980
        result_mmsi = processor.search(mmsi=test_mmsi)
        # print(f"System mgmt (MMSI={test_mmsi}):", result_mmsi)
        assert isinstance(result_mmsi, pd.DataFrame), (
            "Expected DataFrame for a valid MMSI"
        )

        # 4. Date range filter
        start_date = "2016-07-26"
        end_date = "2016-07-28"
        result_date = processor.search(start_date=start_date, end_date=end_date)
        # print(f"System mgmt (Date range {start_date} to {end_date}):", result_date)
        assert isinstance(result_date, pd.DataFrame), (
            "Expected DataFrame for date range filter"
        )
        assert not result_date.empty, "Expecting data for given dates."
