import pytest
#from src/maritimeviz/ import AISDatabase
import pandas as pd
import geopandas as gpd

from src.maritimeviz.ais_db import AISDatabase
from . import logger

file_path = "tests/ais_2016_07_28_aa"
db_path ="test_db.duckdb"
existing_db_path = "ais_data .duckdb"

def test_initialize_database_works():
    db = AISDatabase(db_path)
    assert db.connection is not None
    db.close()

# def test_initialize_existing_database_works():
#    db = AISDatabase(existing_db_path)
#    result = db.search(9111254)
#    logger.info(f"Query Result:\n{result}")
#    print(result)
#
#    assert db.connection is not None
#    assert isinstance(result, gpd.GeoDataFrame)
#    assert len(result) > 0
#
#    db.close()

def test_initialize_existing_database():
   db = AISDatabase("test_db.duckdb")
   conn = db.connection()
   # tables = conn.execute(
   #     "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
   # print(tables)
   result = conn.execute("SELECT * FROM ais_msg_123 LIMIT 10").fetchdf()
   print(result)

   assert conn is not None
   assert len(result) > 0

   conn.close()

'''
=======
#    conn.close()

>>>>>>> efae62e3cc69db8a8e44196664b891ada43daa16
def test_process_file():
    db = AISDatabase(db_path)
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


def test_search():
    """
    Test searching a specific table within the database.
    """
    db = AISDatabase(existing_db_path)

    try:

        result_mmsi = db.search(9111254)
        result_list_mmsi = db.search(mmsi=[9111254, 9111253])
        result_start_date = db.search(start_date='2016-07-28')
        result_end_date = db.search(end_date='2016-07-29')

        # WKT Test Polygon for the Pacific Ocean
        #pacific_polygon = "POLYGON((-180 -60, -180 60, 180 60, 180 -60, -180 -60))"
        #result_polygon_bounds = db.search(polygon_bounds=pacific_polygon)

        # Assert the results are valid
        assert len(result_mmsi) > 0, "Should have at one result for mmsi"
        assert len(result_list_mmsi) > 0, "Should have at one result for list_mmsi"
        assert len(result_start_date) > 0, "Should have at one result for start_date"
        assert len(result_end_date) > 0, "Should have at one result for end_date"
        #assert len(result_polygon_bounds) > 0, "Should have at one result for polygon_bounds"

    finally:
        # Close the database connection
        db.close()
        '''
