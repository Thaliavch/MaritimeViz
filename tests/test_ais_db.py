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

def test_initialize_existing_database_works():
    db = AISDatabase(existing_db_path)
    result = db.search_mmsi(9111254)
    logger.info(f"Query Result:\n{result}")
    print(result)

    assert db.connection is not None
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) > 0

    db.close()

def test_initialize_existing_database():
    db = AISDatabase("test_db.duckdb")
    conn = db.connection()
    result = conn.execute("SELECT * FROM vessels").fetchdf()

    print(result)

    assert conn.connection is not None
    assert isinstance(result, gpd.GeoDataFrame)
    assert len(result) > 0

    conn.close()

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
    assert row_count_5 > 0, "Table ais_msg_5 should not be empty after processing."

    db.close()


