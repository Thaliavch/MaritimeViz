import pytest
from src.maritimeviz.ais_db import AISDatabase

file_path = "tests/ais_2016_07_28_aa"
db_path ="test_db.duckdb"

def test_initialize_database():
    db = AISDatabase(db_path)
    db.initialize_database()
    assert db.connection is not None
    db.close()

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

