from functools import lru_cache
from . import logger
from concurrent.futures import ThreadPoolExecutor
import ais
import duckdb
from src.maritimeviz.utils.ais_db_utils import *



class AISDatabase:
    """
    Class to manage the initialization, population, and interaction with the AIS database.
    """
    def __init__(self, db_path="ais_data.duckdb"):
        self.db_path = db_path
        self.connection = self.init_db(db_path)

    def init_db(self, db_path):
        """
        Initialize DuckDB database with schema.
        """
        connection = duckdb.connect(db_path)
        create_table_query_for_1_2_3 = """
        CREATE TABLE IF NOT EXISTS ais_msg_123 (
            id INTEGER,
            repeat_indicator INTEGER,
            mmsi BIGINT,
            nav_status INTEGER,
            rot_over_range BOOLEAN,
            rot FLOAT,
            sog FLOAT,
            position_accuracy INTEGER,
            x DOUBLE,
            y DOUBLE,
            cog FLOAT,
            true_heading INTEGER,
            timestamp INTEGER,
            special_manoeuvre INTEGER,
            spare INTEGER,
            raim BOOLEAN,
            sync_state INTEGER,
            slot_timeout INTEGER,
            slot_number INTEGER,
            tagblock_group JSON,
            tagblock_line_count INTEGER,
            tagblock_station TEXT,
            tagblock_timestamp BIGINT
        );
        """
        create_table_query_for_5 = """
        CREATE TABLE IF NOT EXISTS ais_msg_5 (
            id INTEGER,
            repeat_indicator INTEGER,
            mmsi BIGINT,
            ais_version INTEGER,
            imo BIGINT,
            call_sign VARCHAR,
            ship_name VARCHAR,
            type_of_ship_and_cargo INTEGER,
            to_bow INTEGER,
            to_stern INTEGER,
            to_port INTEGER,
            to_starboard INTEGER,
            position_fixing_device INTEGER,
            eta VARCHAR,
            max_present_static_draught FLOAT,
            destination VARCHAR,
            dte BOOLEAN
        );
        """
        connection.execute(create_table_query_for_1_2_3)
        connection.execute(create_table_query_for_5)
        return connection


    def process_file(self, file_path, threading_stats=(4,500)):
      """
      Process the AIS file using on-the-fly chunk splitting and multithreading.
      """

      try:
        threading_stats = optimal_threading_stats(file_path) # thread and chunk size
        logger.info(f"Threading parameters: {threading_stats}")
      except:
        logger.info("Using default threading values: 4 threads and chunks of 500 lines")


      # Use a ThreadPoolExecutor for processing
      with ThreadPoolExecutor(max_workers= threading_stats[0]) as executor:
          for chunk in split_file_generator(file_path, threading_stats[1]):
            executor.submit(process_chunk_to_db, self.connection, chunk)


    def open_conn(self):
        """
        Open database connection if not already open
        """
        if not self.connection:
            self.connection = duckdb.connect(self.db_path)
            return self.connection
        else:
            print("Returning existing connection")
            return self.connection


    def close_conn(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_conn(self):
        """
        Return current connection
        """
        return self.connection

    # All results will be verified here for previous cache
    @lru_cache(maxsize=100)  # Cache up to 100 unique query results
    def _cached_query(self, query, params):
        """
        Verify requested query for cached results.
        """
        return self.connection.execute(query, params).fetchall()

    def info(self, mmsi, conn=None, start_date=None, end_date=None,
             polygon_bounds=None):
        """
        Retrieve AIS data for a specific MMSI with optional date and geographical bounds.

        Parameters:
        - mmsi (int): The MMSI (Maritime Mobile Service Identity) to filter data.
        - conn: Optional. The DuckDB connection. Defaults to self.connection.
        - start_date (str): Optional. Start of the date range (ISO 8601 format: 'YYYY-MM-DD').
        - end_date (str): Optional. End of the date range (ISO 8601 format: 'YYYY-MM-DD').
        - polygon_bounds (list of tuples): Optional. List of (longitude, latitude) tuples forming a polygon. Last tuple is the closing point.

        Returns:
        - For now it returns a list of rows that match the criteria, but we will add a new table for the given mmsi to
        the database and return a dataframe-like object
        """
        if not conn:
            conn = self.connection

        try:
            # Base query
            query = """
            SELECT *
            FROM ais_msg_123
            WHERE mmsi = ?
            """

            # Parameters to pass to the query
            params = [mmsi]

            # Add date range filter
            if start_date and end_date:
                start_tagblock_timestamp = date_to_tagblock_timestamp(
                    int(start_date[:4]), int(start_date[5:7]),
                    int(start_date[8:10]))
                end_tagblock_timestamp = date_to_tagblock_timestamp(
                    int(end_date[:4]), int(end_date[5:7]), int(end_date[8:10]))
                query += " AND tagblock_timestamp BETWEEN ? AND ?"
                params.extend(
                    [start_tagblock_timestamp, end_tagblock_timestamp])

            # Add polygon bounds filter
            if polygon_bounds:
                query += """
                AND ST_Within(
                    ST_Point(x, y), 
                    ST_GeomFromText(?)
                )
                """
                params.append(polygon_bounds)

            # Execute query and fetch results
            results = self._cached_query(query, tuple(params))
            return results

        except Exception as e:
            logger.error(f"Error retrieving data: {e}")
            return []

    def query_by_region_and_time(self, conn=None, polygon_bounds=None, start_date=None, end_date=None):
    """
    Retrieve all information for data points within a polygon and a specific time range.

    Parameters:
    - conn: Optional. The DuckDB connection. Defaults to self.connection.
    - polygon_bounds (str): Polygon bounds in WKT format.
    - start_date (str): Optional. Start of the date range (ISO 8601 format: 'YYYY-MM-DD').
    - end_date (str): Optional. End of the date range (ISO 8601 format: 'YYYY-MM-DD').

    Returns:
    - pandas.DataFrame containing rows matching the criteria.
    """
    if not conn:
        conn = self.connection

    try:
        # Base query
        query = "SELECT * FROM ais_msg_123 WHERE 1=1"
        params = []

        # Add date range filter
        if start_date and end_date:
            start_timestamp = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
            end_timestamp = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
            query += " AND tagblock_timestamp BETWEEN ? AND ?"
            params.extend([start_timestamp, end_timestamp])

        # Add polygon bounds filter
        if polygon_bounds:
            query += """
            AND ST_Within(
                ST_Point(x, y),
                ST_GeomFromText(?)
            )
            """
            params.append(polygon_bounds)

        # Execute query and fetch results
        results = self._cached_query(query, tuple(params))
        return results

    except Exception as e:
        logger.error(f"Error retrieving data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


    # def info(self, conn=None):
    #     if not conn:
    #         conn = self.connection
    #
    #     try:
    #         query


