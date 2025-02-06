from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache

import duckdb
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from .constants import *

from src.maritimeviz.utils.ais_db_utils import *
from . import logger
import json
import geojson


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

      self.connection.commit()


    def open(self):
        """
        Open database connection if not already open
        """
        if not self.connection:
            self.connection = duckdb.connect(self.db_path)
            return self.connection
        else:
            print("Returning existing connection")
            return self.connection


    def close(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.commit()
            self.connection.close()
            self.connection = None

    def connection(self):
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
        if not params:
            return self.connection.execute(query).fetchall()

        if type(params) is not tuple:
            if type(params) is not list:
                params = [params]
            params = tuple(params)

        return self.connection.execute(query, params).fetchall()

    def search(self, mmsi=None, conn=None, start_date=None, end_date=None, polygon_bounds=None, styled=True):
        """
        Consolidated search function to retrieve AIS data based on optional filters.

        Parameters:
        - mmsi (int, optional): The MMSI number of the vessel to filter by.
        - conn (duckdb.Connection, optional): The DuckDB connection to execute the query. Defaults to `self.connection`.
        - start_date (str, optional): Start date in ISO 8601 format ('YYYY-MM-DD').
        - end_date (str, optional): End date in ISO 8601 format ('YYYY-MM-DD').
        - polygon_bounds (list of tuples or str, optional): A bounding box or WKT polygon for spatial filtering.
        - styled (bool, optional): Whether to return a styled GeoDataFrame. Defaults to True.

        Returns:
        - gpd.GeoDataFrame: Filtered AIS data as a GeoDataFrame.
        """
        if not conn:
            conn = self.connection

        try:
            # Base query
            query = "SELECT * FROM ais_msg_123 WHERE 1=1"
            params = []

            # Add MMSI filter
            if mmsi:
                query += " AND mmsi = ?"
                params.append(mmsi)

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

            # Execute query
            query_out = self._cached_query(query, tuple(params))
            if not query_out:
                return gpd.GeoDataFrame(columns=["geometry"])

            # Build GeoDataFrame
            df = pd.DataFrame(query_out, columns=AIS_MSG_123_COLUMNS)
            df["geometry"] = df.apply(lambda row: Point(row["x"], row["y"]), axis=1)
            df["datetime"] = pd.to_datetime(df["tagblock_timestamp"], unit="s", utc=True)
            df.sort_values(by="tagblock_timestamp", inplace=True)
            df.reset_index(drop=True, inplace=True)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")  # lat/lon WGS84

            if styled:
                return gdf.style.set_table_styles([{
                    "selector": "th, td",
                    "props": [("border", "1px solid black"), ("text-align", "left")]
                }])

            return gdf

        except Exception as e:
            logger.error(f"Error retrieving data: {e}")
            return gpd.GeoDataFrame()  # Return empty GeoDataFrame on error

    # Change so it also takes a list of vessels
    def get_vessel_info(self, mmsi=None, conn=None, styled=True):
        """
        Retrieves vessel static information from `ais_msg_5`.

        Example AIS fields from type 5 messages:
          - ship_name
          - imo
          - call_sign
          - type_of_ship_and_cargo
          - destination
          - max_present_static_draught
        """
        if not conn:
            conn = self.connection

        try:
            query = """
                SELECT
                    mmsi,
                    ship_name,
                    imo,
                    call_sign,
                    type_of_ship_and_cargo,
                    destination,
                    max_present_static_draught
                FROM ais_msg_5
            """
            if mmsi:
                query += " WHERE mmsi = ?"

            results = self._cached_query(query, mmsi)
            # Add option for processing also a list of mmsis

            if not results:
                return {"No static MMSI info found."}

            df = pd.DataFrame(results, columns=AIS_MSG_5_COLUMNS)

            # --- Optionally retrieve more from an external table or API not sure aobut global fish wash---
            # Suppose we have a 'vessel_details' table with columns [mmsi, captain, fleet_operator, flag]
            # ext_query = """SELECT captain, fleet_operator, flag FROM vessel_details WHERE mmsi = ?"""
            # ext_info = conn.execute(ext_query, [mmsi]).fetchone()
            # if ext_info:
            #     info_dict["captain"] = ext_info[0]
            #     info_dict["fleet_operator"] = ext_info[1]
            #     info_dict["flag"] = ext_info[2]

            if styled:
                return df.style.set_table_styles([{"selector": "th, td", "props": [
                    ("border", "1px solid black"), ("text-align", "left")]}])
            return df

        except Exception as e:
            logger.error(f"Error retrieving vessel info: {e}")
            return {"mmsi": mmsi, "error": str(e)}

    def get_geojson(self, mmsi: int, start_date=None, end_date=None,
                    polygon_bounds=None):
        """
        Return a GeoJSON representation of the vessel route (from `ais_msg_123` data).
        This GeoJSON can be passed directly to a Leafmap/Geemap layer.
        """
        try:
            gdf = self.search_mmsi(
                mmsi=mmsi,
                start_date=start_date,
                end_date=end_date,
                polygon_bounds=polygon_bounds,
                styled=False
            )
            if gdf.empty:
                logger.info(f"No AIS data found for {mmsi}")
                return {}

            # Setting datetime to json serializable format
            gdf["datetime"] = gdf["datetime"].astype(str)


            # Convert to GeoJSON
            # gdf.to_json() returns a JSON string; we can convert it to a dictionary with json.loads
            geojson_str = gdf.to_json()

            geojson_dict = json.loads(geojson_str)
            return geojson_dict

        except Exception as e:
            logger.error(f"Error generating GeoJSON for MMSI {mmsi}: {e}")
            return {}


