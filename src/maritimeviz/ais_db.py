from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from typing import Optional

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
    def __init__(self, db_path="ais_data.duckdb", enable_cache=True):
        self._db_path = db_path
        self._conn = self.init_db(db_path)
        self._filter: Optional[FilterCriteria] = None  # Store user-defined filters
        # TODO(Thalia) maybe add options for using to disable caching at class or method levels, since it may cause issues
        #self.enable_cache = enable_cache

    def set_filter(self, filter_obj: Optional[FilterCriteria]):
        """Sets or clears the filter object."""
        if filter_obj is not None and not isinstance(filter_obj, dict):
            raise TypeError(
                "Filter object must be a dictionary following FilterCriteria structure.")

        self._filter = filter_obj

    def clear_filter(self):
        self._filter = None

    # All results will be verified here for previous cache
    @lru_cache(maxsize=100)  # Cache up to 100 unique query results
    def _cached_query(self, query, params, df=False):
        """
        Verify requested query for cached results.
        """
        if not params:
            return self._conn.execute(
                query).fetchdf() if df else self._conn.execute(
                query).fetchall()

        if type(params) is not tuple:
            if type(params) is not list:
                params = [params]
            params = tuple(params)

        return self._conn.execute(query,
                                  params).fetchdf() if df else self._conn.execute(
            query, params).fetchall()

    # TODO (Thalia): Move to utitilies and use in search() and static_info()
    def filter_mmsi_query(mmsi: int | list[int], query: str,
                          params: list) -> str:
        """
        Modifies the query string to filter by MMSI and updates the params list.

        Args:
            mmsi (int | list[int] | None): The MMSI or list of MMSIs to filter.
            query (str): The base SQL query.
            params (list): The list of query parameters (mutated in place).

        Returns:
            str: The modified query string with MMSI filtering applied.
        """
        if mmsi is not None:
            if isinstance(mmsi, int):
                query += " AND mmsi = ?"
                params.append(mmsi)
            elif isinstance(mmsi, list) and all(
                isinstance(i, int) for i in mmsi):
                placeholders = ', '.join('?' * len(mmsi))
                query += f" AND mmsi IN ({placeholders})"
                params.extend(mmsi)
            else:
                raise ValueError(
                    "MMSI must be an integer or a list of integers.")

        return query

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


    def process_file(self, file_path, threading_stats=(4, 500)):
        """
        Process the AIS file using on-the-fly chunk splitting and multithreading.
        """
        try:
            threading_stats = optimal_threading_stats(file_path)  # thread and chunk size
            logger.info(f"Threading parameters: {threading_stats}")
        except Exception as e:
            logger.warning(f"Using default threading values: 4 threads and chunks of 500 lines")

        max_threads, chunk_size = threading_stats
        futures = []

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for chunk in split_file_generator(file_path, chunk_size):
                # Submit chunk processing asynchronously
                future = executor.submit(process_chunk_to_db, self._conn, chunk)
                futures.append(future)
                #as_completed() ensures that results are processed immediately

            # Ensure all tasks complete before committing
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")

        # Ensure transaction commits after all threads finish
        with self._conn.transaction():
            self._conn.commit()



    def open(self):
        """
        Open database connection if not already open
        """
        if not self._conn:
            self._conn = duckdb.connect(self._db_path)
            return self._conn
        else:
            print("Returning existing connection")
            return self._conn


    def close(self):
        """
        Close the database connection.
        """
        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def connection(self):
        """
        Return current connection
        """
        return self._conn

    def clear_cache(self):
        """Manually clear the search cache."""
        self._cached_query.cache_clear()

    def search(self,
               mmsi: Optional[Union[int, List[int]]] = None,
               conn: Optional[duckdb.Connection] = None,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               polygon_bounds: Optional[str] = None,
               min_velocity: Optional[float] = None,
               max_velocity: Optional[float] = None,
               direction: Optional[str] = None,
               min_turn_rate: Optional[float] = None,
               max_turn_rate: Optional[float] = None) -> gpd.GeoDataFrame:
        """
        Search AIS data with optional filters.

        Parameters:
        - mmsi (int | list[int], optional): MMSI number(s) to filter.
        - conn (duckdb.Connection, optional): DuckDB connection (defaults to self._conn).
        - start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        - end_date (str, optional): End date in 'YYYY-MM-DD' format.
        - polygon_bounds (str, optional): WKT polygon for spatial filtering.
        - min_velocity (float, optional): Minimum speed over ground (sog).
        - max_velocity (float, optional): Maximum speed over ground (sog).
        - direction (str, optional): Cardinal direction ("N", "E", "S", or "W") to filter by course over ground (cog).
        - min_turn_rate (float, optional): Minimum rate of turn (rot).
        - max_turn_rate (float, optional): Maximum rate of turn (rot).

        Returns:
        - gpd.GeoDataFrame: Filtered AIS data.
        """
        if not conn:
            conn = self._conn

        try:
            # Base query
            query = "SELECT * FROM ais_msg_123 WHERE 1=1"
            params = []

            # Apply stored filter if set (stored filter values are used unless explicitly overridden)
            if self._filter:
                mmsi = mmsi or self._filter.get("mmsi")
                start_date = start_date or self._filter.get("start_date")
                end_date = end_date or self._filter.get("end_date")
                polygon_bounds = polygon_bounds or self._filter.get(
                    "polygon_bounds")
                min_velocity = min_velocity or self._filter.get("min_velocity")
                max_velocity = max_velocity or self._filter.get("max_velocity")
                direction = direction or self._filter.get("direction")
                min_turn_rate = min_turn_rate or self._filter.get(
                    "min_turn_rate")
                max_turn_rate = max_turn_rate or self._filter.get(
                    "max_turn_rate")

            # MMSI filtering
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(
                    isinstance(i, int) for i in mmsi):
                    placeholders = ', '.join(['?'] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError(
                        "MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date and end_date:
                try:
                    start_timestamp = date_to_tagblock_timestamp(
                        *map(int, start_date.split("-")))
                    end_timestamp = date_to_tagblock_timestamp(
                        *map(int, end_date.split("-")))
                    query += " AND tagblock_timestamp BETWEEN ? AND ?"
                    params.extend([start_timestamp, end_timestamp])
                except ValueError:
                    raise ValueError(
                        "Invalid date format. Expected YYYY-MM-DD.")

            # Polygon bounds filter (using parameterized query)
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            # Velocity filter
            if min_velocity is not None:
                query += " AND sog >= ?"
                params.append(min_velocity)
            if max_velocity is not None:
                query += " AND sog <= ?"
                params.append(max_velocity)

            # Turn rate filter
            if min_turn_rate is not None:
                query += " AND rot >= ?"
                params.append(min_turn_rate)
            if max_turn_rate is not None:
                query += " AND rot <= ?"
                params.append(max_turn_rate)

            # Direction filter (based on course over ground, cog)
            if direction:
                direction = direction.upper()
                if direction == "N":
                    # North: cog >= 315 or cog < 45
                    query += " AND (cog >= ? OR cog < ?)"
                    params.extend([315, 45])
                elif direction == "E":
                    query += " AND (cog >= ? AND cog < ?)"
                    params.extend([45, 135])
                elif direction == "S":
                    query += " AND (cog >= ? AND cog < ?)"
                    params.extend([135, 225])
                elif direction == "W":
                    query += " AND (cog >= ? AND cog < ?)"
                    params.extend([225, 315])
                else:
                    raise ValueError(
                        "Direction must be one of 'N', 'E', 'S', 'W'.")

            # Log query for debugging
            logger.info(f"Executing query: {query} with params: {params}")

            # Execute query
            df = conn.execute(query, params).fetchdf()
            if df.empty:
                return gpd.GeoDataFrame(
                    columns=["geometry"])  # Return empty GeoDataFrame

            # Build GeoDataFrame
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            return gdf

        except duckdb.DatabaseError as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on failure


    # Change so it also takes a list of vessels
    def static_info(self, mmsi: int | list[int] = None, conn=None):
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
            conn = self._conn

        try:
            # Base query
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
            params = []

            # Handle MMSI filtering
            if mmsi is not None:
                if isinstance(mmsi, int):
                    query += " WHERE mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(
                    isinstance(i, int) for i in mmsi):
                    query += f" WHERE mmsi IN ({', '.join('?' * len(mmsi))})"
                    params.extend(mmsi)
                else:
                    raise ValueError(
                        "MMSI must be an integer or a list of integers.")

            # Execute query
            df = self._cached_query(query, params, True)

            if df.empty:
                return {"No static MMSI info found."}

            # --- Optionally retrieve more from an external table or API not sure aobut global fish wash---
            # Suppose we have a 'vessel_details' table with columns [mmsi, captain, fleet_operator, flag]
            # ext_query = """SELECT captain, fleet_operator, flag FROM vessel_details WHERE mmsi = ?"""
            # ext_info = conn.execute(ext_query, [mmsi]).fetchone()
            # if ext_info:
            #     info_dict["captain"] = ext_info[0]
            #     info_dict["fleet_operator"] = ext_info[1]
            #     info_dict["flag"] = ext_info[2]

            return df

        except Exception as e:
            logger.error(f"Error retrieving vessel info: {e}")
            return {"mmsi": mmsi, "error": str(e)}

    def get_geojson(self, mmsi: None, start_date=None, end_date=None,
                    polygon_bounds=None):
        """
        Return a GeoJSON representation of the vessel route (from `ais_msg_123` data).
        This GeoJSON can be passed directly to a Leafmap/Geemap layer.
        """
        try:
            gdf = self.search(
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

    def get_csv(self, file_path="ais_data.csv", mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Exports AIS data to a CSV file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_csv(file_path, index=False)
        return f"CSV saved at {file_path}"

    def get_parquet(self, file_path="ais_data.parquet", mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Exports AIS data to a Parquet file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_parquet(file_path)
        return f"Parquet file saved at {file_path}"

    def get_json(self, file_path="ais_data.json", mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Return JSON object and export to json file
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."
        with open(file_path, "w") as f:
            f.write(gdf.to_json())
        return json.loads(gdf.to_json())

    def get_shapefile(self, file_path="ais_shapefile", mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Exports AIS data to a Shapefile.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_file(file_path, driver="ESRI Shapefile")
        return f"Shapefile saved at {file_path}"

    def get_kml(self,file_path="ais_data.kml", mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Exports AIS data to a KML file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_file(file_path, driver="KML")
        return f"KML file saved at {file_path}"

    def get_excel(self, file_path="ais_data.xlsx",  mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Exports AIS data to an Excel file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_excel(file_path, index=False)
        return f"Excel file saved at {file_path}"

    def get_wkt(self, mmsi=None, start_date=None, end_date=None, polygon_bounds=None):
        """
        Returns AIS data in Well-Known Text (WKT) format.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        return gdf["geometry"].apply(lambda geom: geom.wkt).tolist()


