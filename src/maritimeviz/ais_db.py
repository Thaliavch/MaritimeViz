from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cache
from typing import Optional, Union, List

import duckdb
import pandas as pd
import geopandas as gpd
import json

from shapely.geometry import Point
from .constants import *
from src.maritimeviz.utils.ais_db_utils import *
from . import logger
from abc import ABC, abstractmethod

class AISDatabase:
    """
    Parent class that manages the initialization, connection, and common queries for the AIS database.
    It also provides factory methods to get message-type processors.
    """
    # module counter for database instances on same runtime
    _default_db_counter = 0
    def __init__(self, db_path: Optional[str] = None, enable_cache: bool = True):
        self._db_path = db_path if db_path else self._get_default_db_path() # create new file_name if one is not given or if given an emtpy string
        self._conn = self._init_db(self._db_path)
        self._init_tables()
        self._global_filter: Optional[dict] = None

    def _init_db(self, db_path: str) -> duckdb.DuckDBPyConnection:
        try:
            conn = duckdb.connect(db_path)
            return conn
        except Exception as e:
            print(f"Error connecting to database at {db_path}: {e}")
            raise e

    def _init_tables(self):
        try:
            # Call query to init all tables when database is created
            for query in DATABASE_ALL_TABLE_CREATION_QUERIES + DATABASE_ALL_VIEWS_CREATION_QUERIES:
                self._conn.execute(query)
        except Exception as e:
            print(f"Error connecting to database a {self._db_path}: {e}")

    @classmethod
    def _get_default_db_path(cls) -> str:
        cls._default_db_counter += 1
        return f"ais_data_{cls._default_db_counter}.duckdb"

    # TODO(Thalia): have one universal filter object and implement global set_filter function
    def set_filter(self, filter_obj: Optional[dict]) -> None:
        pass

    def clear_filter(self) -> None:
        self._filter = None

    # TODO(Thalia): See what I will do with this method. We may call it since this block of code is repeated on two methods so far.
    # def _filter_mmsi_query(self, mmsi: Union[int, List[int]], query: str, params: List) -> str:
    #     if mmsi is not None:
    #         if isinstance(mmsi, int):
    #             query += " AND mmsi = ?"
    #             params.append(mmsi)
    #         elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
    #             placeholders = ", ".join(["?"] * len(mmsi))
    #             query += f" AND mmsi IN ({placeholders})"
    #             params.extend(mmsi)
    #         else:
    #             raise ValueError("MMSI must be an integer or a list of integers.")
    #     return query

    def open(self):
        if not self._conn:
            self._conn = duckdb.connect(self._db_path)
        return self._conn

    def close(self):
        if self._conn:
            self._conn.commit()
            self._conn.close()
            self._conn = None

    def connection(self):
        return self._conn

    def path(self):
        return self._db_path

    @staticmethod
    def clear_cache() -> None:
        '''
        This will clear cache for all modules
        '''
        call_in_cached_query.cache_clear()

    def _get_view_name(self, data: str) -> str:
        """
        Determine the view name based on the 'data' parameter.

        Parameters:
            data (str): One of "all", "dynamic", or "static".

        Returns:
            str: The name of the view to query.
        """
        mapping = {
            "all": "global_ais_data",
            "dynamic": "global_ais_dynamic",
            "static": "global_ais_static"
        }
        if data not in mapping:
            raise ValueError(
                "Invalid data parameter. Must be 'all', 'dynamic', or 'static'.")
        return mapping[data]

    def _get_global_df(self, data: str = "all",
                       mmsi: Optional[int] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       polygon_bounds: Optional[str] = None,
                       as_geodf: bool = True) -> Union[
        pd.DataFrame, gpd.GeoDataFrame]:
        """
        Query the appropriate global view (all/dynamic/static) with optional filters.

        Parameters:
            data (str): Which dataset to query – "all", "dynamic", or "static".
            mmsi (Optional[int]): Optional MMSI filter.
            start_date (Optional[str]): Start date ("YYYY-MM-DD").
            end_date (Optional[str]): End date ("YYYY-MM-DD").
            polygon_bounds (Optional[str]): WKT polygon for spatial filtering.
            as_geodf (bool): If True, returns a GeoDataFrame (assumes x and y exist).

        Returns:
            DataFrame or GeoDataFrame with the query result.
        """
        view_name = self._get_view_name(data)
        query = f"SELECT * FROM {view_name} WHERE 1=1"
        params = []

        # MMSI filter
        if mmsi is not None:
            query += " AND mmsi = ?"
            params.append(mmsi)

        # Date range filter:
        if start_date:
            try:
                start_ts = date_to_tagblock_timestamp(
                    *map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            except Exception as e:
                raise ValueError(
                    "Invalid start date format. Expected YYYY-MM-DD.") from e
        if end_date:
            try:
                end_ts = date_to_tagblock_timestamp(
                    *map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)
            except Exception as e:
                raise ValueError(
                    "Invalid end date format. Expected YYYY-MM-DD.") from e

        # Polygon bounds filter (if applicable)
        if polygon_bounds:
            query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
            params.append(polygon_bounds)

        try:
            df = cached_query(self._conn, query, tuple(params), True)
        except Exception as e:
            logger.error(f"Error querying view {view_name}: {e}")
            return pd.DataFrame()

        if as_geodf and "x" in df.columns and "y" in df.columns:
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
        return df

    def search(self,data: str = "all",
                    mmsi: Optional[int] = None,
                    start_date: Optional[str] = None,
                    end_date: Optional[str] = None,
                    polygon_bounds: Optional[str] = None):
        return self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds)

    def get_geojson(self, data: str = "all",
                           mmsi: Optional[int] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           polygon_bounds: Optional[str] = None) -> dict:
        """
        Return a GeoJSON representation of global AIS data.

        Parameters:
            data (str): One of "all", "dynamic", or "static".
            ... (other filters)

        Returns:
            dict: The GeoJSON representation.
        """
        gdf = self._get_global_df(data, mmsi, start_date, end_date,
                                  polygon_bounds, as_geodf=True)
        if gdf.empty:
            logger.info(f"No AIS data available for MMSI {mmsi}")
            return {}
        if "datetime" in gdf.columns:
            gdf["datetime"] = gdf["datetime"].astype(str)
        return json.loads(gdf.to_json())

    def get_csv(self, file_path: str = "ais_data.csv",
                       data: str = "all",
                       mmsi: Optional[int] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       polygon_bounds: Optional[str] = None) -> str:
        df = self._get_global_df(data, mmsi, start_date, end_date,
                                 polygon_bounds, as_geodf=False)
        if df.empty:
            return "No data available to export."
        df.to_csv(file_path, index=False)
        return f"CSV saved at {file_path}"

    def get_parquet(self, file_path: str = "ais_data.parquet",
                           data: str = "all",
                           mmsi: Optional[int] = None,
                           start_date: Optional[str] = None,
                           end_date: Optional[str] = None,
                           polygon_bounds: Optional[str] = None) -> str:
        """
        Exports global AIS data to a Parquet file.
        """
        df = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                 as_geodf=False)
        if df.empty:
            return "No data available to export."
        df.to_parquet(file_path)
        return f"Parquet file saved at {file_path}"

    def get_json(self, file_path: str = "ais_data.json",
                        data: str = "all",
                        mmsi: Optional[int] = None,
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        polygon_bounds: Optional[str] = None):
        """
        Returns a JSON object and exports the global AIS data to a JSON file.
        """
        df = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                 as_geodf=False)
        if df.empty:
            return "No data available to export."
        with open(file_path, "w") as f:
            f.write(df.to_json())
        return json.loads(df.to_json())

    def get_shapefile(self, file_path: str = "ais_shapefile",
                             data: str = "all",
                             mmsi: Optional[int] = None,
                             start_date: Optional[str] = None,
                             end_date: Optional[str] = None,
                             polygon_bounds: Optional[str] = None) -> str:
        """
        Exports global AIS data to a Shapefile.
        """
        gdf = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                  as_geodf=True)
        if gdf.empty:
            return "No data available to export."
        gdf.to_file(file_path, driver="ESRI Shapefile")
        return f"Shapefile saved at {file_path}"

    def get_kml(self, file_path: str = "ais_data.kml",
                       data: str = "all",
                       mmsi: Optional[int] = None,
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       polygon_bounds: Optional[str] = None) -> str:
        """
        Exports global AIS data to a KML file.
        """
        gdf = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                  as_geodf=True)
        if gdf.empty:
            return "No data available to export."
        gdf.to_file(file_path, driver="KML")
        return f"KML file saved at {file_path}"

    def get_excel(self, file_path: str = "ais_data.xlsx",
                         data: str = "all",
                         mmsi: Optional[int] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         polygon_bounds: Optional[str] = None) -> str:
        """
        Exports global AIS data to an Excel file.
        """
        df = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                 as_geodf=False)
        if df.empty:
            return "No data available to export."
        df.to_excel(file_path, index=False)
        return f"Excel file saved at {file_path}"

    def get_wkt(self, mmsi: Optional[int] = None,
                       data: str = "all",
                       start_date: Optional[str] = None,
                       end_date: Optional[str] = None,
                       polygon_bounds: Optional[str] = None):
        """
        Returns global AIS data in Well-Known Text (WKT) format.
        """
        gdf = self._get_global_df(data, mmsi, start_date, end_date, polygon_bounds,
                                  as_geodf=True)
        if gdf.empty:
            return "No data available to export."
        return gdf["geometry"].apply(lambda geom: geom.wkt).tolist()

    # Factory methods for message-type processing.
    def typeA(self):
        return ClassAMessages(self._conn)

    def typeB(self):
        return ClassBMessages(self._conn)

    def others(self):
        return OtherMessages(self._conn)


class BaseMessageProcessor:
    """
    Base class for processing AIS messages from files.
    Subclasses should implement _filter_message() and _insert_message to filter and insert
    messages of a specific type into their designated tables.
    """
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn
        self._filter: Optional[dict] = None
        print("BaseMessageProcessor instantiated ...") #debug

    '''
    Private methods
    '''
    def _process_chunk(self, chunk: list):
        """
            Process a chunk of lines and insert messages into the database.
            """
        import ais.stream  # Import required for threading compatibility
        for msg in ais.stream.decode(chunk):
            try:
                if self._filter_message(msg):  # Filter messages
                    self._insert_message(msg)  # Insert message into database
            except Exception as e:
                logger.error(f"Error processing message: {msg} - {e}")

    '''
    Abstract methods
    '''
    @abstractmethod
    def _filter_message(self, msg: dict) -> bool:
        """Return True if the message matches this processor's criteria."""
        raise NotImplementedError("Subclasses must implement _filter_message.")

    @abstractmethod
    def _insert_message(self, msg: dict):
        """Insert the message into the appropriate table."""
        raise NotImplementedError("Subclasses must implement _insert_message.")

    # TODO(Thalia): to update view when new data is inserted
    def _update_global_views(self):
        try:
            for query in DATABASE_ALL_VIEWS_CREATION_QUERIES:
                self._conn.execute(query)
        except Exception as e:
            logger.error(f"Error updating views: {e}")

    @abstractmethod
    def search(self, **kwargs) -> pd.DataFrame:
        """Query vessel dynamic data applying given arguments"""
        raise NotImplementedError("Subclasses must implement search.")

    @abstractmethod
    def static_info(self, **kwargs) -> pd.DataFrame:
        """Query vessel static data applying given arguments"""
        raise NotImplementedError("Subclasses must implement static_info.")

    @abstractmethod
    def set_filter(self, filter_obj: Optional[dict]) -> None:
        """ Set filter object for querying data from database """
        raise NotImplementedError("Subclasses must implement set_filter.")
    '''
    Public methods start here
    '''
    # TODO(Thalia) Update so the process function checks for file extension and call function to process raw or csv file types.
    def process(self, file_path: str, threading_stats=(4, 500)):
        with ThreadPoolExecutor(max_workers=threading_stats[0]) as executor:
            futures = [
                executor.submit(self._process_chunk, chunk)
                for chunk in split_file_generator(file_path, threading_stats[1])
            ]
            for future in as_completed(futures):
                future.result()
        self._update_global_views()

    '''
    Export Methods
    '''
    # Note that because search() is abstract, the methods below will query from each
    # subclass' respective table.
    def get_geojson(self, mmsi: None, start_date=None, end_date=None,
                    polygon_bounds=None):
        """
        Return a GeoJSON representation of the vessel route.
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


class ClassAMessages(BaseMessageProcessor):
    """
    Processes Class A messages (Types 1, 2, 3 and static Type 5).
    """
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        # Process only if the message id is one of the Class A types.
        return msg.get('id') in {1, 2, 3, 5}

    def _insert_message(self, msg: dict):
        # Use .get() to provide default values for missing attributes
        # Note in _process_chunk we are already filtering per messages of type A
        if msg.get('id') == 5:
            query = """
                    INSERT INTO ais_msg_5 (
                        id, repeat_indicator, mmsi, ais_version, imo, call_sign, ship_name,
                        type_of_ship_and_cargo, to_bow, to_stern, to_port, to_starboard,
                        position_fixing_device, eta, max_present_static_draught, destination, dte
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """
            params = (
                msg.get('id'),
                msg.get('repeat_indicator'),
                msg.get('mmsi'),
                msg.get('ais_version_indicator'),
                msg.get('imo'),
                msg.get('call_sign'),
                msg.get('ship_name'),
                msg.get('type_of_ship_and_cargo'),
                msg.get('dimension_to_bow'),
                msg.get('dimension_to_stern'),
                msg.get('dimension_to_port'),
                msg.get('dimension_to_starboard'),
                msg.get('position_fixing_device'),
                msg.get('eta'),
                msg.get('max_present_static_draught'),
                msg.get('destination'),
                msg.get('dte')
            )
        else:
            # For dynamic messages (Type 1, 2, 3)
            query = """
                    INSERT INTO ais_msg_123 (
                        id, repeat_indicator, mmsi, nav_status, rot_over_range, rot, sog,
                        position_accuracy, x, y, cog, true_heading, timestamp, special_manoeuvre,
                        spare, raim, sync_state, slot_timeout, slot_number, tagblock_group,
                        tagblock_line_count, tagblock_station, tagblock_timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """
            params = (
                msg.get('id'),
                msg.get('repeat_indicator'),
                msg.get('mmsi'),
                msg.get('nav_status'),
                msg.get('rot_over_range'),
                msg.get('rot'),
                msg.get('sog'),
                msg.get('position_accuracy'),
                msg.get('x'),
                msg.get('y'),
                msg.get('cog'),
                msg.get('true_heading'),
                msg.get('timestamp'),
                msg.get('special_manoeuvre'),
                msg.get('spare'),
                msg.get('raim'),
                msg.get('sync_state'),
                msg.get('slot_timeout'),
                msg.get('slot_number', None),  # Default to None if not present
                json.dumps(msg.get('tagblock_group', {})),
                # Default to an empty JSON object
                msg.get('tagblock_line_count'),
                msg.get('tagblock_station'),
                msg.get('tagblock_timestamp')
            )
        self._conn.execute(query, params)

    def set_filter(self, filter_obj: Optional[dict]) -> None:
        if filter_obj is not None:
            if not isinstance(filter_obj, dict):
                raise TypeError("Filter object must be a dictionary.")
            if not set(filter_obj.keys()).issubset(ALLOWED_FILTER_KEYS_CLASS_A):
                raise TypeError("Filter object contains invalid keys.") # TODO(Thalia): add link to documentation in error message
        self._filter = filter_obj

    def search(self,
               mmsi: Optional[Union[int, List[int]]] = None,
               conn: Optional[duckdb.DuckDBPyConnection] = None,
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
        - conn (duckdb.DuckDBPyConnection, optional): DuckDB connection (defaults to self._conn).
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
        # TODO(Thalia) I wonder if this is really necessary. May refactor later ....
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

            # Date range filter:
            if start_date:
                try:
                    start_ts = date_to_tagblock_timestamp(
                        *map(int, start_date.split("-")))
                    query += " AND tagblock_timestamp >= ?"
                    params.append(start_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid start date format. Expected YYYY-MM-DD.") from e
            if end_date:
                try:
                    end_ts = date_to_tagblock_timestamp(
                        *map(int, end_date.split("-")))
                    query += " AND tagblock_timestamp <= ?"
                    params.append(end_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid end date format. Expected YYYY-MM-DD.") from e

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
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(
                    columns=["geometry"])  # Return empty GeoDataFrame

            # Build GeoDataFrame
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            print(gdf) #debuging
            return gdf

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on failure


    # TODO(Thalia): Change so it also takes a list of vessels
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
            df = cached_query(conn, query, params, True)

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


class ClassBMessages(BaseMessageProcessor):
    """
    Processes Class B messages (Types 18, 19 and static Type 24).
    """
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)
        print("Inside class B messages constructor...") #debugging

    def _filter_message(self, msg: dict) -> bool:
        # Process only if the message id is one of the Class B types.
        return msg.get('id') in {18, 19, 24}

    def _insert_message(self, msg: dict):
        if msg.get('id') == 24:
            query = """
                INSERT INTO ais_msg_24 (
                    id, repeat_indicator, mmsi, part_num, name, type_and_cargo,
                    vendor_id, callsign, dim_a, dim_b, dim_c, dim_d, spare,
                    tagblock_group, tagblock_line_count, tagblock_station, tagblock_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                msg.get('id'),
                msg.get('repeat_indicator'),
                msg.get('mmsi'),
                msg.get('part_num'),
                msg.get('name'),
                msg.get('type_and_cargo'),
                msg.get('vendor_id'),
                msg.get('call_sign'),
                msg.get('dimension_to_bow'),
                msg.get('dimension_to_stern'),
                msg.get('dimension_to_port'),
                msg.get('dimension_to_starboard'),
                msg.get('spare'),
                json.dumps(msg.get('tagblock_group', {})),
                msg.get('tagblock_line_count'),
                msg.get('tagblock_station'),
                msg.get('tagblock_timestamp')
            )
        else:
            print("Message is 18 or 19")
            # For dynamic messages (Types 18 and 19)
            query = """
                INSERT INTO ais_msg_18_19 (
                    id, repeat_indicator, mmsi, spare, sog, position_accuracy,
                    x, y, cog, true_heading, timestamp, spare2, unit_flag, display_flag,
                    dsc_flag, band_flag, m22_flag, mode_flag, raim, commstate_flag,
                    commstate_cs_fill, tagblock_group, tagblock_line_count, tagblock_station,
                    tagblock_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                msg.get('id'),
                msg.get('repeat_indicator'),
                msg.get('mmsi'),
                msg.get('spare'),
                msg.get('sog'),
                msg.get('position_accuracy'),
                msg.get('x'),
                msg.get('y'),
                msg.get('cog'),
                msg.get('true_heading'),
                msg.get('timestamp'),
                msg.get('spare2'),
                msg.get('unit_flag'),
                msg.get('display_flag'),
                msg.get('dsc_flag'),
                msg.get('band_flag'),
                msg.get('m22_flag'),
                msg.get('mode_flag'),
                msg.get('raim'),
                msg.get('commstate_flag'),
                msg.get('commstate_cs_fill'),
                json.dumps(msg.get('tagblock_group', {})),
                msg.get('tagblock_line_count'),
                msg.get('tagblock_station'),
                msg.get('tagblock_timestamp')
            )
        self._conn.execute(query, params)

    # TODO(Thalia): write function implementation and filter object for messages of class B
    def set_filter(self, filter_obj: Optional[dict]) -> None:
        pass

    #TODO(Update this search function and global search function)
    def search(self,
               mmsi: Optional[Union[int, List[int]]] = None,
               start_date: Optional[str] = None,
               end_date: Optional[str] = None,
               **kwargs) -> pd.DataFrame:
        """
        Searches dynamic Class B messages in the ais_msg_18_19 table.
        """
        query = "SELECT * FROM ais_msg_18_19 WHERE 1=1"
        params = []
        if mmsi is not None:
            if isinstance(mmsi, int):
                query += " AND mmsi = ?"
                params.append(mmsi)
            elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                placeholders = ", ".join(["?"] * len(mmsi))
                query += f" AND mmsi IN ({placeholders})"
                params.extend(mmsi)
            else:
                raise ValueError("MMSI must be an integer or a list of integers.")

        # Date range filter:
        if start_date:
            try:
                start_ts = date_to_tagblock_timestamp(
                    *map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            except Exception as e:
                raise ValueError(
                    "Invalid start date format. Expected YYYY-MM-DD.") from e
        if end_date:
            try:
                end_ts = date_to_tagblock_timestamp(
                    *map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)
            except Exception as e:
                raise ValueError(
                    "Invalid end date format. Expected YYYY-MM-DD.") from e

        try:
            return cached_query(self._conn, query, params, True)
        except Exception as e:
            logger.error(f"Error executing dynamic search for ClassB: {e}")
            return pd.DataFrame()

    def static_info(self,
                    mmsi: Optional[Union[int, List[int]]] = None,
                    **kwargs) -> pd.DataFrame:
        """
        Retrieves static/voyage-related Class B information from the ais_msg_24 table.
        """
        query = "SELECT * FROM ais_msg_24 WHERE 1=1"
        params = []
        if mmsi is not None:
            if isinstance(mmsi, int):
                query += " AND mmsi = ?"
                params.append(mmsi)
            elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                placeholders = ", ".join(["?"] * len(mmsi))
                query += f" AND mmsi IN ({placeholders})"
                params.extend(mmsi)
            else:
                raise ValueError("MMSI must be an integer or a list of integers.")

        try:
            return cached_query(self._conn, query, params, True)
        except Exception as e:
            logger.error(f"Error executing static_info search for ClassB: {e}")
            return pd.DataFrame()

# TODO(Thalia) need to define how to process rest of AIS messages types.
class OtherMessages(BaseMessageProcessor):
    """
    Processes all remaining AIS messages not classified as Class A or B.
    """
    pass


