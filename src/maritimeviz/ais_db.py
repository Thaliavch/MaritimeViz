import json
from abc import abstractmethod
from typing import Optional, Union, List

import duckdb
import geopandas as gpd
import pandas as pd

from . import logger
from .constants import (
    DATABASE_ALL_TABLE_CREATION_QUERIES,
    DATABASE_ALL_VIEWS_CREATION_QUERIES,
    ALLOWED_FILTER_KEYS,
    ALLOWED_FILTER_KEYS_CLASS_A,
    ALLOWED_FILTER_KEYS_CLASS_B,
)
from .utils.ais_db_utils import (
    call_in_cached_query,
    date_to_tagblock_timestamp,
    cached_query,
    split_file_generator,
    guess_vessel_type,
)


class AISDatabase:
    """
    Parent class that manages the initialization, connection, and common
    queries for the AIS database. It also provides factory methods to get
    message-type processors.
    """

    # module counter for database instances on same runtime
    _default_db_counter = 0

    def __init__(self, db_path: Optional[str] = None, enable_cache: bool = True):
        self._db_path = (
            db_path if db_path else self._get_default_db_path()
        )  # create new file_name if one is not given or if given an emtpy string
        self._conn = self._init_db(self._db_path)
        self._init_tables()
        self._filter: Optional[dict] = None

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
            for query in (
                DATABASE_ALL_TABLE_CREATION_QUERIES
                + DATABASE_ALL_VIEWS_CREATION_QUERIES
            ):
                self._conn.execute(query)
        except Exception as e:
            print(f"Error connecting to database a {self._db_path}: {e}")

    @classmethod
    def _get_default_db_path(cls) -> str:
        cls._default_db_counter += 1
        return f"ais_data_{cls._default_db_counter}.duckdb"

    def set_filter(self, filter_obj: Optional[dict]) -> None:
        if filter_obj is not None:
            if not isinstance(filter_obj, dict):
                raise TypeError("Filter object must be a dictionary.")
            if not set(filter_obj.keys()).issubset(ALLOWED_FILTER_KEYS):
                raise TypeError(
                    "Filter object contains invalid keys."
                )  # TODO(Thalia): add link to documentation in error message
        self._filter = filter_obj

    def clear_filter(self) -> None:
        self._filter = None

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
        """
        This will clear cache for all modules
        """
        call_in_cached_query.cache_clear()

    def _get_view_name(self, data: str) -> str:
        """
        Determine the view name based on the 'data' parameter.

        Parameters:
            data (str): One of "position" or "static"

        Returns:
            str: The name of the view to query.
        """
        mapping = {"position": "global_ais_dynamic", "static": "global_ais_static"}
        if data not in mapping:
            raise ValueError("Invalid data parameter. Must be 'position' or 'static'.")
        return mapping[data]

    def _get_global_df(
        self,
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        as_geodf: bool = True,
    ) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
        """
        Query the appropriate global view (all/dynamic/static) with optional filters.

        Parameters:
            report_type (str): Which dataset to query – position or static reports.
            mmsi (Optional[int]): Optional MMSI filter.
            start_date (Optional[str]): Start date ("YYYY-MM-DD").
            end_date (Optional[str]): End date ("YYYY-MM-DD").
            polygon_bounds (Optional[str]): WKT polygon for spatial filtering.
            as_geodf (bool): If True, returns a GeoDataFrame (assumes x and y exist).

        Returns:
            DataFrame or GeoDataFrame with the query result.
        """
        if self._filter:
            # only override if the user didn’t pass in an explicit value:
            mmsi = mmsi or self._filter.get("mmsi")
            start_date = start_date or self._filter.get("start_date")
            end_date = end_date or self._filter.get("end_date")
            polygon_bounds = polygon_bounds or self._filter.get("polygon_bounds")

        view_name = self._get_view_name(data=report_type)
        query = f"SELECT * FROM {view_name} WHERE 1=1"
        params = []

        # MMSI filter
        if mmsi is not None:
            query += " AND mmsi = ?"
            params.append(mmsi)

        # Date range filter:
        if start_date:
            try:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            except Exception as e:
                raise ValueError(
                    "Invalid start date format. Expected YYYY-MM-DD."
                ) from e
        if end_date:
            try:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)
            except Exception as e:
                raise ValueError("Invalid end date format. Expected YYYY-MM-DD.") from e

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

    def search(
        self,
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ):
        return self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds
        )

    # TODO(THALIA) Update to drop all rows for which x, y are null
    def get_geojson(
        self,
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> dict:
        """
        Return a GeoJSON representation of global AIS data.

        Parameters:
            report_type (str): One of "position" or "static"
            ... (other filters)

        Returns:
            dict: The GeoJSON representation.
        """
        gdf = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=True
        )
        if gdf.empty:
            logger.info(f"No AIS data available for MMSI {mmsi}")
            return {}
        if "datetime" in gdf.columns:
            gdf["datetime"] = gdf["datetime"].astype(str)
        return json.loads(gdf.to_json())

    def get_csv(
        self,
        file_path: str = "ais_data.csv",
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> str:
        df = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=False
        )
        if df.empty:
            return "No data available to export."
        df.to_csv(file_path, index=False)
        return f"CSV saved at {file_path}"

    def get_shapefile(
        self,
        file_path: str = "ais_shapefile",
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> str:
        """
        Exports global AIS data to a Shapefile.
        """
        gdf = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=True
        )
        if gdf.empty:
            return "No data available to export."
        gdf.to_file(file_path, driver="ESRI Shapefile")
        return f"Shapefile saved at {file_path}"

    def get_kml(
        self,
        file_path: str = "ais_data.kml",
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> str:
        """
        Exports global AIS data to a KML file.
        """
        gdf = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=True
        )
        if gdf.empty:
            return "No data available to export."
        gdf.to_file(file_path, driver="KML")
        return f"KML file saved at {file_path}"

    def get_excel(
        self,
        file_path: str = "ais_data.xlsx",
        report_type: str = "position",
        mmsi: Optional[int] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> str:
        """
        Exports global AIS data to an Excel file.
        """
        df = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=False
        )
        if df.empty:
            return "No data available to export."
        df.to_excel(file_path, index=False)
        return f"Excel file saved at {file_path}"

    def get_wkt(
        self,
        mmsi: Optional[int] = None,
        report_type: str = "position",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ):
        """
        Returns global AIS data in Well-Known Text (WKT) format.
        """
        gdf = self._get_global_df(
            report_type, mmsi, start_date, end_date, polygon_bounds, as_geodf=True
        )
        if gdf.empty:
            return "No data available to export."
        return gdf["geometry"].apply(lambda geom: geom.wkt).tolist()

    # Factory methods for message-type processing.
    def class_a(self):
        """Class A position and static messages (Types 1, 2, 3 and 5)."""
        return ClassAMessages(self._conn)

    def class_b(self):
        """Class B position and static messages (Types 18, 19 and 24)."""
        return ClassBMessages(self._conn)

    def long_range(self):
        """Long range broadcast messages (Type 27)."""
        return LongRangeMessages(self._conn)

    def addressed_binary(self):
        """Addressed Binary Message 6 (binary payload)"""
        return AddressedBinaryHandler(self._conn)

    def broadcast_text(self):
        """Broadcast Binary Message 8 (text payload)"""
        return BroadcastTextHandler(self._conn)

    def short_binary(self):
        """Single Slot Binary Message (25) & Multi Slot Binary Message (26)"""
        return ShortBinaryHandler(self._conn)

    def aton(self):
        """Aid to Navigation messages (Type 21)."""
        return AidToNavigationMessages(self._conn)

    def base_station(self):
        """Base Station Report (Type 4)."""
        return BaseStationMessages(self._conn)

    def safety(self):
        """Factory method for Safety Messages (Types 7, 13)"""
        return SafetyMessages(self._conn)

    def ack(self):
        """Factory method for Acknowledgement Messages (Types 12, 14)"""
        return AcknowledgementMessages(self._conn)

    def sar_aircraft(self):
        """Factory method for Search and Rescue Aircraft Position Messages (Type 9)"""
        return SarAircraftMessages(self._conn)

    def utc_date(self):
        """Factory method for UTC/Date Inquiry and Response Messages (Types 10, 11)"""
        return UtcDateMessages(self._conn)

    def system_management(self):
        """
        Factory method for System Management Messages (Types 15, 16, 17, 20, 22, 23)
        Includes:
          - Interrogation
          - Assignment Mode
          - DGNSS
          - Data Link Management
          - Channel Management
          - Group Assignment
        """
        return SystemManagementMessages(self._conn)

    def process(self, file_path: str) -> None:
        """
        Process a raw AIS data file and populate the database with Class A and Class B messages.
        This method will parsed each nmea sentence using libais and insert to its corresponding
        table in the database.

        Parameters:
            file_path (str):
                Path to your raw AIS data file

        Returns:
            None
        """
        ABMessagesProcessor(self._conn).process(file_path)


class BaseMessageProcessor:
    """
    Base class for processing AIS messages from files.
    Subclasses should implement _filter_message() and _prepare_insert to filter and insert
    messages of a specific type into their designated tables.
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        self._conn = conn
        self._filter: Optional[dict] = None

    """
    Private methods
    """

    def _process_chunk(self, chunk: list):
        import ais.stream

        batches: dict[str, list[tuple]] = {}

        for msg in ais.stream.decode(chunk):
            try:
                if self._filter_message(msg):
                    q, params = self._prepare_insert(msg)
                    batches.setdefault(q, []).append(params)
            except Exception as e:
                logger.error(f"Error processing message: {msg} — {e}")

        # one executemany per distinct SQL
        for q, param_list in batches.items():
            self._conn.executemany(q, param_list)

    """
    Abstract methods
    """

    @abstractmethod
    def _filter_message(self, msg: dict) -> bool:
        """Return True if the message matches this processor's criteria."""
        raise NotImplementedError("Subclasses must implement _filter_message.")

    @abstractmethod
    def _prepare_insert(self, msg: dict):
        """Insert the message into the appropriate table."""
        raise NotImplementedError("Subclasses must implement _prepare_insert.")

    # TODO(Thalia): to update view when new data is inserted
    def _update_global_views(self):
        try:
            for query in DATABASE_ALL_VIEWS_CREATION_QUERIES:
                self._conn.execute(query)
                print("Data Inserted and Views Successfully Updated")  # debug
        except Exception as e:
            logger.error(f"Error updating views: {e}")

    @abstractmethod
    def set_filter(self, filter_obj: Optional[dict]) -> None:
        """Set filter object for querying data from database"""
        raise NotImplementedError("Subclasses must implement set_filter.")

    """
    Public methods start here
    """

    # TODO(Thalia) Update so the process function checks for file extension
    # and call function to process raw or csv file types.
    def process(self, file_path: str, chunk_size: int = 5000):
        """
        Read the file in large chunks, batch‑insert,
        then rebuild any global/materialized views at the end.
        """
        try:
            for chunk in split_file_generator(file_path, chunk_size):
                try:
                    self._process_chunk(chunk)
                except Exception as e:
                    logger.error(f"Chunk failed: {e}")
                    raise
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            raise
        finally:
            try:
                self._update_global_views()
                AISDatabase.clear_cache()  # clear_cache to get latest inserted data in view
            except Exception as ve:
                logger.error(f"Failed to rebuild global views: {ve}")

    """
    Export Methods
    """

    # Note that because search() is abstract, the methods below will query from each
    # subclass' respective table.
    def get_geojson(
        self, mmsi: None, start_date=None, end_date=None, polygon_bounds=None
    ):
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
                styled=False,
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

    def get_csv(
        self,
        file_path="ais_data.csv",
        mmsi=None,
        start_date=None,
        end_date=None,
        polygon_bounds=None,
    ):
        """
        Exports AIS data to a CSV file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_csv(file_path, index=False)
        return f"CSV saved at {file_path}"

    def get_shapefile(
        self,
        file_path="ais_shapefile",
        mmsi=None,
        start_date=None,
        end_date=None,
        polygon_bounds=None,
    ):
        """
        Exports AIS data to a Shapefile.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_file(file_path, driver="ESRI Shapefile")
        return f"Shapefile saved at {file_path}"

    def get_kml(
        self,
        file_path="ais_data.kml",
        mmsi=None,
        start_date=None,
        end_date=None,
        polygon_bounds=None,
    ):
        """
        Exports AIS data to a KML file.
        """
        gdf = self.search(mmsi, start_date, end_date, polygon_bounds)
        if gdf.empty:
            return "No data available to export."

        gdf.to_file(file_path, driver="KML")
        return f"KML file saved at {file_path}"

    def get_excel(
        self,
        file_path="ais_data.xlsx",
        mmsi=None,
        start_date=None,
        end_date=None,
        polygon_bounds=None,
    ):
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


class BaseMessageProcessorPositionReport(BaseMessageProcessor):
    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)

    @abstractmethod
    def search(self, **kwargs) -> pd.DataFrame:
        """Query vessel dynamic data applying given arguments"""
        raise NotImplementedError("Subclasses must implement search.")

    @abstractmethod
    def static_info(self, **kwargs) -> pd.DataFrame:
        """Query vessel static data applying given arguments"""
        raise NotImplementedError("Subclasses must implement static_info.")


class ABMessagesProcessor(BaseMessageProcessor):
    """
    Processor that handles both Class A (1, 2, 3, 5) and Class B (18, 19, 24)
    AIS messages in one pass.

    This processor reads a raw AIS file (NMEA stream, CSV, etc.), decodes
    every chunk, and for each message:

      1. If `msg.id` is in {1,2,3,5}, delegates to
       `ClassAMessages._prepare_insert`.
      2. If `msg.id` is in {18,19,24}, delegates to
       `ClassBMessages._prepare_insert`.
      3. Ignores all other message types.

    After all chunks have been processed, it calls `_update_global_views()`
    once so that any “global” views reflect the newly inserted rows.
    """

    def __init__(self, conn):
        super().__init__(conn)
        self._proc_a = ClassAMessages(conn)
        self._proc_b = ClassBMessages(conn)

    def _filter_message(self, msg):
        return msg.get("id") in {1, 2, 3, 5, 18, 19, 24}

    def _prepare_insert(self, msg):
        if msg["id"] in {1, 2, 3, 5}:
            return self._proc_a._prepare_insert(msg)
        else:
            return self._proc_b._prepare_insert(msg)

    def set_filter(self, f):
        pass


class ClassAMessages(BaseMessageProcessorPositionReport):
    """
    Processes Class A messages (Types 1, 2, 3 and static Type 5).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        # Process only if the message id is one of the Class A types.
        return msg.get("id") in {1, 2, 3, 5}

    def _prepare_insert(self, msg: dict):
        # Use .get() to provide default values for missing attributes
        # Note in _process_chunk we are already filtering per messages of type A
        if msg.get("id") == 5:
            query = """
                    INSERT INTO ais_msg_5 (
                        id, repeat_indicator, mmsi, ais_version, imo, call_sign, ship_name,
                        type_of_ship_and_cargo, to_bow, to_stern, to_port, to_starboard,
                        position_fixing_device, eta, max_present_static_draught, destination, dte
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """
            params = (
                msg.get("id"),
                msg.get("repeat_indicator"),
                msg.get("mmsi"),
                msg.get("ais_version_indicator"),
                msg.get("imo"),
                msg.get("call_sign"),
                msg.get("ship_name"),
                msg.get("type_of_ship_and_cargo"),
                msg.get("dimension_to_bow"),
                msg.get("dimension_to_stern"),
                msg.get("dimension_to_port"),
                msg.get("dimension_to_starboard"),
                msg.get("position_fixing_device"),
                msg.get("eta"),
                msg.get("max_present_static_draught"),
                msg.get("destination"),
                msg.get("dte"),
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
                msg.get("id"),
                msg.get("repeat_indicator"),
                msg.get("mmsi"),
                msg.get("nav_status"),
                msg.get("rot_over_range"),
                msg.get("rot"),
                msg.get("sog"),
                msg.get("position_accuracy"),
                msg.get("x"),
                msg.get("y"),
                msg.get("cog"),
                msg.get("true_heading"),
                msg.get("timestamp"),
                msg.get("special_manoeuvre"),
                msg.get("spare"),
                msg.get("raim"),
                msg.get("sync_state"),
                msg.get("slot_timeout"),
                msg.get("slot_number", None),  # Default to None if not present
                json.dumps(msg.get("tagblock_group", {})),
                # Default to an empty JSON object
                msg.get("tagblock_line_count"),
                msg.get("tagblock_station"),
                msg.get("tagblock_timestamp"),
            )
        return (query, params)

    def set_filter(self, filter_obj: Optional[dict]) -> None:
        if filter_obj is not None:
            if not isinstance(filter_obj, dict):
                raise TypeError("Filter object must be a dictionary.")
            if not set(filter_obj.keys()).issubset(ALLOWED_FILTER_KEYS_CLASS_A):
                raise TypeError(
                    "Filter object contains invalid keys."
                )  # TODO(Thalia): add link to documentation in error message
        self._filter = filter_obj

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        min_velocity: Optional[float] = None,
        max_velocity: Optional[float] = None,
        direction: Optional[str] = None,
        min_turn_rate: Optional[float] = None,
        max_turn_rate: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS data with optional filters.

        Parameters:
        - mmsi (int | list[int], optional): MMSI number(s) to filter.
        - conn (duckdb.DuckDBPyConnection, optional): DuckDB connection
         (defaults to self._conn).
        - start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        - end_date (str, optional): End date in 'YYYY-MM-DD' format.
        - polygon_bounds (str, optional): WKT polygon for spatial filtering.
        - min_velocity (float, optional): Minimum speed over ground (sog).
        - max_velocity (float, optional): Maximum speed over ground (sog).
        - direction (str, optional): Cardinal direction ("N", "E", "S", or "W")
         to filter by course over ground (cog).
        - min_turn_rate (float, optional): Minimum rate of turn (rot).
        - max_turn_rate (float, optional): Maximum rate of turn (rot).

        Returns:
        - gpd.GeoDataFrame: Filtered AIS data.
        """
        # TODO(Thalia) I wonder if this is really necessary. May refactor later
        #  .... Get rid of it but ask Kurt first
        if not conn:
            conn = self._conn

        try:
            # Base query
            query = "SELECT * FROM ais_msg_123 WHERE 1=1"
            params = []

            # Apply stored filter if set (stored filter values are used
            # unless explicitly overridden)
            if self._filter:
                mmsi = mmsi or self._filter.get("mmsi")
                start_date = start_date or self._filter.get("start_date")
                end_date = end_date or self._filter.get("end_date")
                polygon_bounds = polygon_bounds or self._filter.get("polygon_bounds")
                min_velocity = min_velocity or self._filter.get("min_velocity")
                max_velocity = max_velocity or self._filter.get("max_velocity")
                direction = direction or self._filter.get("direction")
                min_turn_rate = min_turn_rate or self._filter.get("min_turn_rate")
                max_turn_rate = max_turn_rate or self._filter.get("max_turn_rate")

            # MMSI filtering
            if mmsi:
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
                        *map(int, start_date.split("-"))
                    )
                    query += " AND tagblock_timestamp >= ?"
                    params.append(start_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid start date format. Expected YYYY-MM-DD."
                    ) from e
            if end_date:
                try:
                    end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                    query += " AND tagblock_timestamp <= ?"
                    params.append(end_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid end date format. Expected YYYY-MM-DD."
                    ) from e

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
                    raise ValueError("Direction must be one of 'N', 'E', 'S', 'W'.")

            # Log query for debugging
            logger.info(f"Executing query: {query} with params: {params}")

            # Execute query
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(
                    columns=["geometry"]
                )  # Return empty GeoDataFrame

            # Build GeoDataFrame
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            return gdf

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on failure

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
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    query += f" WHERE mmsi IN ({', '.join('?' * len(mmsi))})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Execute query
            df = cached_query(conn, query, params, True)

            if df.empty:
                return {"No static MMSI info found."}
            return df

        except Exception as e:
            logger.error(f"Error retrieving vessel info: {e}")
            return {"mmsi": mmsi, "error": str(e)}


class ClassBMessages(BaseMessageProcessorPositionReport):
    """
    Processes Class B messages (Types 18, 19 and static Type 24).
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        # Process only if the message id is one of the Class B types.
        return msg.get("id") in {18, 19, 24}

    def _prepare_insert(self, msg: dict):
        if msg.get("id") == 24:
            query = """
                INSERT INTO ais_msg_24 (
                    id, repeat_indicator, mmsi, part_num, name, type_and_cargo,
                    vendor_id, callsign, dim_a, dim_b, dim_c, dim_d, spare,
                    tagblock_group, tagblock_line_count, tagblock_station, tagblock_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                msg.get("id"),
                msg.get("repeat_indicator"),
                msg.get("mmsi"),
                msg.get("part_num"),
                msg.get("name"),
                msg.get("type_and_cargo"),
                msg.get("vendor_id"),
                msg.get("call_sign"),
                msg.get("dimension_to_bow"),
                msg.get("dimension_to_stern"),
                msg.get("dimension_to_port"),
                msg.get("dimension_to_starboard"),
                msg.get("spare"),
                json.dumps(msg.get("tagblock_group", {})),
                msg.get("tagblock_line_count"),
                msg.get("tagblock_station"),
                msg.get("tagblock_timestamp"),
            )
        else:
            # For dynamic messages (Types 18 and 19)
            query = """
                INSERT INTO ais_msg_18_19 (
                    id, repeat_indicator, mmsi, spare, sog, position_accuracy,
                    x, y, cog, true_heading, timestamp, spare2, unit_flag, display_flag,
                    dsc_flag, band_flag, m22_flag, mode_flag, raim, commstate_flag,
                    commstate_cs_fill, tagblock_group, tagblock_line_count, tagblock_station,
                    tagblock_timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,\
                 ?, ?, ?, ?, ?, ?, ?);
            """
            params = (
                msg.get("id"),
                msg.get("repeat_indicator"),
                msg.get("mmsi"),
                msg.get("spare"),
                msg.get("sog"),
                msg.get("position_accuracy"),
                msg.get("x"),
                msg.get("y"),
                msg.get("cog"),
                msg.get("true_heading"),
                msg.get("timestamp"),
                msg.get("spare2"),
                msg.get("unit_flag"),
                msg.get("display_flag"),
                msg.get("dsc_flag"),
                msg.get("band_flag"),
                msg.get("m22_flag"),
                msg.get("mode_flag"),
                msg.get("raim"),
                msg.get("commstate_flag"),
                msg.get("commstate_cs_fill"),
                json.dumps(msg.get("tagblock_group", {})),
                msg.get("tagblock_line_count"),
                msg.get("tagblock_station"),
                msg.get("tagblock_timestamp"),
            )
        return (query, params)

    # TODO(Thalia): write function implementation and filter object for messages of class B
    def set_filter(self, filter_obj: Optional[dict] = None) -> None:
        if filter_obj is not None:
            if not isinstance(filter_obj, dict):
                raise TypeError("Filter object must be a dictionary.")
            if not set(filter_obj.keys()).issubset(ALLOWED_FILTER_KEYS_CLASS_B):
                raise TypeError(
                    "Filter object contains invalid keys."
                )  # TODO(Thalia): add link to documentation in error message
        self._filter = filter_obj

    # TODO(Update this search function and global search function)
    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        min_velocity: Optional[float] = None,
        max_velocity: Optional[float] = None,
        direction: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS data with optional filters.

        Parameters:
        - mmsi (int | list[int], optional): MMSI number(s) to filter.
        - conn (duckdb.DuckDBPyConnection, optional): DuckDB connection
        (defaults to self._conn).
        - start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        - end_date (str, optional): End date in 'YYYY-MM-DD' format.
        - polygon_bounds (str, optional): WKT polygon for spatial filtering.
        - min_velocity (float, optional): Minimum speed over ground (sog).
        - max_velocity (float, optional): Maximum speed over ground (sog).
        - direction (str, optional): Cardinal direction ("N", "E", "S", or "W")
         to filter by course over ground (cog).

        Returns:
        - gpd.GeoDataFrame: Filtered AIS data.
        """
        if not conn:
            conn = self._conn

        try:
            # Base query
            query = "SELECT * FROM ais_msg_18_19 WHERE 1=1"
            params = []

            # Apply stored filter if set (stored filter values are used unless
            # explicitly overridden)
            if self._filter:
                mmsi = mmsi or self._filter.get("mmsi")
                start_date = start_date or self._filter.get("start_date")
                end_date = end_date or self._filter.get("end_date")
                polygon_bounds = polygon_bounds or self._filter.get("polygon_bounds")
                min_velocity = min_velocity or self._filter.get("min_velocity")
                max_velocity = max_velocity or self._filter.get("max_velocity")
                direction = direction or self._filter.get("direction")

            # MMSI filtering
            if mmsi:
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
                        *map(int, start_date.split("-"))
                    )
                    query += " AND tagblock_timestamp >= ?"
                    params.append(start_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid start date format. Expected YYYY-MM-DD."
                    ) from e
            if end_date:
                try:
                    end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                    query += " AND tagblock_timestamp <= ?"
                    params.append(end_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid end date format. Expected YYYY-MM-DD."
                    ) from e

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
                    raise ValueError("Direction must be one of 'N', 'E', 'S', 'W'.")

            # Log query for debugging
            logger.info(f"Executing query: {query} with params: {params}")

            # Execute query
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(
                    columns=["geometry"]
                )  # Return empty GeoDataFrame

            # Build GeoDataFrame
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            return gdf

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on failure

    def static_info(
        self, mmsi: Optional[Union[int, List[int]]] = None, **kwargs
    ) -> pd.DataFrame:
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


class OtherMessages(BaseMessageProcessor):
    """
    Processes all remaining AIS messages
    """

    def __init__(self, conn: duckdb.DuckDBPyConnection):
        super().__init__(conn)


class LongRangeMessages(BaseMessageProcessorPositionReport):
    """
    Handles long-range AIS messages (Message Type 27).
    Applies to both Class A and Class B SO equipped vessels.
    vessel_type attribute is programmatically determined based on mmsi.
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") == 27

    def _prepare_insert(self, msg: dict):
        query = """
            INSERT INTO ais_msg_27 (
                id, repeat_indicator, mmsi, position_accuracy, raim, nav_status,
                x, y, sog, cog, gnss, spare, vessel_type,
                tagblock_group, tagblock_line_count, tagblock_station, tagblock_timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        params = (
            msg.get("id"),
            msg.get("repeat_indicator"),
            msg.get("mmsi"),
            msg.get("position_accuracy"),
            msg.get("raim"),
            msg.get("nav_status"),
            msg.get("x"),
            msg.get("y"),
            msg.get("sog"),
            msg.get("cog"),
            msg.get("gnss"),
            msg.get("spare"),
            guess_vessel_type(msg.get("mmsi")),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        min_velocity: Optional[float] = None,
        max_velocity: Optional[float] = None,
        direction: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS data with optional filters.

        Parameters:
        - mmsi (int | list[int], optional): MMSI number(s) to filter.
        - conn (duckdb.DuckDBPyConnection, optional): DuckDB connection
        (defaults to self._conn).
        - start_date (str, optional): Start date in 'YYYY-MM-DD' format.
        - end_date (str, optional): End date in 'YYYY-MM-DD' format.
        - polygon_bounds (str, optional): WKT polygon for spatial filtering.
        - min_velocity (float, optional): Minimum speed over ground (sog).
        - max_velocity (float, optional): Maximum speed over ground (sog).
        - direction (str, optional): Cardinal direction ("N", "E", "S", or "W")
         to filter by course over ground (cog).

        Returns:
        - gpd.GeoDataFrame: Filtered AIS data.
        """
        if not conn:
            conn = self._conn

        try:
            # Base query
            query = "SELECT * FROM ais_msg_27 WHERE 1=1"
            params = []

            # Apply stored filter if set (stored filter values are used unless
            # explicitly overridden)
            if self._filter:
                mmsi = mmsi or self._filter.get("mmsi")
                start_date = start_date or self._filter.get("start_date")
                end_date = end_date or self._filter.get("end_date")
                polygon_bounds = polygon_bounds or self._filter.get("polygon_bounds")
                min_velocity = min_velocity or self._filter.get("min_velocity")
                max_velocity = max_velocity or self._filter.get("max_velocity")
                direction = direction or self._filter.get("direction")

            # MMSI filtering
            if mmsi:
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
                        *map(int, start_date.split("-"))
                    )
                    query += " AND tagblock_timestamp >= ?"
                    params.append(start_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid start date format. Expected YYYY-MM-DD."
                    ) from e
            if end_date:
                try:
                    end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                    query += " AND tagblock_timestamp <= ?"
                    params.append(end_ts)
                except Exception as e:
                    raise ValueError(
                        "Invalid end date format. Expected YYYY-MM-DD."
                    ) from e

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
                    raise ValueError("Direction must be one of 'N', 'E', 'S', 'W'.")

            # Log query for debugging
            logger.info(f"Executing query: {query} with params: {params}")

            # Execute query
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(
                    columns=["geometry"]
                )  # Return empty GeoDataFrame

            # Build GeoDataFrame
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
            return gdf

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()  # Return empty GeoDataFrame on failure

    def static_info(self, **kwargs) -> pd.DataFrame:
        logger.info("No static info provided by long range")
        return pd.DataFrame()  # Not applicable to Message 27


class AddressedBinaryHandler(BaseMessageProcessor):
    """Handles Message 6: Addressed Binary Message"""

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") == 6

    def _prepare_insert(self, msg: dict):
        """
        Insert a single message of type 6 into the ais_msg_6 table.
        """
        core_cols = {
            "id": msg.get("id"),  # 6
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "spare": msg.get("spare"),
            "spare2": msg.get("spare2"),
            "dac": msg.get("dac"),
            "fid": msg.get("fi") or msg.get("fid"),
            "x": msg.get("x"),
            "y": msg.get("y"),
        }
        # Exclude the columns we just used from leftover
        used_keys = set(core_cols.keys()) | {"fi", "fid"}
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
            INSERT INTO ais_msg_6 (
                id, repeat_indicator, mmsi, spare, spare2,
                dac, fid, x, y,
                application_data,
                tagblock_group, tagblock_line_count, tagblock_station, tagblock_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["spare"],
            core_cols["spare2"],
            core_cols["dac"],
            core_cols["fid"],
            core_cols["x"],
            core_cols["y"],
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        dac: Optional[int] = None,
        fid: Optional[int] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS AtoN data (Message 21) with optional filters.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_6 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            # DAC & FID filters
            if dac is not None:
                query += " AND dac = ?"
                params.append(dac)

            if fid is not None:
                query += " AND fid = ?"
                params.append(fid)

            logger.info(f"Executing AtoN query: {query} with params: {params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class BroadcastTextHandler(BaseMessageProcessor):
    """Handles Message 8: Broadcast Binary Message (may carry human-readable payloads)"""

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") == 8

    def _prepare_insert(self, msg: dict):
        """
        Insert a single message of type 8 into the ais_msg_8 table.
        """
        core_cols = {
            "id": msg.get("id"),  # 8
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "dac": msg.get("dac"),
            "fid": msg.get("fi") or msg.get("fid"),
            "x": msg.get("x"),
            "y": msg.get("y"),
        }
        used_keys = set(core_cols.keys()) | {"fi", "fid"}
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
            INSERT INTO ais_msg_8 (
                id, repeat_indicator, mmsi, dac, fid, x, y,
                application_data,
                tagblock_group, tagblock_line_count, tagblock_station, tagblock_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["dac"],
            core_cols["fid"],
            core_cols["x"],
            core_cols["y"],
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        dac: Optional[int] = None,
        fid: Optional[int] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS AtoN data (Message 21) with optional filters.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_8 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            # DAC & FID filters
            if dac is not None:
                query += " AND dac = ?"
                params.append(dac)

            if fid is not None:
                query += " AND fid = ?"
                params.append(fid)

            logger.info(f"Executing AtoN query: {query} with params: {params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class ShortBinaryHandler(BaseMessageProcessor):
    """Handles Message 25 and 26: Slot Binary Messages"""

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") in {25, 26}

    def _prepare_insert(self, msg: dict):
        core_cols = {
            "id": msg.get("id"),
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "dest_mmsi": msg.get("dest_mmsi"),
            "sync_state": msg.get("sync_state"),
            "x": msg.get("x"),
            "y": msg.get("y"),
        }
        used_keys = set(core_cols.keys())
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
        INSERT INTO ais_msg_25_26 (
            id, repeat_indicator, mmsi, dest_mmsi, sync_state,
            x, y, application_data,
            tagblock_group, tagblock_line_count, tagblock_station,
            tagblock_timestamp
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = tuple(core_cols.get(k) for k in core_cols) + (
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS AtoN data (Message 21) with optional filters.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_25_26 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            logger.info(f"Executing AtoN query: {query} with params: {params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


# todo(Thalia): maybe implement wrapper asm


class AidToNavigationMessages(BaseMessageProcessor):
    """
    Handles AIS Aid to Navigation reports (Message Type 21).
    These represent fixed or virtual navigation aids such as buoys, beacons, etc.
    Identified by MMSIs starting with 993.
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") == 21

    def _prepare_insert(self, msg: dict):
        """
        Insert a parsed AIS Message 21 (AtoN) into the ais_msg_21 table.
        Known columns are stored in top-level fields, everything else goes into
        application_data.
        """

        core_cols = {
            "id": msg.get("id"),
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "spare": msg.get("spare"),
            "aton_type": msg.get("aton_type"),
            "name": msg.get("name"),
            "position_accuracy": msg.get("position_accuracy"),
            "x": msg.get("x"),
            "y": msg.get("y"),
            "dim_a": msg.get("dim_a"),
            "dim_b": msg.get("dim_b"),
            "dim_c": msg.get("dim_c"),
            "dim_d": msg.get("dim_d"),
            "fix_type": msg.get("fix_type"),
            "timestamp": msg.get("timestamp"),
            "off_pos": msg.get("off_pos"),
            "aton_status": msg.get("aton_status"),
            "raim": msg.get("raim"),
            "virtual_aton": msg.get("virtual_aton"),
            "assigned_mode": msg.get("assigned_mode"),
        }

        # Build leftover dict
        used_keys = set(core_cols.keys()) | {
            "tagblock_group",
            "tagblock_line_count",
            "tagblock_station",
            "tagblock_timestamp",
        }
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
            INSERT
            INTO
            ais_msg_21(
                id, repeat_indicator, mmsi, spare, aton_type, name,
                position_accuracy,
                x, y, dim_a, dim_b, dim_c, dim_d, fix_type, timestamp, off_pos,
                aton_status, raim, virtual_aton, assigned_mode,
                application_data,
                tagblock_group, tagblock_line_count, tagblock_station,
                tagblock_timestamp
            )
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["spare"],
            core_cols["aton_type"],
            core_cols["name"],
            core_cols["position_accuracy"],
            core_cols["x"],
            core_cols["y"],
            core_cols["dim_a"],
            core_cols["dim_b"],
            core_cols["dim_c"],
            core_cols["dim_d"],
            core_cols["fix_type"],
            core_cols["timestamp"],
            core_cols["off_pos"],
            core_cols["aton_status"],
            core_cols["raim"],
            core_cols["virtual_aton"],
            core_cols["assigned_mode"],
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    # TODO(Thalia): Update
    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS AtoN data (Message 21) with optional filters.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_21 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            logger.info(f"Executing AtoN query: {query} with params: {params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class BaseStationMessages(BaseMessageProcessor):
    """
    Handles AIS Base Station Position Reports (Message Type 4).
    These are shore-based stations providing time synchronization and position.
    MMSIs typically begin with 00MIDxxxxx.
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") == 4

    def _prepare_insert(self, msg: dict):
        core_cols = {
            "id": msg.get("id"),
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "year": msg.get("year"),
            "month": msg.get("month"),
            "day": msg.get("day"),
            "hour": msg.get("hour"),
            "minute": msg.get("minute"),
            "second": msg.get("second"),
            "position_accuracy": msg.get("position_accuracy"),
            "x": msg.get("x"),
            "y": msg.get("y"),
            "fix_type": msg.get("fix_type"),
            "transmission_ctl": msg.get("transmission_ctl"),
            "spare": msg.get("spare"),
            "raim": msg.get("raim"),
            "sync_state": msg.get("sync_state"),
            "slot_timeout": msg.get("slot_timeout"),
            "slot_offset": msg.get("slot_offset"),
            "slot_number": msg.get("slot_number"),
            "received_stations": msg.get("received_stations"),
        }

        # Leftover dict
        used_keys = set(core_cols.keys())
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
        INSERT
        INTO
        ais_msg_4(
            id, repeat_indicator, mmsi,
            year, month, day, hour, minute, second,
            position_accuracy, x, y, fix_type, transmission_ctl, spare, raim,
            sync_state, slot_timeout, slot_offset, slot_number,
            received_stations,
            application_data,
            tagblock_group, tagblock_line_count, tagblock_station,
            tagblock_timestamp
        )
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["year"],
            core_cols["month"],
            core_cols["day"],
            core_cols["hour"],
            core_cols["minute"],
            core_cols["second"],
            core_cols["position_accuracy"],
            core_cols["x"],
            core_cols["y"],
            core_cols["fix_type"],
            core_cols["transmission_ctl"],
            core_cols["spare"],
            core_cols["raim"],
            core_cols["sync_state"],
            core_cols["slot_timeout"],
            core_cols["slot_offset"],
            core_cols["slot_number"],
            core_cols["received_stations"],
            json.dumps(leftover),  # store everything else in JSON
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    # TODO(Thalia) Update
    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search AIS Base Station data (Message 4) with optional filters.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_4 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an integer or a list of integers.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)
            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            logger.info(f"Executing Base Station query: {query} with params: {params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class AcknowledgementMessages(BaseMessageProcessor):
    """
    Processes Acknowledgement Messages (Types 7 and 13).
    Inserts messages into the ais_msg_7_13 table.
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") in {7, 13}

    def _prepare_insert(self, msg: dict):
        core_cols = {
            "id": msg.get("id"),
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "ack_count": msg.get("ack_count"),
        }
        # Build leftover dictionary from any keys not in core_cols.
        used_keys = set(core_cols.keys())
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
        INSERT INTO ais_msg_7_13 (
            id, repeat_indicator, mmsi, ack_count,
            application_data, tagblock_group, tagblock_line_count,
            tagblock_station, tagblock_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["ack_count"],
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(self, mmsi=None, conn=None, **kwargs):
        """Search for Acknowledgement messages (Types 7 & 13) in ais_msg_7_13."""
        if not conn:
            conn = self._conn
        query = "SELECT * FROM ais_msg_7_13 WHERE 1=1"
        params = []
        if mmsi:
            query += " AND mmsi = ?"
            params.append(mmsi)
        df = cached_query(conn, query, params, True)
        return df


class SafetyMessages(BaseMessageProcessor):
    """
    Processes Safety Messages (Types 12 and 14).
    Inserts messages into the ais_msg_12_14 table.
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") in {12, 14}

    def _prepare_insert(self, msg: dict):
        core_cols = {
            "id": msg.get("id"),
            "repeat_indicator": msg.get("repeat_indicator"),
            "mmsi": msg.get("mmsi"),
            "message_text": msg.get("message_text"),
            # For message 12, "addressed" is True; for 14 it is False.
            "addressed": (msg.get("id") == 12),
        }
        used_keys = set(core_cols.keys())
        leftover = {k: v for k, v in msg.items() if k not in used_keys}

        query = """
        INSERT INTO ais_msg_12_14 (
            id, repeat_indicator, mmsi, message_text, addressed,
            application_data, tagblock_group, tagblock_line_count,
            tagblock_station, tagblock_timestamp
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            core_cols["id"],
            core_cols["repeat_indicator"],
            core_cols["mmsi"],
            core_cols["message_text"],
            core_cols["addressed"],
            json.dumps(leftover),
            json.dumps(msg.get("tagblock_group", {})),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )
        return (query, params)

    def search(self, mmsi=None, conn=None, **kwargs):
        """Search for Safety messages (Types 12 & 14) in ais_msg_12_14."""
        if not conn:
            conn = self._conn
        query = "SELECT * FROM ais_msg_12_14 WHERE 1=1"
        params = []
        if mmsi:
            query += " AND mmsi = ?"
            params.append(mmsi)
        df = cached_query(conn, query, params, True)
        return df


class SarAircraftMessages(BaseMessageProcessor):
    """
    Message 9 only (SAR Aircraft Position Reports).
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        # Only process if it's actually message type 9.
        return msg.get("id") == 9

    def _prepare_insert(self, msg: dict):
        """
        Inserts a Message 9 (SAR aircraft position report) into the `ais_msg_9` table.
        """
        query = """
            INSERT INTO ais_msg_9 (
                id,
                repeat_indicator,
                mmsi,
                altitude,
                sog,
                position_accuracy,
                x,
                y,
                cog,
                timestamp,
                raim,
                spare,
                application_data,
                tagblock_group,
                tagblock_line_count,
                tagblock_station,
                tagblock_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Convert to JSON where needed (if your schema expects JSON text).
        # Adjust field names depending on how they appear in `msg`.
        params = (
            msg.get("id"),
            msg.get("repeat_indicator"),
            msg.get("mmsi"),
            msg.get("altitude"),
            msg.get("sog"),
            msg.get("position_accuracy"),
            msg.get("x"),
            msg.get("y"),
            msg.get("cog"),
            msg.get("timestamp"),
            msg.get("raim"),
            msg.get("spare"),
            json.dumps(msg.get("application_data")),
            json.dumps(msg.get("tagblock_group")),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )

        return (query, params)

    def search(
        self,
        mmsi: Optional[Union[int, List[int]]] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        min_altitude: Optional[int] = None,
        max_altitude: Optional[int] = None,
        min_sog: Optional[float] = None,
        max_sog: Optional[float] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search SAR Aircraft data (Message 9) with optional filters.

        Parameters:
            mmsi (int | list[int], optional): Filter by one or multiple MMSIs.
            conn (duckdb.DuckDBPyConnection, optional): DB connection (defaults
             to self._conn).
            start_date (str, optional): Start date in 'YYYY-MM-DD' format.
            end_date (str, optional): End date in 'YYYY-MM-DD' format.
            polygon_bounds (str, optional): WKT polygon to filter by location.
            min_altitude (int, optional): Minimum altitude to filter.
            max_altitude (int, optional): Maximum altitude to filter.
            min_sog (float, optional): Minimum speed over ground (knots).
            max_sog (float, optional): Maximum speed over ground (knots).

        Returns:
            gpd.GeoDataFrame: Results with geometry built from x,y.
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_9 WHERE 1=1"
            params = []

            # MMSI filter
            if mmsi is not None:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(x, int) for x in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("mmsi must be an int or list of ints.")

            # Date range filter
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)

            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon bounds filter
            if polygon_bounds:
                # param for WKT string
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            # Altitude filters
            if min_altitude is not None:
                query += " AND altitude >= ?"
                params.append(min_altitude)
            if max_altitude is not None:
                query += " AND altitude <= ?"
                params.append(max_altitude)

            # SOG filters
            if min_sog is not None:
                query += " AND sog >= ?"
                params.append(min_sog)
            if max_sog is not None:
                query += " AND sog <= ?"
                params.append(max_sog)

            logger.info(
                f"Executing SAR Aircraft (Message 9) query: {query} with params {params}"
            )
            df = cached_query(conn, query, params, return_df=True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            # Convert x,y to geometry
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class UtcDateMessages(BaseMessageProcessor):
    """
    For messages 10 (UTC/Date inquiry) and 11 (UTC/Date response).
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") in {10, 11}

    def _prepare_insert(self, msg: dict):
        query = """
            INSERT INTO ais_msg_10_11 (
                id,
                repeat_indicator,
                mmsi,
                spare,
                dest_mmsi,
                spare2,
                year,
                month,
                day,
                hour,
                minute,
                second,
                position_accuracy,
                x,
                y,
                fix_type,
                transmission_ctl,
                raim,
                sync_state,
                slot_timeout,
                slot_offset,
                tagblock_group,
                tagblock_line_count,
                tagblock_station,
                tagblock_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        # Because 10 and 11 use different fields, we .get() each with a default of None.
        params = (
            msg.get("id"),
            msg.get("repeat_indicator"),
            msg.get("mmsi"),
            msg.get("spare"),
            msg.get("dest_mmsi"),
            msg.get("spare2"),
            msg.get("year"),
            msg.get("month"),
            msg.get("day"),
            msg.get("hour"),
            msg.get("minute"),
            msg.get("second"),
            msg.get("position_accuracy"),
            msg.get("x"),
            msg.get("y"),
            msg.get("fix_type"),
            msg.get("transmission_ctl"),
            msg.get("raim"),
            msg.get("sync_state"),
            msg.get("slot_timeout"),
            msg.get("slot_offset"),
            json.dumps(msg.get("tagblock_group")),  # JSON
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )

        return (query, params)

    def search(
        self,
        msg_id: Optional[int] = None,  # 10 or 11
        mmsi: Optional[Union[int, List[int]]] = None,
        dest_mmsi: Optional[Union[int, List[int]]] = None,  # for msg 10
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        polygon_bounds: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> gpd.GeoDataFrame:
        """
        Search UTC/Date messages (Message 10 or 11).

        Parameters:
          - msg_id: If set, filter by message type (10 or 11).
          - mmsi:  Filter by MMSI (single int or list of ints).
          - dest_mmsi: Filter by destination MMSI (applies to message 10).
          - start_date, end_date: date range in YYYY-MM-DD format (filters tagblock_timestamp).
          - polygon_bounds: WKT polygon for location-based filtering if x,y are present.
          - conn: optional DuckDB connection override

        Returns:
          A GeoDataFrame with columns x,y -> geometry
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_10_11 WHERE 1=1"
            params = []

            # Filter by message type 10 or 11, if desired
            if msg_id is not None:
                query += " AND id = ?"
                params.append(msg_id)  # must be 10 or 11

            # MMSI filter
            if mmsi is not None:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(x, int) for x in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("mmsi must be an int or list of ints.")

            # dest_mmsi filter (usually for message 10)
            if dest_mmsi is not None:
                if isinstance(dest_mmsi, int):
                    query += " AND dest_mmsi = ?"
                    params.append(dest_mmsi)
                elif isinstance(dest_mmsi, list) and all(
                    isinstance(x, int) for x in dest_mmsi
                ):
                    placeholders = ", ".join(["?"] * len(dest_mmsi))
                    query += f" AND dest_mmsi IN ({placeholders})"
                    params.extend(dest_mmsi)
                else:
                    raise ValueError("dest_mmsi must be an int or list of ints.")

            # Date range filters
            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)

            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            # Polygon filter if you store x,y
            if polygon_bounds:
                query += " AND ST_Within(ST_Point(x, y), ST_GeomFromText(?))"
                params.append(polygon_bounds)

            logger.info(
                f"Executing UTC/Date (10/11) query: {query} with params: {params}"
            )
            df = cached_query(conn, query, params, True)
            if df.empty:
                return gpd.GeoDataFrame(columns=["geometry"])

            # Build geometry from x,y
            df["geometry"] = gpd.points_from_xy(df["x"], df["y"])
            return gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        return gpd.GeoDataFrame()


class SystemManagementMessages(BaseMessageProcessor):
    """
    For 15,16,17,20,22,23
    """

    def __init__(self, conn):
        super().__init__(conn)

    def _filter_message(self, msg: dict) -> bool:
        return msg.get("id") in {15, 16, 17, 20, 22, 23}

    def _prepare_insert(self, msg: dict):
        """
        Insert any of messages 15, 16, 17, 20, 22, or 23 into our combined table.
        Unused fields for a given message type can be left as None.
        """
        query = """
            INSERT INTO ais_msg_15_16_17_20_22_23 (
                id,
                repeat_indicator,
                mmsi,

                mmsi_1,
                msg_1_1,
                slot_offset_1_1,
                dest_msg_1_2,
                slot_offset_1_2,
                mmsi_2,
                msg_2,
                slot_offset_2,

                dest_mmsi_a,
                offset_a,
                inc_a,
                dest_mmsi_b,
                offset_b,
                inc_b,

                x,
                y,

                reservations,

                chan_a,
                chan_b,
                txrx_mode,
                power_low,
                x1,
                y1,
                x2,
                y2,
                chan_a_bandwidth,
                chan_b_bandwidth,
                zone_size,

                x1_23,
                y1_23,
                x2_23,
                y2_23,
                station_type,
                type_and_cargo,
                interval_raw,
                quiet,

                spare,
                spare2,
                spare3,
                spare4,

                tagblock_group,
                tagblock_line_count,
                tagblock_station,
                tagblock_timestamp
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
             ?, ?, ?, ?, ?)
        """

        # Extract and default any fields not present
        params = (
            msg.get("id"),
            msg.get("repeat_indicator"),
            msg.get("mmsi"),
            # Message 15 fields
            msg.get("mmsi_1"),
            msg.get("msg_1_1"),
            msg.get("slot_offset_1_1"),
            msg.get("dest_msg_1_2"),
            msg.get("slot_offset_1_2"),
            msg.get("mmsi_2"),
            msg.get("msg_2"),
            msg.get("slot_offset_2"),
            # Message 16 fields
            msg.get("dest_mmsi_a"),
            msg.get("offset_a"),
            msg.get("inc_a"),
            msg.get("dest_mmsi_b"),
            msg.get("offset_b"),
            msg.get("inc_b"),
            # Message 17 fields
            msg.get("x"),
            msg.get("y"),
            # Message 20 fields (array of reservations)
            json.dumps(msg.get("reservations")) if msg.get("reservations") else None,
            # Message 22 fields
            msg.get("chan_a"),
            msg.get("chan_b"),
            msg.get("txrx_mode"),
            msg.get("power_low"),
            msg.get("x1"),
            msg.get("y1"),
            msg.get("x2"),
            msg.get("y2"),
            msg.get("chan_a_bandwidth"),
            msg.get("chan_b_bandwidth"),
            msg.get("zone_size"),
            # Message 23 fields
            msg.get("x1_23"),
            msg.get("y1_23"),
            msg.get("x2_23"),
            msg.get("y2_23"),
            msg.get("station_type"),
            msg.get("type_and_cargo"),
            msg.get("interval_raw"),
            msg.get("quiet"),
            # spares
            msg.get("spare"),
            msg.get("spare2"),
            msg.get("spare3"),
            msg.get("spare4"),
            # Tagblock
            json.dumps(msg.get("tagblock_group")),
            msg.get("tagblock_line_count"),
            msg.get("tagblock_station"),
            msg.get("tagblock_timestamp"),
        )

        return (query, params)

    def search(
        self,
        msg_id: Optional[Union[int, List[int]]] = None,
        mmsi: Optional[Union[int, List[int]]] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ) -> gpd.GeoDataFrame:
        """
        Return a minimal query on system management messages (15,16,17,20,22,23)
        with optional filtering by:
          - msg_id: single int or list of {15,16,17,20,22,23}
          - mmsi: single int or list
          - start_date, end_date: date range (based on tagblock_timestamp)

        We omit lat/lon filtering for now (some messages don't have them anyway).
        """
        if not conn:
            conn = self._conn

        try:
            query = "SELECT * FROM ais_msg_15_16_17_20_22_23 WHERE 1=1"
            params = []

            # todo(thalia) add a logging info message if an invalid id is given
            valid_ids = {15, 16, 17, 20, 22, 23}
            # Filter by message ID(s)
            if msg_id is not None:
                if isinstance(msg_id, int):
                    msg_id = [msg_id]
                requested_ids = set(msg_id).intersection(valid_ids)
                if requested_ids:
                    placeholders = ", ".join(["?"] * len(requested_ids))
                    query += f" AND id IN ({placeholders})"
                    params.extend(requested_ids)
                else:
                    # If user requested IDs not in {15,16,17,20,22,23}, return empty
                    return gpd.GeoDataFrame(columns=["geometry"])

            # Filter by MMSI (either single int or list of ints)
            if mmsi is not None:
                if isinstance(mmsi, int):
                    query += " AND mmsi = ?"
                    params.append(mmsi)
                elif isinstance(mmsi, list) and all(isinstance(i, int) for i in mmsi):
                    placeholders = ", ".join(["?"] * len(mmsi))
                    query += f" AND mmsi IN ({placeholders})"
                    params.extend(mmsi)
                else:
                    raise ValueError("MMSI must be an int or list of ints.")

            if start_date:
                start_ts = date_to_tagblock_timestamp(*map(int, start_date.split("-")))
                query += " AND tagblock_timestamp >= ?"
                params.append(start_ts)

            if end_date:
                end_ts = date_to_tagblock_timestamp(*map(int, end_date.split("-")))
                query += " AND tagblock_timestamp <= ?"
                params.append(end_ts)

            logger.info(f"System Management query: {query} with params={params}")
            df = cached_query(conn, query, params, True)
            if df.empty:
                # Return an empty GeoDataFrame with a geometry column
                return gpd.GeoDataFrame(columns=["geometry"])

            # We won't do lat/lon geometry building here
            # We can just return a normal DataFrame as a GeoDataFrame with no geometry
            return gpd.GeoDataFrame(df, geometry=None)

        except duckdb.Error as db_err:
            logger.error(f"DuckDB error: {db_err}")
        except ValueError as ve:
            logger.error(f"Value error: {ve}")
        except Exception as e:
            logger.error(f"Unexpected error: {e}")

        # On any failure, return empty
        return gpd.GeoDataFrame()
