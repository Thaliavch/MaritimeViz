"""
Universal constants, variables and user defined data types
"""
from typing import TypedDict, List, Union
from enum import Enum

"""
Table Columns
"""
AIS_MSG_123_COLUMNS = [
    "id", "repeat_indicator", "mmsi", "nav_status", "rot_over_range", "rot",
    "sog", "position_accuracy", "x", "y", "cog", "true_heading", "timestamp",
    "special_manoeuvre", "spare", "raim", "sync_state", "slot_timeout",
    "received_stations", "tagblock_group", "tagblock_line_count",
    "tagblock_station", "tagblock_timestamp"
]
AIS_MSG_5_COLUMNS = [
    "id", "repeat_indicator", "mmsi", "ais_version", "imo", "call_sign",
    "ship_name", "type_of_ship_and_cargo", "to_bow", "to_stern", "to_port",
    "to_starboard", "position_fixing_device", "eta",
    "max_present_static_draught", "destination", "dte"
]

# Queries for database table creation

# dynamic reports for Class A
QUERY_CREATE_TABLE_1_2_3 = """
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

# Static reports for Class A
QUERY_CREATE_TABLE_5 = """
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

# Table for AIS messages type 18 and 19 (dynamic reports for Class B)
QUERY_CREATE_TABLE_18_19 = """
            CREATE TABLE IF NOT EXISTS ais_msg_18_19 (
                id INTEGER,
                repeat_indicator INTEGER,
                mmsi BIGINT,
                spare INTEGER,
                sog FLOAT,
                position_accuracy INTEGER,
                x DOUBLE,
                y DOUBLE,
                cog FLOAT,
                true_heading INTEGER,
                timestamp INTEGER,
                spare2 INTEGER,
                unit_flag INTEGER,
                display_flag INTEGER,
                dsc_flag INTEGER,
                band_flag INTEGER,
                m22_flag INTEGER,
                mode_flag INTEGER,
                raim BOOLEAN,
                commstate_flag INTEGER,
                commstate_cs_fill INTEGER,
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

# Table for AIS messages type 24 (static/voyage-related reports)
QUERY_CREATE_TABLE_24 = """
            CREATE TABLE IF NOT EXISTS ais_msg_24 (
                id INTEGER,
                repeat_indicator INTEGER,
                mmsi BIGINT,
                part_num INTEGER,
                name VARCHAR,               -- Vessel name (present in part 0)
                type_and_cargo INTEGER,     -- Type and cargo (part 1)
                vendor_id VARCHAR,          -- Vendor ID if provided
                callsign VARCHAR,           -- Vessel callsign
                dim_a INTEGER,              -- Dimension A (bow)
                dim_b INTEGER,              -- Dimension B (stern)
                dim_c INTEGER,              -- Dimension C (port)
                dim_d INTEGER,              -- Dimension D (starboard)
                spare INTEGER,
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

QUERY_CREATE_GLOBAL_DYNAMIC_VIEW = """
            CREATE OR REPLACE VIEW global_ais_dynamic AS
            SELECT
              mmsi,
              x,
              y,
              tagblock_timestamp,
              nav_status,
              rot_over_range,
              rot,
              sog,
              position_accuracy,
              cog,
              true_heading,
              timestamp,
              special_manoeuvre,
              spare,
              raim,
              sync_state,
              slot_timeout,
              slot_number,
              tagblock_group,
              tagblock_line_count,
              tagblock_station,
              NULL AS ais_version,
              NULL AS imo,
              NULL AS call_sign,
              NULL AS ship_name,
              NULL AS type_of_ship_and_cargo,
              NULL AS to_bow,
              NULL AS to_stern,
              NULL AS to_port,
              NULL AS to_starboard,
              NULL AS position_fixing_device,
              NULL AS eta,
              NULL AS max_present_static_draught,
              NULL AS destination,
              NULL AS dte,
              'dynamicA' AS message_type
            FROM ais_msg_123
            UNION ALL
            SELECT
              mmsi,
              x,
              y,
              timestamp AS tagblock_timestamp,
              NULL AS nav_status,
              NULL AS rot_over_range,
              NULL AS rot,
              sog,
              position_accuracy,
              cog,
              true_heading,
              timestamp,
              NULL AS special_manoeuvre,
              spare,
              raim,
              NULL AS sync_state,
              NULL AS slot_timeout,
              NULL AS slot_number,
              tagblock_group,
              tagblock_line_count,
              tagblock_station,
              NULL AS ais_version,
              NULL AS imo,
              NULL AS call_sign,
              NULL AS ship_name,
              NULL AS type_of_ship_and_cargo,
              NULL AS to_bow,
              NULL AS to_stern,
              NULL AS to_port,
              NULL AS to_starboard,
              NULL AS position_fixing_device,
              NULL AS eta,
              NULL AS max_present_static_draught,
              NULL AS destination,
              NULL AS dte,
              'dynamicB' AS message_type
            FROM ais_msg_18_19;

            """

QUERY_CREATE_GLOBAL_STATIC_VIEW = """
            CREATE OR REPLACE VIEW global_ais_static AS
            SELECT
              mmsi,
              NULL AS x,
              NULL AS y,
              tagblock_timestamp,
              NULL AS nav_status,
              NULL AS rot_over_range,
              NULL AS rot,
              NULL AS sog,
              NULL AS position_accuracy,
              NULL AS cog,
              NULL AS true_heading,
              NULL AS timestamp,
              NULL AS special_manoeuvre,
              NULL AS spare,
              NULL AS raim,
              NULL AS sync_state,
              NULL AS slot_timeout,
              NULL AS slot_number,
              NULL AS tagblock_group,
              NULL AS tagblock_line_count,
              NULL AS tagblock_station,
              ais_version,
              imo,
              call_sign,
              ship_name,
              type_of_ship_and_cargo,
              to_bow,
              to_stern,
              to_port,
              to_starboard,
              position_fixing_device,
              eta,
              max_present_static_draught,
              destination,
              dte,
              'staticA' AS message_type
            FROM ais_msg_5
            UNION ALL
            SELECT
              mmsi,
              NULL AS x,
              NULL AS y,
              tagblock_timestamp,
              NULL AS nav_status,
              NULL AS rot_over_range,
              NULL AS rot,
              NULL AS sog,
              NULL AS position_accuracy,
              NULL AS cog,
              NULL AS true_heading,
              NULL AS timestamp,
              NULL AS special_manoeuvre,
              NULL AS spare,
              NULL AS raim,
              NULL AS sync_state,
              NULL AS slot_timeout,
              NULL AS slot_number,
              tagblock_group,
              tagblock_line_count,
              tagblock_station,
              NULL AS ais_version,
              NULL AS imo,
              NULL AS call_sign,
              NULL AS ship_name,
              NULL AS type_of_ship_and_cargo,
              NULL AS to_bow,
              NULL AS to_stern,
              NULL AS to_port,
              NULL AS to_starboard,
              NULL AS position_fixing_device,
              NULL AS eta,
              NULL AS max_present_static_draught,
              NULL AS destination,
              NULL AS dte,
              'staticB' AS message_type
            FROM ais_msg_24;

            """

QUERY_CREATE_GLOBAL_VIEW = """
            CREATE OR REPLACE VIEW global_ais_data AS
            SELECT * FROM global_ais_dynamic
            UNION ALL
            SELECT * FROM global_ais_static;
            """

# List of all table creation queries
DATABASE_ALL_TABLE_CREATION_QUERIES = [QUERY_CREATE_TABLE_1_2_3, QUERY_CREATE_TABLE_5, QUERY_CREATE_TABLE_18_19, QUERY_CREATE_TABLE_24]
DATABASE_TYPE_A_TABLE_CREATION_QUERIES = [QUERY_CREATE_TABLE_1_2_3, QUERY_CREATE_TABLE_5]
DATABASE_TYPE_B_TABLE_CREATION_QUERIES = [QUERY_CREATE_TABLE_18_19, QUERY_CREATE_TABLE_24]
DATABASE_ALL_VIEWS_CREATION_QUERIES = [QUERY_CREATE_GLOBAL_DYNAMIC_VIEW, QUERY_CREATE_GLOBAL_STATIC_VIEW, QUERY_CREATE_GLOBAL_VIEW]

ALLOWED_FILTER_KEYS = {
    "mmsi", "start_date", "end_date", "polygon_bounds",
    "min_velocity", "max_velocity", "direction",
    "min_turn_rate", "max_turn_rate"
}

# TODO(Thalia): Crete global filter object and one per class.
class FilterCriteria(TypedDict, total=False): # total set to False to make all fields optional
    mmsi: Union[int, List[int]]
    start_date: str
    end_date: str
    polygon_bounds: str
    min_velocity: float      # Minimum speed (sog)
    max_velocity: float      # Maximum speed (sog)
    direction: str           # Cardinal direction filter ("N", "E", "S", "W")
    min_turn_rate: float     # Minimum rate of turn (rot)
    max_turn_rate: float     # Maximum rate of turn (rot)

# enum for message types TODO(Thalia): get rid of it if not used after refactoring
class MessageType(Enum):
    A = "A"
    B = "B"
    C = "C"
