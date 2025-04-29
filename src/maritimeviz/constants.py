"""
Universal constants, variables and user defined data types
"""

from typing import TypedDict, List, Union
from enum import Enum

"""
Table Columns
"""
AIS_MSG_123_COLUMNS = [
    "id",
    "repeat_indicator",
    "mmsi",
    "nav_status",
    "rot_over_range",
    "rot",
    "sog",
    "position_accuracy",
    "x",
    "y",
    "cog",
    "true_heading",
    "timestamp",
    "special_manoeuvre",
    "spare",
    "raim",
    "sync_state",
    "slot_timeout",
    "received_stations",
    "tagblock_group",
    "tagblock_line_count",
    "tagblock_station",
    "tagblock_timestamp",
]
AIS_MSG_5_COLUMNS = [
    "id",
    "repeat_indicator",
    "mmsi",
    "ais_version",
    "imo",
    "call_sign",
    "ship_name",
    "type_of_ship_and_cargo",
    "to_bow",
    "to_stern",
    "to_port",
    "to_starboard",
    "position_fixing_device",
    "eta",
    "max_present_static_draught",
    "destination",
    "dte",
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

QUERY_CREATE_TABLE_6 = """
        CREATE TABLE IF NOT EXISTS ais_msg_6 (
            id INTEGER,                  -- Always 6
            repeat_indicator INTEGER,    -- 0-3
            mmsi INTEGER,

            spare INTEGER,
            spare2 INTEGER,
            dac INTEGER,
            fid INTEGER,                 -- fi or fid (6-bit function ID)
            x DOUBLE,                    -- longitude (if present)
            y DOUBLE,                    -- latitude (if present)

            application_data JSON,       -- leftover data or entire binary payload

            -- Tagblock metadata:
            tagblock_group JSON,
            tagblock_line_count INTEGER,
            tagblock_station TEXT,
            tagblock_timestamp BIGINT
        );

        """

QUERY_CREATE_TABLE_8 = """
            CREATE TABLE IF NOT EXISTS ais_msg_8 (
                id INTEGER,                  -- Always 8
                repeat_indicator INTEGER,    -- 0-3
                mmsi INTEGER,

                dac INTEGER,
                fid INTEGER,                 -- fi or fid (6-bit function ID)
                x DOUBLE,                    -- longitude (if present)
                y DOUBLE,                    -- latitude (if present)

                application_data JSON,       -- leftover data or entire binary payload

                -- Tagblock metadata:
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

# todo(thalia) ask kurt if better to have separate tables and what about x and y for 25/26
QUERY_CREATE_TABLE_25_26 = """
            CREATE TABLE IF NOT EXISTS ais_msg_25_26 (
              id INTEGER,
              repeat_indicator INTEGER,
              mmsi INTEGER,

              -- Not Shared fields:
              dest_mmsi INTEGER,
              sync_state INTEGER,
              received_stations INTEGER,
              x DOUBLE,
              y DOUBLE,

              -- JSON fallback:
              application_data JSON,

              -- Tagblock/metadata:
              tagblock_group JSON,
              tagblock_line_count INTEGER,
              tagblock_station TEXT,
              tagblock_timestamp BIGINT
            );

            """

# Long Range position report for vessels of class A and class B 'SO' equipped vessels"
QUERY_CREATE_TABLE_27 = """
            CREATE TABLE IF NOT EXISTS ais_msg_27 (
                id INTEGER,
                repeat_indicator INTEGER,
                mmsi INTEGER,
                position_accuracy INTEGER,
                raim BOOLEAN,
                nav_status INTEGER,
                x DOUBLE,
                y DOUBLE,
                sog INTEGER,
                cog INTEGER,
                gnss BOOLEAN,
                spare INTEGER,
                vessel_type TEXT, -- programmatically determined based on mmsi ('A' or 'B')
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

# Aid to Navigation (ATON) Messages
QUERY_CREATE_TABLE_21 = """
            CREATE TABLE IF NOT EXISTS ais_msg_21 (
                -- Core AIS fields:
                id INTEGER,               -- 21
                repeat_indicator INTEGER, -- 0-3
                mmsi INTEGER,

                spare INTEGER,
                aton_type INTEGER,
                name TEXT,
                position_accuracy INTEGER,
                x DOUBLE,
                y DOUBLE,
                dim_a INTEGER,
                dim_b INTEGER,
                dim_c INTEGER,
                dim_d INTEGER,
                fix_type INTEGER,
                timestamp INTEGER,
                off_pos BOOLEAN,
                aton_status INTEGER,
                raim BOOLEAN,
                virtual_aton BOOLEAN,
                assigned_mode BOOLEAN,

                -- Catch-all JSON for leftover fields:
                application_data JSON,

                -- Tagblock metadata:
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

# Base Stations Report
QUERY_CREATE_TABLE_4 = """
            CREATE TABLE IF NOT EXISTS ais_msg_4 (
                id INTEGER,                   -- 4
                repeat_indicator INTEGER,     -- 0-3
                mmsi INTEGER,

                -- Date/time fields (commonly found in Msg 4):
                year INTEGER,
                month INTEGER,
                day INTEGER,
                hour INTEGER,
                minute INTEGER,
                second INTEGER,

                position_accuracy INTEGER,    -- 0 or 1
                x DOUBLE,                     -- Longitude
                y DOUBLE,                     -- Latitude
                fix_type INTEGER,             -- e.g. 7 or 15
                transmission_ctl INTEGER,     -- e.g. 'transmission_ctl': 0
                spare INTEGER,
                raim BOOLEAN,
                sync_state INTEGER,           -- e.g. 0
                slot_timeout INTEGER,         -- up to 7
                slot_offset INTEGER,          -- For some messages
                slot_number INTEGER,          -- For others
                received_stations INTEGER,    -- If present

                -- Fallback JSON for anything else:
                application_data JSON,

                -- Tagblock / metadata:
                tagblock_group JSON,
                tagblock_line_count INTEGER,
                tagblock_station TEXT,
                tagblock_timestamp BIGINT
            );
            """

# *******************************************************

QUERY_CREATE_TABLE_7_13 = """
CREATE TABLE IF NOT EXISTS ais_msg_7_13 (
    id INTEGER,                -- 7 or 13
    repeat_indicator INTEGER,
    mmsi BIGINT,
    ack_count INTEGER,         -- could store in JSON

    application_data JSON,
    tagblock_group JSON,
    tagblock_line_count INTEGER,
    tagblock_station TEXT,
    tagblock_timestamp BIGINT
);
"""

QUERY_CREATE_TABLE_12_14 = """
CREATE TABLE IF NOT EXISTS ais_msg_12_14 (
    id INTEGER,         -- 12 or 14
    repeat_indicator INTEGER,
    mmsi BIGINT,

    message_text TEXT,
    addressed BOOLEAN,  -- True if ID=12, False if ID=14

    application_data JSON,
    tagblock_group JSON,
    tagblock_line_count INTEGER,
    tagblock_station TEXT,
    tagblock_timestamp BIGINT
);
"""

QUERY_CREATE_TABLE_9 = """
CREATE TABLE IF NOT EXISTS ais_msg_9 (
    id INTEGER,               -- 9
    repeat_indicator INTEGER,
    mmsi BIGINT,
    altitude INTEGER,
    sog FLOAT,
    position_accuracy INTEGER,
    x DOUBLE,
    y DOUBLE,
    cog FLOAT,
    timestamp INTEGER,
    raim BOOLEAN,
    spare INTEGER,
    application_data JSON,
    tagblock_group JSON,
    tagblock_line_count INTEGER,
    tagblock_station TEXT,
    tagblock_timestamp BIGINT
);
"""

# todo(thalia) ask kurt if it is better to break into two tables and use a view
QUERY_CREATE_TABLE_10_11 = """
        CREATE TABLE IF NOT EXISTS ais_msg_10_11 (
            -- Common fields:
            id INTEGER,
            repeat_indicator INTEGER,
            mmsi BIGINT,

            -- Fields for Message 10 (UTC/Date inquiry):
            spare INTEGER,
            dest_mmsi BIGINT,
            spare2 INTEGER,

            -- Fields for Message 11 (UTC/Date response):
            year INTEGER,
            month INTEGER,
            day INTEGER,
            hour INTEGER,
            minute INTEGER,
            second INTEGER,
            position_accuracy INTEGER,
            x DOUBLE,
            y DOUBLE,
            fix_type INTEGER,
            transmission_ctl INTEGER,
            raim BOOLEAN,
            sync_state INTEGER,
            slot_timeout INTEGER,
            slot_offset INTEGER,

            -- Tagblock metadata (common to both):
            tagblock_group JSON,
            tagblock_line_count INTEGER,
            tagblock_station TEXT,
            tagblock_timestamp BIGINT
        );
        """

QUERY_CREATE_TABLE_15_16_17_20_22_23 = """
        CREATE TABLE IF NOT EXISTS ais_msg_15_16_17_20_22_23 (
            id INTEGER,
            repeat_indicator INTEGER,
            mmsi BIGINT,

            -- For message 15
            mmsi_1 BIGINT,
            msg_1_1 INTEGER,
            slot_offset_1_1 INTEGER,
            dest_msg_1_2 INTEGER,
            slot_offset_1_2 INTEGER,
            mmsi_2 BIGINT,
            msg_2 INTEGER,
            slot_offset_2 INTEGER,

            -- For message 16
            dest_mmsi_a BIGINT,
            offset_a INTEGER,
            inc_a INTEGER,
            dest_mmsi_b BIGINT,
            offset_b INTEGER,
            inc_b INTEGER,

            -- For message 17
            x DOUBLE,
            y DOUBLE,

            -- For message 20 (contains an array of reservations)
            reservations JSON,

            -- For message 22
            chan_a INTEGER,
            chan_b INTEGER,
            txrx_mode INTEGER,
            power_low BOOLEAN,
            x1 DOUBLE,
            y1 DOUBLE,
            x2 DOUBLE,
            y2 DOUBLE,
            chan_a_bandwidth INTEGER,
            chan_b_bandwidth INTEGER,
            zone_size INTEGER,

            -- For message 23
            x1_23 DOUBLE,
            y1_23 DOUBLE,
            x2_23 DOUBLE,
            y2_23 DOUBLE,
            station_type INTEGER,
            type_and_cargo INTEGER,
            interval_raw INTEGER,
            quiet INTEGER,

            -- Generic spare fields (some messages have extra spares)
            spare INTEGER,
            spare2 INTEGER,
            spare3 INTEGER,
            spare4 INTEGER,

            -- Tagblock fields
            tagblock_group JSON,
            tagblock_line_count INTEGER,
            tagblock_station TEXT,
            tagblock_timestamp BIGINT
        );
        """

# ********************************************************

QUERY_CREATE_GLOBAL_DYNAMIC_VIEW = """
            CREATE OR REPLACE VIEW global_ais_dynamic AS
            SELECT
              id,
              repeat_indicator,
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
              NULL AS part_num,
              NULL AS name,
              NULL AS type_and_cargo,
              NULL AS vendor_id,
              NULL AS callsign,
              NULL AS dim_a,
              NULL AS dim_b,
              NULL AS dim_c,
              NULL AS dim_d,
              NULL AS spare,
              'dynamicA' AS message_type
            FROM ais_msg_123
            UNION ALL
            SELECT
              id,
              repeat_indicator,
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
              NULL AS part_num,
              NULL AS name,
              NULL AS type_and_cargo,
              NULL AS vendor_id,
              NULL AS callsign,
              NULL AS dim_a,
              NULL AS dim_b,
              NULL AS dim_c,
              NULL AS dim_d,
              NULL AS spare,
              'dynamicB' AS message_type
            FROM ais_msg_18_19;

            """

QUERY_CREATE_GLOBAL_STATIC_VIEW = """
            CREATE OR REPLACE VIEW global_ais_static AS
            SELECT
              id,
              repeat_indicator,
              mmsi,
              NULL AS x,
              NULL AS y,
              NULL AS tagblock_timestamp,
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
              NULL AS part_num,
              NULL AS name,
              NULL AS type_and_cargo,
              NULL AS vendor_id,
              NULL AS callsign,
              NULL AS dim_a,
              NULL AS dim_b,
              NULL AS dim_c,
              NULL AS dim_d,
              NULL AS spare,
              'staticA' AS message_type
            FROM ais_msg_5
            UNION ALL
            SELECT
              id,
              repeat_indicator,
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
              part_num,
              name,
              type_and_cargo,
              vendor_id,
              callsign,
              dim_a,
              dim_b,
              dim_c,
              dim_d,
              spare,
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
DATABASE_TYPE_A_TABLE_CREATION_QUERIES = [
    QUERY_CREATE_TABLE_1_2_3,
    QUERY_CREATE_TABLE_5,
]
DATABASE_TYPE_B_TABLE_CREATION_QUERIES = [
    QUERY_CREATE_TABLE_18_19,
    QUERY_CREATE_TABLE_24,
]
DATABASE_ASM_CREATION_QUERIES = [
    QUERY_CREATE_TABLE_6,
    QUERY_CREATE_TABLE_8,
    QUERY_CREATE_TABLE_25_26,
]
DATABASE_STANDALONE_MULTIPURPOSE_CREATION_QUERY = [
    QUERY_CREATE_TABLE_27,
    QUERY_CREATE_TABLE_21,
    QUERY_CREATE_TABLE_4,
    QUERY_CREATE_TABLE_9,
    QUERY_CREATE_TABLE_10_11,
    QUERY_CREATE_TABLE_15_16_17_20_22_23,
]  # 27 for long range broadcasting, 21 for aid to navigation, 4 for Base Station Report
DATABASE_SAFETY_ACK_TABLE_CREATION_QUERIES = [
    QUERY_CREATE_TABLE_7_13,
    QUERY_CREATE_TABLE_12_14,
]

DATABASE_ALL_TABLE_CREATION_QUERIES = (
    DATABASE_TYPE_A_TABLE_CREATION_QUERIES
    + DATABASE_TYPE_B_TABLE_CREATION_QUERIES
    + DATABASE_ASM_CREATION_QUERIES
    + DATABASE_STANDALONE_MULTIPURPOSE_CREATION_QUERY
    + DATABASE_SAFETY_ACK_TABLE_CREATION_QUERIES
)


DATABASE_ALL_VIEWS_CREATION_QUERIES = [
    QUERY_CREATE_GLOBAL_DYNAMIC_VIEW,
    QUERY_CREATE_GLOBAL_STATIC_VIEW,
    QUERY_CREATE_GLOBAL_VIEW,
]

ALLOWED_FILTER_KEYS = {
    "mmsi",
    "start_date",
    "end_date",
    "polygon_bounds",
    "min_velocity",
    "max_velocity",
    "direction",
    "min_turn_rate",
    "max_turn_rate",
}

ALLOWED_FILTER_KEYS_CLASS_A = {
    "mmsi",
    "start_date",
    "end_date",
    "polygon_bounds",
    "min_velocity",
    "max_velocity",
    "direction",
    "min_turn_rate",
    "max_turn_rate",
}

ALLOWED_FILTER_KEYS_CLASS_B = {
    "mmsi",
    "start_date",
    "end_date",
    "polygon_bounds",
    "min_velocity",
    "max_velocity",
    "direction",
}


class FilterCriteria(
    TypedDict, total=False
):  # total set to False to make all fields optional
    mmsi: Union[int, List[int]]
    start_date: str
    end_date: str
    polygon_bounds: str
    min_velocity: float  # Minimum speed (sog)
    max_velocity: float  # Maximum speed (sog)
    direction: str  # Cardinal direction filter ("N", "E", "S", "W")
    min_turn_rate: float  # Minimum rate of turn (rot)
    max_turn_rate: float  # Maximum rate of turn (rot)


# enum for message types TODO(Thalia): get rid of it if not used after refactoring
class MessageType(Enum):
    A = "A"
    B = "B"
    C = "C"
