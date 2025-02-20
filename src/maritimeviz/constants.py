"""
Constant variables
"""

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

# List of all table creation queries
DATABASE_TABLE_CREATION_QUERIES = [QUERY_CREATE_TABLE_1_2_3, QUERY_CREATE_TABLE_5, QUERY_CREATE_TABLE_18_19, QUERY_CREATE_TABLE_24]
