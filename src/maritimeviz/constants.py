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

