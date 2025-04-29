import os
import datetime
from functools import cache
import duckdb
import pandas as pd
from typing import Optional, Union, List
import geopandas as gpd


# TODO(Thalia): thinking about renaming these two
def cached_query(
    conn: duckdb.DuckDBPyConnection,
    query: str,
    params: Optional[Union[List, tuple]],
    df: bool = False,
) -> Union[pd.DataFrame, List]:
    """
    Wrapper function for call_in_cached_query to handle conversion before caching
    """
    if not isinstance(params, tuple):
        if not isinstance(params, list):
            params = [params]
        params = tuple(params)
    return call_in_cached_query(conn, query, params, df)


@cache
def call_in_cached_query(
    conn: duckdb.DuckDBPyConnection, query: str, params: tuple, df: bool = False
) -> Union[pd.DataFrame, List]:
    """
    Execute an SQL query on the given DuckDB connection with caching.

    This function runs the provided SQL query with the given parameters on the
    DuckDB connection. If `df` is True, it returns the query results as a pandas
    DataFrame using `fetchdf()`. If `df` is False, it returns the raw results (a
    list of rows) using `fetchall()`.

    Parameters:
        conn (duckdb.DuckDBPyConnection): The DuckDB connection object.
        query (str): The SQL query string to execute.
        params (Union[List, tuple]): The parameters to use with the SQL query.
        df (bool, optional): Determines the format of the return value.
            - True: Return a pandas DataFrame.
            - False: Return a list of rows.
            Defaults to False.

    Returns:
        Union[pd.DataFrame, List]: The query results as a pandas DataFrame if `df`
        is True, or as a list of rows if `df` is False.
    """
    if not params:
        return conn.execute(query).fetchdf() if df else conn.execute(query).fetchall()
    return (
        conn.execute(query, params).fetchdf()
        if df
        else conn.execute(query, params).fetchall()
    )


def estimate_lines_by_size(file_path, avg_bytes_per_line=90):
    """
    Estimate the number of lines in a file based on its size in bytes.
    """
    file_size = os.path.getsize(file_path)
    estimated_lines = file_size // avg_bytes_per_line
    return estimated_lines


def count_lines(file_path):
    """
    Count the number of lines in a file.
    """
    with open(file_path, "r") as file:
        return sum(1 for _ in file)


def lines_per_file(file_path, avg_bytes_per_line=90, use_line_count=False):
    """
    Determine file stats (line count or estimated lines) based on preference.
    """
    if use_line_count:
        return count_lines(file_path)
    else:
        return estimate_lines_by_size(file_path, avg_bytes_per_line)


def split_file_generator(file_path, chunk_size=500):
    """
    Splits the file into fixed-size chunks and yields each chunk.
    """
    with open(file_path, "r") as file:
        chunk = []
        for i, line in enumerate(file):
            chunk.append(line)
            if (i + 1) % chunk_size == 0:  # Yield chunk when size is reached
                yield chunk
                chunk = []
        if chunk:  # Yield any remaining lines
            yield chunk


# TODO(Thalia): get rid of it once tests pass for refactored code
# def process_chunk_to_db(conn, chunk):
#     """
#     Process a chunk of lines and insert messages into the database.
#     """
#     import ais.stream  # Import required for threading compatibility
#
#     for msg in ais.stream.decode(chunk):
#         try:
#             if msg['id'] in {1, 2, 3, 5}:  # Filter messages
#                 insert_msg_to_db(conn, msg)  # Insert message into database
#         except Exception as e:
#             logger.error(f"Error processing message: {msg} - {e}")


def date_to_tagblock_timestamp(year, month, day, hour=0, minute=0, second=0):
    """
    Convert a specific date and time to a tagblock_timestamp (Unix timestamp).

    Parameters:
        year (int): Year of the date (e.g., 2025).
        month (int): Month of the date (1-12).
        day (int): Day of the month (1-31).
        hour (int): Hour of the day (0-23). Default is 0.
        minute (int): Minute of the hour (0-59). Default is 0.
        second (int): Second of the minute (0-59). Default is 0.

    Returns:
        int: Tagblock timestamp as a Unix timestamp.
    """
    # Create a datetime object for the given date and time in UTC
    dt = datetime.datetime(year, month, day, hour, minute, second)

    # Convert datetime to Unix timestamp
    timestamp = int(dt.timestamp())

    return timestamp


# May need this for testing, so probably will be moved to a testing utils file
def tagblock_timestamp_to_date(tagblock_timestamp):
    """
    Convert a tagblock_timestamp (Unix timestamp) to a human-readable date and time.

    Parameters:
        tagblock_timestamp (int): Unix timestamp.

    Returns:
        str: Date and time in the format "YYYY-MM-DD HH:MM:SS" (UTC).
    """
    # Convert the Unix timestamp to a datetime object in UTC
    dt = datetime.datetime.utcfromtimestamp(tagblock_timestamp)

    # Format the datetime object as a string
    readable_time = dt.strftime("%Y-%m-%d %H:%M:%S")

    return readable_time


def merge_dfs(
    dfs: List[pd.DataFrame], as_geodf: bool = True
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Merge a list of AIS DataFrames into a single DataFrame or GeoDataFrame.

    Parameters:
        dfs (List[pd.DataFrame]): List of DataFrames to merge.
        as_geodf (bool, optional): If True, converts the merged DataFrame into
        a GeoDataFrame by creating a geometry column from 'x' and 'y'. If False,
         returns a plain DataFrame. Defaults to True.

    Returns:
        Union[pd.DataFrame, gpd.GeoDataFrame]: The merged DataFrame
        (GeoDataFrame if as_geodf is True).
    """
    valid_dfs = [df for df in dfs if not df.empty]
    if not valid_dfs:
        return gpd.GeoDataFrame(columns=["geometry"]) if as_geodf else pd.DataFrame()

    merged_df = pd.concat(valid_dfs, ignore_index=True)

    if as_geodf and "x" in merged_df.columns and "y" in merged_df.columns:
        merged_df["geometry"] = gpd.points_from_xy(merged_df["x"], merged_df["y"])
        return gpd.GeoDataFrame(merged_df, geometry="geometry", crs="EPSG:4326")
    else:
        return merged_df


def guess_vessel_type(mmsi: int) -> str:
    """
    Guess the vessel type (Class A or Class B "SO") based on the MMSI number.

    According to ITU and IMO guidelines, MMSI numbers in the range 980000000 to 983999999
    are typically assigned to Class B "Self-Organizing" (SO) AIS transponders.
    All other MMSI values are assumed to belong to Class A vessels.

    Parameters:
        mmsi (int): The Maritime Mobile Service Identity number of the vessel.

    Returns:
        str: "A" if the vessel is likely Class A, "B" if it is likely Class B (SO).
    """
    return "B" if 980000000 <= mmsi <= 983999999 else "A"
