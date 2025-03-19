import os
import json
from . import logger
import datetime


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


def optimal_threading_stats(ais_file, cpu_cores=None, min_chunk_size=500):
    try:
        total_lines = lines_per_file(ais_file)
        cpu_cores = os.cpu_count() or 4  # Default to 4 cores if unavailable

        # Ensure minimum lines per chunk to avoid too many threads
        max_chunks = min(total_lines // min_chunk_size,
                         cpu_cores * 4)  # Allow threads to oversubscribe slightly
        optimal_threads = min(cpu_cores,
                              max_chunks)  # Use no more threads than chunks

        # Calculate chunk size based on the number of threads
        chunk_size = max(min_chunk_size, total_lines // optimal_threads)

        return optimal_threads, chunk_size

    except:
        logger.error("Using default: {threads: 4, chunk_size: 500}")
        return 4, 500

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


