from . import logger
from concurrent.futures import ThreadPoolExecutor
import ais
import duckdb
from src.maritimeviz.utils.ais_db_utils import *



class AISDatabase:
    """
    Class to manage the initialization, population, and interaction with the AIS database.
    """
    def __init__(self, db_path="ais_data.duckdb"):
        self.db_path = db_path
        self.connection = None

    def initialize_database(self):
        """
        Initialize DuckDB database.
        """
        self.connection = duckdb.connect(self.db_path)
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
        self.connection.execute(create_table_query_for_1_2_3)
        self.connection.execute(create_table_query_for_5)



    def process_file(self, file_path, threading_stats=(4,500)):
      """
      Process the AIS file using on-the-fly chunk splitting and multithreading.
      """
    # Create the database tables
      self.initialize_database()

      try:
        threading_stats = optimal_threading_stats(file_path) # thread and chunk size
        print(f"Threading parameters: {threading_stats}")
      except:
        logging.info("Using default threading values: 4 threads and chunks of 500 lines")


      # Use a ThreadPoolExecutor for processing
      with ThreadPoolExecutor(max_workers= threading_stats[0]) as executor:
          for chunk in split_file_generator(file_path, threading_stats[1]):
              executor.submit(process_chunk_to_db, self.connection, chunk)

      self.close()

    def close(self):
        """
        Close the database connection.
        """
        if self.connection:
            self.connection.close()
            self.connection = None
