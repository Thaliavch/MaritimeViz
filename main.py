import logging
from src.maritimeviz.utils.logging_utils import setup_logging

setup_logging()
logging.getLogger(__name__).info("maritimeviz package initialized.")
