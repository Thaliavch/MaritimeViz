"""Unit test package for maritimeviz."""

import os
import logging

log_dir = os.path.join(
    os.path.dirname(__file__), "..", "logs"
)  # Creates logs/ in project root
log_file = os.path.join(log_dir, "test_log.log")

# Ensure the directory exists
os.makedirs(log_dir, exist_ok=True)

# Configure logging
logging.basicConfig(
    filename=log_file,
    filemode="w",  # "w" to overwrite, "a" to append
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)
logger.info("Logging initialized. Test execution started.")
