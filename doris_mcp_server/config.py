# doris_mcp_server/config.py
import os
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(override=True)

# Get Log Level from environment variable, default to 'info'
LOG_LEVEL_STR = os.getenv('LOG_LEVEL', 'info').upper()

# Map string level to logging level constant
LOG_LEVEL_MAP = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}
LOG_LEVEL = LOG_LEVEL_MAP.get(LOG_LEVEL_STR, logging.INFO)

# Function to load config (can be expanded later if needed)
def load_config():
    """Loads configuration settings."""
    # Currently, configuration is mainly handled by environment variables
    # and constants defined in this module.
    # This function can be used to perform additional setup if required.
    logging.getLogger(__name__).info("Configuration loaded (mainly from environment variables).")

# You can add other configuration constants here if needed
# Example: DB_HOST = os.getenv("DB_HOST", "localhost")
# But often it's better to access os.getenv directly where needed
# or pass config dictionaries around. 