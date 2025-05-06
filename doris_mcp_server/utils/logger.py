"""
Unified Logging Configuration Module

Provides unified logging configuration, including:
- General logs: Record all program execution information
- Audit logs: Record JSON data for key operations and processing results
- Error logs: Specifically record program exceptions and errors
"""

import os
import sys
import logging
import logging.handlers
from pathlib import Path
from typing import Dict
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv(override=True)

# Get project root directory
PROJECT_ROOT = Path(__file__).parents[2].absolute()

# Get log configuration from environment variables
LOG_DIR = os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs"))
LOG_PREFIX = os.getenv("LOG_PREFIX", "doris_mcp")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_MAX_DAYS = int(os.getenv("LOG_MAX_DAYS", "30"))
# Whether to output logs to the console (should be disabled when running as a service)
CONSOLE_LOGGING = os.getenv("CONSOLE_LOGGING", "false").lower() == "true"
# Whether stdio transport mode is being used
STDIO_MODE = os.getenv("MCP_TRANSPORT_TYPE", "").lower() == "stdio"

def purge_old_logs():
    """Clean up expired log files"""
    # --- Only perform cleanup in non-Stdio mode ---
    if STDIO_MODE:
        return 
    try:
        now = datetime.now()
        log_dir = Path(LOG_DIR)
        # Check if directory exists and is readable/writable
        if not log_dir.is_dir() or not os.access(LOG_DIR, os.W_OK):
             if not STDIO_MODE: # Avoid printing to stdout in stdio mode
                  print(f"Warning: Log directory {LOG_DIR} not accessible, skipping log purge.", file=sys.stderr)
             return

        for log_file in log_dir.glob(f"{LOG_PREFIX}*.20*"):
            # Parse date
            file_name = log_file.name
            date_str = None
            
            # Try to find the date part
            parts = file_name.split('.')
            for part in parts:
                if part.startswith('20') and len(part) == 8:  # 20YYMMDD format
                    date_str = part
                    break
            
            if date_str:
                try:
                    file_date = datetime.strptime(date_str, '%Y%m%d')
                    days_old = (now - file_date).days
                    
                    if days_old > LOG_MAX_DAYS:
                        os.remove(log_file)
                        if not STDIO_MODE:
                            print(f"Deleted expired log file: {log_file}")
                except (ValueError, OSError) as e:
                    if not STDIO_MODE:
                        print(f"Error processing log file {file_name}: {e}", file=sys.stderr)
    except Exception as e:
        if not STDIO_MODE:
            print(f"Error cleaning up logs: {e}", file=sys.stderr)

# Force disable console log output if in stdio mode
if STDIO_MODE:
    CONSOLE_LOGGING = False

# --- Only create log directory and clean old logs in non-Stdio mode ---
if not STDIO_MODE:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        # Clean up expired logs on startup (also moved here, as it only handles file logs)
        purge_old_logs() 
    except OSError as e:
        # If directory creation fails (e.g., permission issue), print warning but continue to avoid startup failure
        print(f"Warning: Failed to create log directory {LOG_DIR} or purge logs: {e}", file=sys.stderr)

# Log file paths (definition still needed, but files might not be created/used)
LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.log")
AUDIT_LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.audit")
ERROR_LOG_FILE = os.path.join(LOG_DIR, f"{LOG_PREFIX}.error")

# Log level mapping
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL
}

# Log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
AUDIT_FORMAT = '%(asctime)s - %(name)s - %(message)s'
ERROR_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s'

# Dedicated audit log level
AUDIT = 25  # Level between INFO and WARNING
logging.addLevelName(AUDIT, "AUDIT")

# Logger object cache
_loggers: Dict[str, logging.Logger] = {}

# Handler type mapping, used to ensure no duplicates are added
_handler_types = {
    'console': logging.StreamHandler,
    'file': logging.handlers.TimedRotatingFileHandler,
    'audit': logging.handlers.TimedRotatingFileHandler,
    'error': logging.handlers.TimedRotatingFileHandler
}


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name
    
    Args:
        name: Logger name
        
    Returns:
        logging.Logger: Configured logger
    """
    if name in _loggers:
        return _loggers[name]
    
    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))
    
    # Avoid duplicate logs caused by propagation
    logger.propagate = False
    
    # Check if handlers already exist to avoid duplicates
    handler_types = set(type(h) for h in logger.handlers)
    
    # Add audit log method
    def audit(self, message, *args, **kwargs):
        self.log(AUDIT, message, *args, **kwargs)
    
    logger.audit = audit.__get__(logger)
    
    # General log handler - output to console (only if enabled)
    if CONSOLE_LOGGING and _handler_types['console'] not in handler_types:
        # Use stderr instead of stdout to avoid conflicts with MCP communication
        console_handler = logging.StreamHandler(sys.stderr)
        console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(console_handler)
    
    # --- Only add file handlers in non-Stdio mode ---
    if not STDIO_MODE:
        # General log handler - daily rotating file
        if _handler_types['file'] not in handler_types:
            try: # Add try-except block
                file_handler = logging.handlers.TimedRotatingFileHandler(
                    LOG_FILE,
                    when='midnight',
                    interval=1,
                    backupCount=LOG_MAX_DAYS,
                    encoding='utf-8'
                )
                file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
                file_handler.suffix = "%Y%m%d"
                logger.addHandler(file_handler)
            except OSError as e:
                 print(f"Warning: Failed to add file log handler for {LOG_FILE}: {e}", file=sys.stderr)

        # Audit log handler - only logs AUDIT level
        if _handler_types['audit'] not in handler_types:
            try: # Add try-except block
                audit_handler = logging.handlers.TimedRotatingFileHandler(
                    AUDIT_LOG_FILE,
                    when='midnight',
                    interval=1,
                    backupCount=LOG_MAX_DAYS,
                    encoding='utf-8'
                )
                audit_handler.setFormatter(logging.Formatter(AUDIT_FORMAT))
                audit_handler.suffix = "%Y%m%d"
                audit_handler.setLevel(AUDIT)
                audit_handler.addFilter(lambda record: record.levelno == AUDIT)
                logger.addHandler(audit_handler)
            except OSError as e:
                 print(f"Warning: Failed to add audit log handler for {AUDIT_LOG_FILE}: {e}", file=sys.stderr)

        # Error log handler - only logs ERROR level and above
        if _handler_types['error'] not in handler_types:
            try: # Add try-except block
                error_handler = logging.handlers.TimedRotatingFileHandler(
                    ERROR_LOG_FILE,
                    when='midnight',
                    interval=1,
                    backupCount=LOG_MAX_DAYS,
                    encoding='utf-8'
                )
                error_handler.setFormatter(logging.Formatter(ERROR_FORMAT))
                error_handler.suffix = "%Y%m%d"
                error_handler.setLevel(logging.ERROR)
                logger.addHandler(error_handler)
            except OSError as e:
                 print(f"Warning: Failed to add error log handler for {ERROR_LOG_FILE}: {e}", file=sys.stderr)

    # Cache logger
    _loggers[name] = logger
    
    return logger

# Default logger
logger = get_logger('doris_mcp')

# Audit logger - for recording processing results, business operations, etc.
audit_logger = get_logger('audit')

# Call to clean logs moved after directory creation, and added non-stdio check