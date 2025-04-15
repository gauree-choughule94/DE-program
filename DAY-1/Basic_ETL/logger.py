import logging
import os
from datetime import datetime

# Ensure the "logs" directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Generate a single log file per day
log_filename = datetime.now().strftime("app_%Y-%m-%d.log")  
LOG_FILE = os.path.join(LOG_DIR, log_filename)

# Check if logging has already been configured to prevent duplicates
if not logging.getLogger().hasHandlers():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(LOG_FILE)  # Write logs to a single file
        ]
    )

# Create a logger instance
logger = logging.getLogger("ReportLogger")

def log_success(message):
    """Log a success message."""
    logger.info(f"SUCCESS: {message}")

def log_warning(message):
    """Log a warning message."""
    logger.warning(f"WARNING: {message}")

def log_error(message):
    """Log an error message."""
    logger.error(f"ERROR: {message}")
