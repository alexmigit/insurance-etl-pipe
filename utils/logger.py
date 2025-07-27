import logging

logging.basicConfig(
    # Configure the logging system
    level=logging.INFO,
    # Set the logging level to INFO to capture all messages at this level and above     
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# Create a logger object
# This logger will be used to log messages throughout the application
logger = logging.getLogger()