import logging

# Configure the logger
logging.basicConfig(
    filename="logger.txt",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

