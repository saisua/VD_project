import os
from functools import lru_cache
import logging
from pathlib import Path
from time import sleep

# from utils.data_download.ghcn import load_data
# from utils.data_download.download_csv import load_data
# from utils.data_download.osm import load_data
# from utils.data_download.hisdac import load_data
from utils.data_download.world_data import load_data


logger = logging.getLogger(__name__)


DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DOWNLOADED_DATA_FILE = Path(os.environ.get(
    "DOWNLOADED_DATA_FILE", "daily-summaries-latest.tar.gz"
))
COMPRESSED_DATA_FILE = Path(os.environ.get(
    "COMPRESSED_DATA_FILE", "ghcnd-data.tar.gz"
))
DEFAULT_FILE_SUFFIX = os.environ.get("DEFAULT_FILE_SUFFIX", ".csv")
DEFAULT_FILE_LOAD_LIMIT = os.environ.get("DEFAULT_FILE_LOAD_LIMIT", 20)

lock_file = DEFAULT_DATA_DIR / ".lock"

cached_load_data = lru_cache(maxsize=1)(load_data)


def load_data(*args, **kwargs):
    # Wait for lock to be released
    if lock_file.exists():
        logger.info("Waiting for lock to be released")
        while lock_file.exists():
            sleep(1)

    # Create lock file
    try:
        lock_file.touch()
    except Exception as e:
        logger.error(f"Failed to create lock file: {e}")
        raise

    try:
        # Load the data
        return cached_load_data(*args, **kwargs)
    finally:
        # Remove lock file
        try:
            lock_file.unlink(missing_ok=True)
        except Exception as e:
            logger.error(f"Failed to remove lock file: {e}")
            raise
