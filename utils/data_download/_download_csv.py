import logging
import requests
import gzip
import os
from pathlib import Path
from typing import Optional

import polars as pl


logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DOWNLOADED_DATA_FILE = Path(os.environ.get(
    "DOWNLOADED_DATA_FILE", "airports-extended.dat"
))
COMPRESSED_DATA_FILE = Path(os.environ.get(
    "COMPRESSED_DATA_FILE", "airports-extended.csv.gz"
))
DEFAULT_FILE_SUFFIX = os.environ.get("DEFAULT_FILE_SUFFIX", ".csv")
DEFAULT_FILE_LOAD_LIMIT = os.environ.get("DEFAULT_FILE_LOAD_LIMIT", 20)

airports_url = (
    "https://raw.githubusercontent.com/jpatokal/openflights/"
    f"master/data/{DOWNLOADED_DATA_FILE}"
)


def download_and_compress_csv(
    url: str = airports_url,
    output_path: Path = DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE,
    chunk_size: int = 8192,
    compression_level: int = 9,
) -> Optional[Path]:
    """Download a CSV file from URL and store it compressed.

    Args:
        url: URL of the CSV file to download
        output_path: Path to save the compressed file
        chunk_size: Size of chunks to download (default: 8192)
        compression_level: Gzip compression level (1-9, default: 9)

    Returns:
        Path to compressed file if successful, None otherwise
    """
    try:
        # Ensure parent directory exists
        os.makedirs(output_path.parent, exist_ok=True)

        # Download the file in chunks
        with requests.get(url, stream=True) as response:
            response.raise_for_status()

            # Open gzip file for writing
            with gzip.open(
                output_path,
                "wb",
                compresslevel=compression_level,
            ) as f_out:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # filter out keep-alive chunks
                        f_out.write(chunk)

        return output_path

    except requests.exceptions.RequestException as e:
        print(f"Failed to download file: {e}")
        return None
    except Exception as e:
        print(f"Error compressing file: {e}")
        return None


def load_data(
    url: str = airports_url,
    file_suffix: str = DEFAULT_FILE_SUFFIX,
    limit: int = DEFAULT_FILE_LOAD_LIMIT,
    *args,
    **kwargs,
) -> pl.LazyFrame:
    """Load station data from the extracted files.

    Args:
        file_suffix: Suffix to filter files in the tar archive (default: .csv)
        limit: Limit the number of files to process (default: None)

    Returns:
        pl.LazyFrame: Concatenated lazyframe of all matching files
    """
    logger.info("Loading station data")
    if not (DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE).exists():
        logger.warning("Data directory not found, downloading data")
        download_and_compress_csv(url)

    with gzip.open(DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE, "rb") as f:
        return pl.read_csv(
            f,
            null_values=["\\N"],
            has_header=False,
            new_columns=[
                "id",
                "name",
                "city",
                "country",
                "iata",
                "icao",
                "latitude",
                "longitude",
                "altitude",
                "timezone",
                "dst",
                "tz_database_time_zone",
                "type",
                "source",
            ],
            *args,
            **kwargs
        ).lazy()
