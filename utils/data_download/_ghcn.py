import os
from pathlib import Path
from typing import Optional
import tarfile
import logging

import requests
import polars as pl


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DOWNLOADED_DATA_FILE = Path(os.environ.get(
    "DOWNLOADED_DATA_FILE", "daily-summaries-latest.tar.gz"
))
COMPRESSED_DATA_FILE = Path(os.environ.get(
    "COMPRESSED_DATA_FILE", "ghcnd-data.tar.gz"
))
DEFAULT_FILE_SUFFIX = os.environ.get("DEFAULT_FILE_SUFFIX", ".csv")
DEFAULT_FILE_LOAD_LIMIT = os.environ.get("DEFAULT_FILE_LOAD_LIMIT", 1)

ghcn_url = (
    "https://www.ncei.noaa.gov/data/"
    "global-historical-climatology-network-daily/"
    f"archive/{COMPRESSED_DATA_FILE}"
)


def download_data(url: str = ghcn_url) -> str:
    """
    Download the GHCN daily dataset and
    return the path to the downloaded file.
    """
    logger.info("Starting GHCN dataset download")
    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
    output_path = DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE

    try:
        logger.info(f"Downloading from {url} to {output_path}")
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info("Download completed successfully")
        return output_path
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download GHCN dataset: {e}")
        raise Exception(f"Failed to download GHCN dataset: {e}")


def load_data(
    url: str = ghcn_url,
    filter_prefix: Optional[str] = 'ES',
    file_suffix: str = DEFAULT_FILE_SUFFIX,
    limit: int = DEFAULT_FILE_LOAD_LIMIT,
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
        download_data(url)

    if limit is not None:
        limit = int(limit)

    try:
        logger.info(f"Reading data from {COMPRESSED_DATA_FILE}")
        tar_path = DEFAULT_DATA_DIR / COMPRESSED_DATA_FILE
        lazy_frames = []
        seen_prefixes = set()

        with tarfile.open(tar_path, "r:gz") as tar:
            for member in tar:
                if limit and len(lazy_frames) >= limit:
                    break
                if (
                    member.isfile()
                    and member.name.endswith(file_suffix)
                    and (
                        filter_prefix is None
                        or member.name.startswith(filter_prefix)
                    )
                ):
                    file_prefix = member.name[:2]
                    if file_prefix not in seen_prefixes:
                        seen_prefixes.add(file_prefix)
                        logger.info(f"Processing file: {member.name}")
                        with tar.extractfile(member) as f:
                            logger.info("Reading CSV data")
                            df = pl.read_csv(f.read())
                            lazy_frames.append(df.lazy())
                            logger.info(
                                f"Loaded {len(df)} records"
                                f"from {member.name}"
                            )

        if lazy_frames:
            if len(lazy_frames) > 1:
                logger.info(f"Concatenating {len(lazy_frames)} dataframes")
                return pl.concat(lazy_frames, how='diagonal_relaxed')
            else:
                logger.info("Returning single dataframe")
                return lazy_frames[0]

        logger.warning(f"No valid {file_suffix} files found in archive")
        return pl.LazyFrame()
    except Exception as e:
        logger.error(f"Failed to load station data: {e}")
        raise Exception(f"Failed to load station data: {e}")
