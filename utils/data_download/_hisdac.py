import os
from pathlib import Path
from typing import Optional
import zipfile
import logging

import requests
import polars as pl
import geopandas as gpd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
DOWNLOADED_DATA_FILE = Path(os.environ.get(
    "DOWNLOADED_DATA_FILE", "hisdac-data.zip"
))
DEFAULT_FILE_SUFFIX = os.environ.get("DEFAULT_FILE_SUFFIX", ".gpkg")
DEFAULT_FILE_LOAD_LIMIT = os.environ.get("DEFAULT_FILE_LOAD_LIMIT", 10)

HISDAC_URL = "https://figshare.com/ndownloader/files/42033807"


def download_data(url: str = HISDAC_URL) -> str:
    """
    Download the HISDAC dataset and return the path to the downloaded file.
    """
    logger.info("Starting HISDAC dataset download")
    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)
    output_path = DEFAULT_DATA_DIR / DOWNLOADED_DATA_FILE

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
        logger.error(f"Failed to download HISDAC dataset: {e}")
        raise Exception(f"Failed to download HISDAC dataset: {e}")


def load_data(
    url: str = HISDAC_URL,
    filter_suffix: Optional[str] = None,
    filter_contains: Optional[str] = "wideformat",
    limit: int = DEFAULT_FILE_LOAD_LIMIT,
) -> pl.LazyFrame:
    """Load data from the ZIP file directly into a polars dataframe.

    Args:
        filter_suffix: Suffix to filter files in
        the ZIP archive (default: .csv)
        limit: Limit the number of files to process (default: 1)

    Returns:
        pl.LazyFrame: Concatenated lazyframe of all matching files
    """
    logger.info("Loading HISDAC data")
    if not (DEFAULT_DATA_DIR / DOWNLOADED_DATA_FILE).exists():
        logger.warning("Data directory not found, downloading data")
        download_data(url)

    if limit is not None:
        limit = int(limit)

    try:
        logger.info(f"Reading data from {DOWNLOADED_DATA_FILE}")
        zip_path = DEFAULT_DATA_DIR / DOWNLOADED_DATA_FILE
        gpkg_lazy_frames = []
        csv_lazy_frames = []

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            for member in zip_ref.namelist():
                if limit and (
                    len(gpkg_lazy_frames) >= limit
                    or len(csv_lazy_frames) >= limit
                ):
                    break
                if (
                    (filter_suffix is None or member.endswith(filter_suffix))
                    and (filter_contains is None or filter_contains in member)
                ):
                    logger.info(f"Processing file: {member}")
                    with zip_ref.open(member) as f:
                        if member.endswith('.gpkg'):
                            logger.info("Reading GeoPackage data")
                            gdf = gpd.read_file(f)
                            df = pl.from_pandas(
                                gdf.drop(columns=["geometry"])
                            ).with_columns(
                                pl.Series("geometry", gdf["geometry"].tolist())
                            )
                            gpkg_lazy_frames.append(df.lazy())
                        elif member.endswith('.csv'):
                            logger.info("Reading CSV data")
                            df = pl.read_csv(
                                f,
                                null_values=["NA", "NaN", "nan"],
                                infer_schema_length=100_000,
                            )
                            csv_lazy_frames.append(df.lazy())
                        else:
                            logger.warning(f"Unsupported file type: {member}")
                            continue
                        logger.info(f"Loaded {len(df)} records from {member}")

        if gpkg_lazy_frames:
            if len(gpkg_lazy_frames) > 1:
                logger.info(
                    f"Concatenating {len(gpkg_lazy_frames)} dataframes"
                )
                gpkg_lazy_frames = pl.concat(
                    gpkg_lazy_frames,
                    how='diagonal_relaxed'
                )
            else:
                logger.info("Returning single dataframe")
                gpkg_lazy_frames = gpkg_lazy_frames[0]
            any_gpkg_frames = True
        else:
            any_gpkg_frames = False

        if csv_lazy_frames:
            if len(csv_lazy_frames) > 1:
                logger.info(f"Concatenating {len(csv_lazy_frames)} dataframes")
                csv_lazy_frames = pl.concat(
                    csv_lazy_frames,
                    how='diagonal_relaxed'
                )
            else:
                logger.info("Returning single dataframe")
                csv_lazy_frames = csv_lazy_frames[0]
            any_csv_frames = True
        else:
            any_csv_frames = False

        if any_gpkg_frames and any_csv_frames:
            logger.info("Joining GeoPackage and CSV data on NATCODE")
            return gpkg_lazy_frames.join(
                csv_lazy_frames,
                on="NATCODE",
                how="full"
            )
        elif any_gpkg_frames:
            return gpkg_lazy_frames
        elif any_csv_frames:
            return csv_lazy_frames
        else:
            logger.warning(f"No valid {filter_suffix} files found in archive")
            return pl.LazyFrame()
    except Exception as e:
        logger.error(f"Failed to load HISDAC data: {e}")
        raise Exception(f"Failed to load HISDAC data: {e}")


if __name__ == "__main__":
    download_data()
