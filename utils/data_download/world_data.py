import os
from pathlib import Path
from typing import Optional
import zipfile
import logging
import tempfile

import polars as pl

import geopandas as gpd

from utils.data_transform.world_data import transform_data


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
WORLD_DATA_FILE = Path(os.environ.get(
    "WORLD_DATA_FILE", "world_data.zip"
))
DEFAULT_FILE_SUFFIX = os.environ.get("DEFAULT_FILE_SUFFIX", ".csv")

ZIP_FILE_NAMES = {
    "cross-country-literacy-rates.zip": "literacy_df",
    "gdp-per-capita-maddison-project-database.zip": "gdp_df",
    "primary-schools-with-access-to-internet.zip": "ischools_df",
    "median-age.zip": "median_age_df",
    "population-density.zip": "population_density_df",
    "death-rate-from-mental-health-and-substance-use-disorders-who.zip": (
        "mental_health_df"
    ),
}


def load_data(
    filter_prefix: Optional[str] = None,
    file_suffix: str = DEFAULT_FILE_SUFFIX,
) -> pl.LazyFrame:
    """Load data from the extracted .csv files in the .zip archive.

    Args:
        filter_prefix: Prefix to filter files in the zip archive
            (default: None)
        file_suffix: Suffix to filter files in the zip archive
            (default: .csv)
        limit: Limit the number of files to process (default: 1)

    Returns:
        pl.LazyFrame: Concatenated lazyframe of all matching files
    """
    logger.info("Loading world data")
    if not (DEFAULT_DATA_DIR / WORLD_DATA_FILE).exists():
        logger.error(
            f"{WORLD_DATA_FILE} not found in {DEFAULT_DATA_DIR}"
        )
        raise FileNotFoundError(
            f"{WORLD_DATA_FILE} not found in {DEFAULT_DATA_DIR}"
        )

    try:
        logger.info(f"Reading data from {WORLD_DATA_FILE}")
        zip_path = DEFAULT_DATA_DIR / WORLD_DATA_FILE
        lazy_frames = dict()
        found_world_data = False

        with tempfile.TemporaryDirectory() as temp_dir:
            temp_dir = Path(temp_dir)
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                for member in map(Path, zip_ref.namelist()):
                    df_name = ZIP_FILE_NAMES.get(member.name)

                    if df_name and member.name.endswith(".zip"):
                        logger.info(f"Processing file: {member}")
                        with zip_ref.open(str(member)) as f:
                            with zipfile.ZipFile(f) as sub_zip_ref:
                                for sub_member in sub_zip_ref.namelist():
                                    if sub_member.endswith(file_suffix):
                                        logger.info(
                                            "Reading CSV data "
                                            f"from {sub_member}"
                                        )
                                        df = pl.read_csv(
                                            sub_zip_ref.open(sub_member),
                                            null_values=[
                                                'na',
                                                'NA',
                                                "NaN",
                                                "nan"
                                            ]
                                        )

                                        lazy_frames[df_name] = df.lazy()
                                        logger.info(
                                            f"Loaded {len(df)} records "
                                            f"from {sub_member}"
                                        )

                                        break
                                else:
                                    logger.warning(
                                        f"No valid {file_suffix} files "
                                        f"found in {member}"
                                    )
                    elif "ne_10m_admin_0_countries" in member.name:
                        logger.info(f"Processing file: {member}")
                        with zip_ref.open(str(member)) as f:
                            with open(temp_dir / member.name, 'wb') as f_out:
                                f_out.write(f.read())

                        found_world_data = True

            if found_world_data:
                world_gdf = gpd.read_file(
                    temp_dir / "ne_10m_admin_0_countries.shp"
                )
            else:
                world_gdf = None

        if len(lazy_frames) == 0:
            logger.warning(f"No valid {file_suffix} files found in archive")
            return pl.LazyFrame()

        return transform_data(**lazy_frames, world_gdf=world_gdf)
    except Exception as e:
        logger.error(f"Failed to load world data: {e}")
        raise Exception(f"Failed to load world data: {e}")
