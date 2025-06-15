import os
from pathlib import Path
import logging
import re
from typing import List, Optional, Tuple

from joblib import Parallel, delayed, parallel_config

import osmnx as ox

import networkx as nx

import polars as pl


logger = logging.getLogger(__name__)

DEFAULT_DATA_DIR = Path(os.environ.get("DATA_DIR", "data"))
CHUNKS_DIR = Path(os.environ.get("OSM_CHUNKS_DIR", "osm_chunks"))
BUILDINGS_FILE = Path(os.environ.get("BUILDINGS_FILE", "buildings.parquet"))
ROADS_FILE = Path(os.environ.get("ROADS_FILE", "roads.parquet"))
CHUNK_SIZE = float(os.environ.get("OSM_CHUNK_SIZE", 0.2))  # degrees


def _get_area_bbox(place: str) -> Tuple[float, float, float, float]:
    """Get bounding box for a place name"""
    area = ox.geocode_to_gdf(place)
    return area.total_bounds


def _create_bbox_chunks(
    bbox: Tuple[float, float, float, float],
    chunk_size: float = CHUNK_SIZE
) -> List[Tuple[float, float, float, float]]:
    """Divide a bounding box into smaller chunks"""
    minx, miny, maxx, maxy = bbox
    chunks = []
    x = minx
    while x < maxx:
        y = miny
        while y < maxy:
            chunks.append(
                (
                    x,
                    y,
                    min(x + chunk_size, maxx),
                    min(y + chunk_size, maxy)
                )
            )
            y += chunk_size
        x += chunk_size
    return chunks


not_num = re.compile(r"[^0-9]")


def _keep_max(maxspeed):
    if isinstance(maxspeed, (list, tuple)):
        return max(map(float, maxspeed))
    if isinstance(maxspeed, str):
        return float(list(filter(None, not_num.split(maxspeed)))[0])
    return float(maxspeed)


# def _keep_max_width(width):
#     if isinstance(width, (list, tuple)):
#         return max(map(float, width))
#     elif isinstance(width, str):
#         return max(map(float, width.split('-')))
#     return width


def _keep_first(ref):
    if isinstance(ref, (list, tuple)):
        return ref[0]
    if isinstance(ref, str):
        return (list(filter(
            None,
            not_num.split(ref)
        )) or [None])[0]
    return ref


def _keep_first_int(ref):
    if isinstance(ref, (list, tuple)):
        return ref[0]
    if isinstance(ref, str):
        filtered = list(filter(
            None,
            not_num.split(ref)
        ))
        return int(filtered[0]) if filtered else float('nan')
    return ref


def _keep_any_bool(ref):
    if isinstance(ref, (list, tuple)):
        return any(ref)
    if isinstance(ref, str):
        return (list(filter(
            None,
            not_num.split(ref)
        )) or [None])[0].strip().lower() == 'true'
    return ref


def _keep_wkt(geometry):
    if isinstance(geometry, float):
        return geometry
    return geometry.wkt


def _fix_road(data):
    # if "maxspeed" in data:
    #     data.maxspeed = _keep_max(data.maxspeed)
    if "geometry" in data:
        data.geometry = _keep_wkt(data.geometry)
    if "ref" in data:
        data.ref = _keep_first(data.ref)
    if "name" in data:
        data.name = _keep_first(data.name)
    # if "width" in data:
    #     data.width = _keep_max_width(data.width)
    if "highway" in data:
        data.highway = _keep_first(data.highway)
    if "reversed" in data:
        data.reversed = _keep_any_bool(data.reversed)
    if "osmid" in data:
        data.osmid = _keep_first_int(data.osmid)
    if "lanes" in data:
        data.lanes = _keep_max(data.lanes)
    return data


def download_chunk(
    chunk_id: int,
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    network_type: str,
    buildings_chunks_dir: Path,
    roads_chunks_dir: Path,
    num_chunks: int,
):
    buildings_chunk_path = (
        buildings_chunks_dir
        /
        f"buildings_{chunk_id}.parquet"
    )
    roads_chunk_path = (
        roads_chunks_dir
        /
        f"roads_{chunk_id}.parquet"
    )

    logger.info(f"Processing chunk {chunk_id}/{num_chunks} ({chunk_id})")
    try:
        ox.features_from_bbox(
            bbox=[minx, miny, maxx, maxy],
            tags={"building": True}
        ).to_parquet(
            buildings_chunk_path,
            compression="gzip",
            compression_level=9,
        )

        roads_df = nx.to_pandas_edgelist(
            ox.graph_from_bbox(
                bbox=[minx, miny, maxx, maxy],
                network_type=network_type
            )
        )\
            .drop(columns=[
                    "width",
                    "service",
                    "access",
                    "bridge",
                    "maxspeed",
                    "tunnel",
                    "junction",
                    "oneway",
                ],
                errors="ignore"
            )\
            .apply(_fix_road, axis=1)
        roads_df.astype(
            {
                col: dtype
                for col, dtype in {
                    # "width": float,
                    "highway": str,
                    "reversed": bool,
                    "osmid": int,
                    # "maxspeed": float,
                    "name": str,
                    "ref": str,
                    "geometry": str,
                    "lanes": float,
                }.items()
                if col in roads_df.columns
            }
        ).to_parquet(
            roads_chunk_path,
            compression="gzip",
            compression_level=9,
        )

        logger.info(f"Saved buildings chunk to {buildings_chunk_path}")
        logger.info(f"Saved roads chunk to {roads_chunk_path}")
    except Exception as e:
        e = str(e)
        if not (
            e.startswith("No matching features")
            or
            e.startswith("Found no graph nodes")
        ):
            logger.warning(f"Failed chunk {chunk_id}: {e}")
            raise


def download_data(
    place: str,
    buildings_file: Path = DEFAULT_DATA_DIR / BUILDINGS_FILE,
    roads_file: Path = DEFAULT_DATA_DIR / ROADS_FILE,
    network_type: str = "all",
    chunk_size: Optional[float] = None
) -> Tuple[List[Path], Path]:
    """
    Download OSM buildings (in chunks) and roads data.
    Returns: (list_of_building_chunk_paths, roads_file_path)
    """
    logger.info(f"Downloading OSM data for {place}")
    os.makedirs(DEFAULT_DATA_DIR, exist_ok=True)

    buildings_chunks_dir = DEFAULT_DATA_DIR / CHUNKS_DIR / "buildings"
    roads_chunks_dir = DEFAULT_DATA_DIR / CHUNKS_DIR / "roads"

    os.makedirs(buildings_chunks_dir, exist_ok=True)
    os.makedirs(roads_chunks_dir, exist_ok=True)

    if chunk_size is None:
        chunk_size = CHUNK_SIZE

    try:
        # Get area bounds
        bbox = _get_area_bbox(place)
        chunks = _create_bbox_chunks(bbox, chunk_size)

        # Find existing chunks and determine starting point
        existing_building_chunks = list(
            buildings_chunks_dir.glob("buildings_*.parquet")
        )
        existing_road_chunks = list(
            roads_chunks_dir.glob("roads_*.parquet")
        )

        print(f"Downloading {len(chunks)} chunks")

        if existing_building_chunks or existing_road_chunks:
            # Get the highest existing chunk number
            building_nums = [
                int(f.stem.split('_')[1].split('.')[0])
                for f in existing_building_chunks
            ]
            road_nums = [
                int(f.stem.split('_')[1].split('.')[0])
                for f in existing_road_chunks
            ]
            start_chunk = (
                max(building_nums + road_nums) + 1
                if (building_nums or road_nums)
                else 0
            )

            print("+ Starting from chunk", start_chunk)

            # Slice chunks to start from next number
            chunks = chunks[start_chunk:]
        else:
            start_chunk = 0

        # Download buildings in chunks
        logger.info(f"Downloading buildings data in {len(chunks)} chunks")
        with parallel_config(n_jobs=10):
            Parallel()(
                delayed(download_chunk)(
                    chunk_id,
                    minx,
                    miny,
                    maxx,
                    maxy,
                    network_type,
                    buildings_chunks_dir,
                    roads_chunks_dir,
                    len(chunks),
                )
                for (
                    chunk_id,
                    (minx, miny, maxx, maxy)
                ) in enumerate(chunks, start_chunk)
            )
        return buildings_chunks_dir, roads_chunks_dir
    # Failed to download OSM data for some reason
    except Exception as e:
        logger.error(f"Failed to download OSM data: {e}")
        raise


def load_data(
    place: str = "Spain",
    data_type: str = "roads",
    force_download: bool = False,
    **kwargs
) -> pl.LazyFrame:
    """
    Load OSM data from local files.
    For buildings, returns the first chunk found (or downloads if needed).
    """
    if data_type not in ("buildings", "roads"):
        raise ValueError("data_type must be 'buildings' or 'roads'")

    if data_type == "buildings":
        buildings_chunks_dir = DEFAULT_DATA_DIR / CHUNKS_DIR / "buildings"
        existing_chunks = list(
            buildings_chunks_dir.glob("buildings_*.parquet")
        )

        if force_download or not existing_chunks:
            buildings_chunks_dir, _ = download_data(place, **kwargs)
            if buildings_chunks_dir:
                return pl.scan_parquet(buildings_chunks_dir)
            raise ValueError("No building data available")

        return pl.scan_parquet(existing_chunks)
    else:
        roads_chunks_dir = DEFAULT_DATA_DIR / CHUNKS_DIR / "roads"
        existing_chunks = list(roads_chunks_dir.glob("roads_*.parquet"))

        if force_download or not existing_chunks:
            _, roads_chunks_dir = download_data(place, **kwargs)
            if roads_chunks_dir:
                return pl.scan_parquet(roads_chunks_dir)
            raise ValueError("No road data available")

        return pl.scan_parquet(existing_chunks)
