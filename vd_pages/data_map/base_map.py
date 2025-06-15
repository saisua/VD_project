import logging
import os

import altair as alt
import polars as pl
from vega_datasets import data as vega_data

from utils.data_loader import load_data
from vd_pages.countries import map_projections

logging.basicConfig(level=logging.INFO)

DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10000))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 1000))


class BaseMap:
    def __init__(self):
        self.world = alt.topo_feature(vega_data.world_110m.url, 'countries')
        self.background = None
        self.data = None
        self.numeric_columns = []
        self.date_columns = []
        self.columns = []

    def load_data(self):
        data = load_data()
        if isinstance(data, tuple):
            data, world_gdf = data
        else:
            world_gdf = None

        self.columns = data.collect_schema().names()
        self.numeric_columns = data.select(
            pl.selectors.numeric() | pl.col('Code')
        ).collect_schema().names()
        self.date_columns = data.select(
            pl.selectors.date() | pl.selectors.datetime()
        ).collect_schema().names()

        return data, world_gdf

    def project_background(self, chart: alt.Chart, map_type: str):
        if map_type == "Natural Earth":
            chart = chart.project('naturalEarth1')
        elif map_type == "Globe":
            chart = chart.project('orthographic')
        else:
            projection = map_projections[map_type]
            chart = chart.project(
                'mercator',
                center=projection.center,
                scale=projection.scale,
            )
        return chart

    def create_background(
        self,
        map_type: str,
        height: int = DEFAULT_PLOT_HEIGHT,
    ):
        background = alt.Chart(self.world).mark_geoshape(
            fill='lightgray',
            stroke='white'
        ).properties(
            width=height,
            height=height
        )

        background = self.project_background(background, map_type)

        self.background = background
        return background
