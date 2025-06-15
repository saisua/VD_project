import tempfile
from typing import Optional
import logging
# import tempfile

import streamlit as st
import altair as alt
import polars as pl
import geopandas as gpd
import pyarrow as pa
import pandas as pd

from .base_map import (
    BaseMap,
    DEFAULT_PLOT_HEIGHT,
    # DEFAULT_MAX_ROWS,
    DEFAULT_PLOT_WIDTH,
)

from vd_pages.countries import map_projections


class ChoroplethMap(BaseMap):
    def render(self, sample_rows: Optional[int] = None):
        logging.info("Starting choropleth map rendering")
        data = self.load_data()
        if isinstance(data, tuple):
            data, world_gdf = data
        else:
            world_gdf = None

        if world_gdf is None:
            st.error("Data does not contain world data")
            return

        logging.debug(f"Loaded data with schema: {data.collect_schema()}")

        # Metric selection
        selected_metric = st.selectbox(
            "Select metric to visualize",
            self.numeric_columns,
            index=None
        )
        logging.info(f"Selected metric: {selected_metric}")

        # Map type selection
        map_type = st.selectbox(
            "Select map",
            ["Natural Earth", "Globe", *map_projections.keys()],
        )
        logging.info(f"Selected map type: {map_type}")

        chart = self.create_background(map_type)\
            .properties(
                height=DEFAULT_PLOT_HEIGHT,
                width=DEFAULT_PLOT_WIDTH,
            )

        if selected_metric:
            data = data.drop_nulls(selected_metric)

            all_years = data.select(
                pl.col("Year").unique().sort()
            ).collect().to_series().to_list()

            if len(all_years) > 1:
                year = st.select_slider(
                    "Select year",
                    options=all_years,
                    value=all_years[-1],
                )
                data = data.filter(pl.col("Year") == year)
                logging.info(f"Selected year: {year}")
            else:
                year = None

            df = data.select(
                pl.col(selected_metric),
                pl.col("Code"),
                pl.col("Country"),
            )

            merged_gdf: gpd.GeoDataFrame = world_gdf.merge(
                df.collect().to_pandas(),
                left_on='SOV_A3',
                right_on='Code',
                how='left'
            )

            merged_gdf.assign(
                geometry=merged_gdf['geometry'].to_numpy()
            )
            merged_gdf.dropna(
                how='any',
                inplace=True,
                subset=['Country', 'geometry']
            )
            merged_gdf.dropna(
                how='any',
                inplace=True,
                axis='columns'
            )

            if map_type in map_projections:
                title = f"{selected_metric} ({map_type}, {year})"
            else:
                title = f"{selected_metric} ({year})"

            choropleth = alt.Chart(
                merged_gdf,
            )\
                .mark_geoshape()\
                .encode(
                    color=f'{selected_metric}:Q',
                    tooltip=['Country:N', f'{selected_metric}:Q']
                )\
                .properties(
                    height=DEFAULT_PLOT_HEIGHT,
                    width=DEFAULT_PLOT_WIDTH,
                    title=alt.TitleParams(
                        text=title,
                        align='center',
                        color='white'
                    )
                )

            chart = chart + self.project_background(choropleth, map_type)

            chart = chart.properties(
                height=DEFAULT_PLOT_HEIGHT,
                width=DEFAULT_PLOT_WIDTH,
                background='transparent',
            )

            with tempfile.NamedTemporaryFile(suffix='.svg') as file:
                chart.save(
                    file.name,
                    format='svg',
                    height=DEFAULT_PLOT_HEIGHT,
                    width=int(DEFAULT_PLOT_HEIGHT * 1.5),
                )
                st.image(file.name, use_container_width=False)

            logging.debug("Added metric encoding to choropleth")
        else:
            st.altair_chart(chart)

        st.write("Choropleth map visualization")
