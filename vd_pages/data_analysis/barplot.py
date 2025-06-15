import tempfile
from typing import Optional
import logging
import os

import polars as pl
import streamlit as st
import altair as alt


logging.basicConfig(level=logging.DEBUG)

DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10000))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 800))


def render(
    data: pl.LazyFrame,
    selected_x: str,
    selected_y: str,
    sample_rows: Optional[int] = None
):
    logging.debug("Starting render function for barplot")
    logging.debug(f"Selected x-axis: {selected_x}")
    logging.debug(f"Selected y-axis: {selected_y}")

    if selected_x == "index" and "index" not in data.collect_schema().names():
        logging.debug("Adding index column as x-axis")
        data = data\
            .select(pl.col(selected_y))\
            .filter(pl.col(selected_y).is_not_null())\
            .with_columns(
                pl.arange(0, pl.count()).alias("index")
            )
    else:
        logging.debug(f"Selecting and sorting by {selected_x}")
        data = data\
            .select(*map(pl.col, {selected_x, selected_y}))\
            .filter(
                pl.col(selected_x).is_not_null(),
                pl.col(selected_y).is_not_null()
            )\
            .sort(selected_x)

    data = data.collect()
    if sample_rows and len(data) > sample_rows:
        data = data.sample(sample_rows)
    data = data.to_pandas()

    try:
        logging.debug("Creating Altair chart")
        chart = alt.Chart(
            data,
            background='transparent'
        ).mark_bar().encode(
            x=alt.X(selected_x, axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0,
                domain=False,
            )),
            y=alt.Y(selected_y, axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0.2,
                domain=False,
            )),
            tooltip=[selected_x, selected_y],
            color=alt.value('#7EC0EE')
        ).properties(
            height=DEFAULT_PLOT_HEIGHT,
            width=DEFAULT_PLOT_WIDTH,
        ).configure_view(
            strokeOpacity=0
        )

        if len(data) <= DEFAULT_MAX_ROWS:
            logging.info("Displaying interactive line chart")

            st.altair_chart(chart.interactive(), use_container_width=True)
        else:
            logging.info("Dataset too large, converting to static image")

            file = tempfile.NamedTemporaryFile(suffix='.svg')
            chart.save(
                file.name,
                format='svg',
                height=DEFAULT_PLOT_HEIGHT,
                width=int(DEFAULT_PLOT_HEIGHT * 1.5)
            )

            st.image(file.name, use_container_width=False)

            file.close()

        logging.debug("Render function completed")
    except Exception as e:
        logging.exception("Plot generation failed")
        st.error(f"Plot generation failed: {e}")
