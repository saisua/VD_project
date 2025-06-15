import tempfile
from typing import Optional
import logging
import os

import polars as pl
import streamlit as st
import altair as alt


logging.basicConfig(level=logging.DEBUG)
DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10_000))
DEFAULT_MAX_COLUMNS = int(os.environ.get("DEFAULT_MAX_COLUMNS", 7))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 1000))


def render(
    data: pl.LazyFrame,
    sample_rows: Optional[int] = None,
):
    logging.debug("Starting render function for pairplot")

    data = data.select(pl.col(pl.NUMERIC_DTYPES)).collect()
    if sample_rows and len(data) > sample_rows:
        data = data.sample(sample_rows)
    data = data.to_pandas()

    logging.debug("Creating pairplot")
    try:
        chart = alt.Chart(data).mark_circle().encode(
            alt.X(alt.repeat("column"), type='quantitative', axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0,
                domain=False,
            )),
            alt.Y(alt.repeat("row"), type='quantitative', axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0.2,
                domain=False,
            )),
            color=alt.value('#7EC0EE')
        ).properties(
            width=DEFAULT_PLOT_WIDTH,
            height=DEFAULT_PLOT_HEIGHT
        ).repeat(
            row=data.columns,
            column=data.columns,
            background='transparent'
        ).configure_view(
            strokeOpacity=0
        )

        if (
            len(data.columns) <= DEFAULT_MAX_COLUMNS
            and
            len(data) <= DEFAULT_MAX_ROWS
        ):
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
