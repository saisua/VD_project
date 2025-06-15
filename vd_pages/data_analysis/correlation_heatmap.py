import tempfile
import logging
import os

import polars as pl
import streamlit as st
import altair as alt
import pandas as pd


logging.basicConfig(level=logging.DEBUG)

DEFAULT_MAX_COLUMNS = int(os.environ.get("DEFAULT_MAX_COLUMNS", 7))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 800))


def render(data: pl.LazyFrame):
    logging.debug("Starting render function for correlation_heatmap")

    logging.debug("Calculating correlation matrix")
    data = data.select(pl.selectors.numeric())
    data: pd.DataFrame = data.collect().to_pandas()

    corr_matrix = pl.DataFrame(
        data.astype(float, errors='ignore').corr()
    )

    corr_matrix = corr_matrix.with_columns(
        level_0=pl.Series(
            "level_0",
            corr_matrix.columns,
        ),
    )

    corr_matrix = corr_matrix\
        .unpivot(
            index='level_0',
            variable_name='level_1',
            value_name='correlation'
        )\
        .to_pandas()

    logging.debug("Creating correlation heatmap")
    try:
        chart = alt.Chart(
            corr_matrix,
            background='transparent'
        ).mark_rect().encode(
            x=alt.X('level_0:N', axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0,
                domain=False,
            )),
            y=alt.Y('level_1:N', axis=alt.Axis(
                labelColor='white',
                domainOpacity=0,
                gridOpacity=0.2,
                domain=False,
            )),
            color=alt.Color('correlation:Q', scale=alt.Scale(scheme='blues')),
            tooltip=['level_0', 'level_1', 'correlation']
        ).properties(
            height=DEFAULT_PLOT_HEIGHT,
            width=DEFAULT_PLOT_WIDTH,
        ).configure_view(
            strokeOpacity=0
        )

        if len(corr_matrix.columns) <= DEFAULT_MAX_COLUMNS:
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
