import tempfile
from typing import Optional
import os
import logging

import altair as alt
import polars as pl
import streamlit as st


logging.basicConfig(level=logging.DEBUG)
DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10000))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 500))
DEFAULT_NON_SELECTED_OPACITY = float(
    os.getenv("DEFAULT_NON_SELECTED_OPACITY", "0.2")
)


def render(
    data: pl.LazyFrame,
    selected_x: str,
    selected_y: str,
    sample_rows: Optional[int] = None
):
    logging.debug("Starting render function for line_data_all")
    columns = data.collect_schema().names()

    if selected_x == "index" and "index" not in columns:
        logging.debug("Adding index column as x-axis")
        data = data.with_columns(
            pl.arange(0, pl.count()).alias("index")
        )
    else:
        logging.debug(f"Sorting data by selected x-axis: {selected_x}")
        data = data.sort(selected_x)

    data = data.collect()
    if sample_rows and len(data) * len(data.columns) > sample_rows:
        data = data.sample(sample_rows)
    data = data.to_pandas()

    try:
        logging.debug("Creating multi-line chart")

        chart = alt.Chart(
            data,
        ).mark_line().encode(
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
        )

        concated_charts = []
        color_scheme = [
            '#1f77b4',
            '#ff7f0e',
            '#2ca02c',
            '#d62728',
            '#9467bd',
            '#8c564b',
            '#e377c2',
            '#7f7f7f',
            '#bcbd22',
        ]

        for i, column in enumerate(data.columns):
            if column == selected_x or column == selected_y:
                continue

            logging.debug(f"Adding column {column} to chart")
            try:
                concated_charts.append(
                    alt.Chart(data).mark_line(opacity=0.1).encode(
                        x=alt.X(selected_x, axis=alt.Axis(
                            labelColor='white',
                            domainOpacity=0,
                            gridOpacity=0,
                            domain=False,
                        )),
                        y=alt.Y(column, axis=alt.Axis(
                            labelColor='white',
                            domainOpacity=0,
                            gridOpacity=0.2,
                            domain=False,
                        )),
                        tooltip=[selected_x, column],
                        color=alt.value(color_scheme[i % len(color_scheme)])
                    )
                )
            except Exception as e:
                logging.exception(f"Error adding column {column} to chart")
                st.error(f"Error adding column {column} to chart: {e}")

        chart = alt.layer(chart, *concated_charts)\
            .properties(
                height=DEFAULT_PLOT_HEIGHT,
                width=DEFAULT_PLOT_WIDTH,
                background='transparent'
            ).configure_view(
                strokeOpacity=0
            ).configure_axisLeft(
                labels=True,
                ticks=True
            ).configure_axisRight(
                labels=False,
                ticks=False
            )

        if len(data) * (len(concated_charts) + 1) <= DEFAULT_MAX_ROWS:
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
