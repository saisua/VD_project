import tempfile
from typing import Optional
import logging
import os
import numpy as np
from scipy.stats import gaussian_kde

import polars as pl
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go


logging.basicConfig(level=logging.DEBUG)

DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10000))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))
DEFAULT_PLOT_WIDTH = int(os.environ.get("DEFAULT_PLOT_WIDTH", 800))


def render(
    data: pl.LazyFrame,
    selected_x: str,
    selected_y: str,
    selected_z: str = None,
    sample_rows: Optional[int] = None,
):
    logging.debug("Starting render function for contour_plot")
    logging.debug(f"Selected x-axis: {selected_x}")
    logging.debug(f"Selected y-axis: {selected_y}")
    if selected_z:
        logging.debug(f"Selected z-axis: {selected_z}")

    if selected_x == "index" and "index" not in data.collect_schema().names():
        selected_cols = {selected_y}
        if selected_z:
            selected_cols.add(selected_z)

        logging.debug("Adding index column as x-axis")
        data = data\
            .select(*map(pl.col, selected_cols))\
            .filter(
                pl.col(selected_y).is_not_null()
            )\
            .with_columns(
                pl.arange(0, pl.count()).alias("index")
            )
    else:
        logging.debug(f"Selecting and sorting by {selected_x}")
        select_cols = {selected_x, selected_y}
        if selected_z:
            select_cols.add(selected_z)

        data = data\
            .select(*map(pl.col, select_cols))\
            .filter(
                pl.col(selected_x).is_not_null(),
                pl.col(selected_y).is_not_null()
            )\
            .sort(selected_x)

    data = data.select(pl.col(pl.NUMERIC_DTYPES)).collect()
    if sample_rows and len(data) > sample_rows:
        data = data.sample(sample_rows)
    data = data.to_pandas()

    logging.debug("Creating contour plot with scatter and density "
                  f"with {len(data)} rows")

    try:
        # Compute density (z values) for contour plot
        if selected_z:
            z = data[selected_z].to_numpy()
        else:
            xy = np.vstack([data[selected_x], data[selected_y]])
            z = gaussian_kde(xy)(xy)

        # Create scatter plot with density-based coloring
        fig = px.scatter(
            data,
            x=selected_x,
            y=selected_y,
            color=z,
            opacity=0.5,
            color_continuous_scale='Viridis',
        )

        # Add density contour lines
        fig.update_traces(
            marker=dict(size=4),
            selector=dict(mode='markers')
        )

        # Add contour lines
        fig.add_trace(
            go.Contour(
                x=data[selected_x],
                y=data[selected_y],
                z=z,
                colorscale='Viridis',
                contours=dict(
                    # coloring='lines',
                    showlabels=True,
                ),
                line=dict(width=1),
                opacity=0.3,
            )
        )

        # Update layout
        fig.update_layout(
            height=DEFAULT_PLOT_HEIGHT,
            width=DEFAULT_PLOT_WIDTH,
            xaxis=dict(showgrid=True),
            yaxis=dict(showgrid=True),
        )

        if len(data) <= DEFAULT_MAX_ROWS:
            logging.info("Displaying interactive chart")
            st.plotly_chart(fig, use_container_width=True)
        else:
            logging.info("Dataset too large, converting to static image")
            file = tempfile.NamedTemporaryFile(suffix='.png')
            fig.write_image(
                file.name,
                format='png',
                height=DEFAULT_PLOT_HEIGHT,
                width=DEFAULT_PLOT_WIDTH
            )
            st.image(file.name, use_container_width=False)
            file.close()

        logging.debug("Render function completed")
    except Exception as e:
        logging.exception("Plot generation failed")
        st.error(f"Plot generation failed: {e}")
