from typing import Optional
import logging

import streamlit as st

from utils.data_loader import load_data
from vd_pages.data_analysis.line_data_all import render as render_line_data_all
from vd_pages.data_analysis.line_data import render as render_line_data_single
from vd_pages.data_analysis.violin_plot import render as render_violin_plot
from vd_pages.data_analysis.pairplot import render as render_pairplot
from vd_pages.data_analysis.barplot import render as render_barplot
from vd_pages.data_analysis.scatterplot import render as render_scatterplot
from vd_pages.data_analysis.contour_plot import render as render_contour_plot
from vd_pages.data_analysis.correlation_heatmap import (
    render as render_correlation_heatmap,
)


DEFAULT_VISUALIZATION_AXIS_SELECTIONS = {
    "Line Chart": 2,
    "Violin Plot": 1,
    "Pairplot": 0,
    "Barplot": 2,
    "Scatterplot": 2,
    "Contour Plot": 3,
    "Correlation Heatmap": 0,
}
DEFAULT_AXIS_NAMES = ("x", "y", "z")


def render(sample_rows: Optional[int] = None):
    """
    This should've been a class that loads the data analysis plots
    dynamically like the data map page, but I was aiming for something
    more simple and quick to implement at first.
    """
    # No set page config here, it's done in the app.py file
    logging.debug("Starting Data Analysis page render")

    st.header("Data Analysis")

    with st.spinner('Processing data...'):
        logging.debug("Loading configuration section")
        st.subheader("Configuration")

        # Load actual data
        logging.debug("Loading station data")
        data = load_data()
        if isinstance(data, tuple):
            data = data[0]

        columns = data.collect_schema().names()

        # Visualization type selection
        col1, col2 = st.columns(2)
        with col1:
            plot_type = st.selectbox(
                "Select visualization type",
                [
                    "Line Chart",
                    "Violin Plot",
                    "Pairplot",
                    "Barplot",
                    "Scatterplot",
                    "Contour Plot",
                    "Correlation Heatmap"
                ],
            )
            logging.info(f"Selected plot type: {plot_type}")
        with col2:
            num_samples = st.number_input(
                "Max samples",
                0,
                value=10_000,
                step=1000,
            )
            if num_samples != 0:
                logging.info(f"Sampling {num_samples} max samples")

        num_axes = DEFAULT_VISUALIZATION_AXIS_SELECTIONS[plot_type]

        # Place x and y selectors in the same row
        selected_axes = []
        if num_axes:
            cols = st.columns(num_axes)
            for i, col in enumerate(cols):
                if num_axes > 1 and i == 0:
                    options = ["index"] + columns
                else:
                    options = columns

                with col:
                    selected_axis = st.selectbox(
                        "Select a column for the "
                        f"{DEFAULT_AXIS_NAMES[i]} axis",
                        options,
                        index=None,
                    )
                    logging.info(
                        f"Selected {DEFAULT_AXIS_NAMES[i]}-axis:"
                        f" {selected_axis}"
                    )
                    selected_axes.append(selected_axis)

        # TODO: Add the option to switch from cloropleth to
        # data_analysis and back

        if len(list(filter(None, selected_axes))) >= min(num_axes, 2):
            # Toggle for showing all data
            if plot_type == "Line Chart":
                col1, col2 = st.columns(2)

                with col1:
                    show_all = st.checkbox(
                        "Show all data (independent y-scales)",
                        value=False
                    )
                with col2:
                    fit_p = st.checkbox(  # noqa: 841
                        "Fit p line",
                        value=False,
                        disabled=show_all,
                    )
            else:
                show_all = False

            logging.info(f"Show all data option: {show_all}")

            # Altair graph
            logging.debug("Rendering data visualization section")
            st.subheader("Data Visualization")

            if plot_type == "Line Chart":
                if show_all:
                    logging.debug("Rendering all data visualization")
                    render_line_data_all(
                        data,
                        *selected_axes,
                        sample_rows=num_samples,
                    )
                else:
                    render_line_data_single(
                        data,
                        *selected_axes,
                        sample_rows=num_samples,
                        fit_p=fit_p,
                    )
            elif plot_type == "Violin Plot":
                render_violin_plot(
                    data,
                    *selected_axes,
                    sample_rows=num_samples,
                )
            elif plot_type == "Pairplot":
                render_pairplot(
                    data,
                    *selected_axes,
                    sample_rows=num_samples,
                )
            elif plot_type == "Barplot":
                render_barplot(
                    data,
                    *selected_axes,
                    sample_rows=num_samples,
                )
            elif plot_type == "Scatterplot":
                render_scatterplot(
                    data,
                    *selected_axes,
                    sample_rows=num_samples,
                )
            elif plot_type == "Contour Plot":
                render_contour_plot(
                    data,
                    *selected_axes,
                    sample_rows=num_samples,
                )
            elif plot_type == "Correlation Heatmap":
                render_correlation_heatmap(
                    data,
                    *selected_axes,
                )
        else:
            st.warning("Please select the necessary axes for the plot")

        # Data table with pagination
        logging.debug("Displaying raw data table with pagination")
        st.subheader("Raw Data")

        # Create columns for pagination controls
        col1, col2 = st.columns([3, 1])

        with col1:
            page_size = st.selectbox(
                "Rows per page",
                options=[10, 25, 50, 100],
                index=2,  # Default to 50
                key="page_size"
            )

        with col2:
            page_number = st.number_input(
                "Page",
                min_value=1,
                value=1,
                step=1,
                key="page_number"
            )

        # Calculate slice for current page
        start_idx = (page_number - 1) * page_size

        # Display the paginated data
        st.dataframe(data.slice(start_idx, page_size).collect())

        logging.debug("Data Analysis page render completed")


if __name__ == "__main__":
    st.set_page_config(
        page_title="NOAA Data Analysis - Data Analysis",
        layout="wide"
    )
    render()
