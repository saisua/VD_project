import logging
import os

import streamlit as st

from vd_pages.data_map.point_map import PointMap
from vd_pages.data_map.choropleth_map import ChoroplethMap


logging.basicConfig(level=logging.INFO)


DEFAULT_MAX_ROWS = int(os.environ.get("DEFAULT_MAX_ROWS", 10000))
DEFAULT_PLOT_HEIGHT = int(os.environ.get("DEFAULT_PLOT_HEIGHT", 500))


def render():
    st.header("Geospatial Visualization")

    # Add a radio button to select map type
    map_mode = st.selectbox(
        "Select Map Mode",
        options=["Point Map", "Choropleth Map"],
        index=0
    )

    with st.spinner('Generating visualization...'):
        if map_mode == "Point Map":
            st.subheader("Point Map Visualization")
            point_map = PointMap()
            point_map.render()
        else:
            st.subheader("Choropleth Map Visualization")
            choropleth_map = ChoroplethMap()
            choropleth_map.render()


if __name__ == "__main__":
    st.set_page_config(
        page_title="NOAA Data Analysis - Geospatial Visualization",
        layout="wide"
    )
    render()
