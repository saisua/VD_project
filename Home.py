import logging

import streamlit as st

from vd_pages.Map import render as render_map
from vd_pages.Data_Analysis import render as render_data_analysis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    st.set_page_config(
        page_title="Visualización de Datos",
        layout="wide",
    )

    # Top menu for navigation
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Go to",
        ["Home", "Map", "Data Analysis"],
        index=0,
        help="Select a page to view different data visualizations"
    )

    # Author info in sidebar
    st.sidebar.markdown("---")
    st.sidebar.subheader("Visualización de Datos")
    st.sidebar.markdown("**Author:** Ausiàs Prieto Roig")
    st.sidebar.markdown("**University:** Universitat Politècnica de València")

    if page == "Home":
        st.title("NOAA Climate Data Explorer")
        st.markdown("""
## Welcome to the Data Analysis Dashboard

This interactive dashboard allows you to explore and visualize
any data in tabular form.

### Features:
- **Map Visualization**: Explore data geographically
- **Data Analysis**: Various visualization tools including:
    - Line charts
    - Violin plots
    - Scatter plots
    - Correlation heatmaps
    - Pairplots
    - Barplots
    - Contour plots

### How to use:
1. Select a page from the navigation sidebar
2. Choose your visualization type
3. Customize the parameters
4. Explore the data
        """)

    elif page == "Map":
        render_map()
    elif page == "Data Analysis":
        render_data_analysis()


if __name__ == "__main__":
    main()
