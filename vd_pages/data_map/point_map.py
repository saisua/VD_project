from typing import Optional
import altair as alt
import polars as pl
import streamlit as st
import tempfile

from vd_pages.countries import map_projections

from .base_map import BaseMap, DEFAULT_MAX_ROWS, DEFAULT_PLOT_HEIGHT


class PointMap(BaseMap):
    def render(self, sample_rows: Optional[int] = DEFAULT_MAX_ROWS):
        data = self.load_data()
        if isinstance(data, tuple):
            data = data[0]

        col1, col2, col3 = st.columns(3)
        with col1:
            lat_col = st.selectbox(
                "Select latitude column",
                options=self.numeric_columns,
                index=(
                    self.numeric_columns.index('latitude')
                    if 'latitude' in self.numeric_columns
                    else None
                )
            )
        with col2:
            lon_col = st.selectbox(
                "Select longitude column",
                options=self.numeric_columns,
                index=(
                    self.numeric_columns.index('longitude')
                    if 'longitude' in self.numeric_columns
                    else None
                )
            )
        with col3:
            selected_metric = st.selectbox(
                "Select a metric to visualize",
                self.columns,
            )

        map_type = st.selectbox(
            "Select map",
            ["Natural Earth", "Globe", *map_projections.keys()],
        )

        background = self.create_background(map_type)

        if lat_col and lon_col:
            if selected_metric:
                color = alt.Color(
                    f'{selected_metric}:Q',
                    scale=alt.Scale(scheme='reds'),
                    legend=alt.Legend(title=selected_metric)
                )
                data = data.select(
                    pl.col(selected_metric),
                    pl.col(lat_col),
                    pl.col(lon_col),
                )
            else:
                color = alt.value('lightgray')
                data = data.select(pl.col(lat_col), pl.col(lon_col))

            if self.date_columns:
                date_col = st.selectbox(
                    "Select a date column",
                    options=self.date_columns,
                    index=0,
                )
                data = data.filter(pl.col(date_col) == date_col.max())

            data = data.collect()
            if sample_rows and len(data) > sample_rows:
                data = data.sample(sample_rows)
            data = data.to_pandas()

            points = alt.Chart(data).mark_circle().encode(
                longitude='longitude:Q',
                latitude='latitude:Q',
                size=alt.value(100),
                color=color,
            )

            chart = (background + points).properties(
                height=DEFAULT_PLOT_HEIGHT,
            )

            if len(data) <= DEFAULT_MAX_ROWS:
                chart = chart.interactive()
                st.altair_chart(chart, use_container_width=True)
                st.write("Interactive map of climate data.")
            else:
                with tempfile.NamedTemporaryFile(suffix='.svg') as file:
                    chart.save(
                        file.name,
                        format='svg',
                        height=DEFAULT_PLOT_HEIGHT,
                        width=int(DEFAULT_PLOT_HEIGHT * 1.5),
                    )
                    st.image(file.name, use_container_width=False)
        else:
            chart = background.interactive()
            st.altair_chart(chart, use_container_width=True)
            st.write("Interactive map of climate data.")
