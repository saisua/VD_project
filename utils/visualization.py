import plotly.express as px
import pandas as pd


def plot_choropleth(data: pd.DataFrame) -> None:
    """Plot an interactive Choropleth map."""
    # Placeholder for actual implementation
    fig = px.choropleth(data, locations="country", color="value")
    fig.show()


def plot_line(data: pd.DataFrame, x: str, y: str) -> None:
    """Plot an interactive line chart."""
    fig = px.line(data, x=x, y=y)
    fig.show()
