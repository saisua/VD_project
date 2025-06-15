import polars as pl
import geopandas as gpd


def transform_data(
    literacy_df: pl.LazyFrame,
    gdp_df: pl.LazyFrame,
    ischools_df: pl.LazyFrame,
    median_age_df: pl.LazyFrame,
    population_density_df: pl.LazyFrame,
    mental_health_df: pl.LazyFrame,
    world_gdf: gpd.GeoDataFrame,
) -> pl.LazyFrame:
    literacy_df = literacy_df.with_columns(
        pl.col("Entity").cast(pl.Categorical).alias("Country"),
        pl.col("Code").cast(pl.Categorical),
        pl.col("Year").cast(pl.Int16),
        pl.col("Literacy rate").alias("Literacy"),
    ).drop("Literacy rate", "Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    gdp_df = gdp_df \
        .drop('900793-annotations') \
        .with_columns(
            pl.col("Entity").cast(pl.Categorical).alias("Country"),
            pl.col("Code").cast(pl.Categorical),
            pl.col("Year").cast(pl.Int16),
            pl.col("GDP per capita").alias("GDP"),
        ).drop("GDP per capita", "Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    internet_perc_col_name = (
        "4.a.1 - Proportion of schools with access to the internet "
        "for pedagogical purposes, by "
        "education level (%) - SE_ACS_INTNT - Primary"
    )
    ischools_df = ischools_df.with_columns(
        pl.col("Entity").cast(pl.Categorical).alias("Country"),
        pl.col("Code").cast(pl.Categorical),
        pl.col("Year").cast(pl.Int16),
        pl.col(internet_perc_col_name).alias("Internet%"),
    ).drop(internet_perc_col_name, "Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    median_estimates_col_name = (
        "Median age - Sex: all - Age: all - Variant: estimates"
    )
    median_medium_col_name = (
        "Median age - Sex: all - Age: all - Variant: medium"
    )
    median_age_df = median_age_df.with_columns(
        pl.col("Entity").cast(pl.Categorical).alias("Country"),
        pl.col("Code").cast(pl.Categorical),
        pl.col("Year").cast(pl.Int16),
        pl.col(median_estimates_col_name).alias("Median Age Estimates"),
        pl.col(median_medium_col_name).alias("Median Age Medium"),
    ).drop(median_estimates_col_name, median_medium_col_name, "Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    population_density_df = population_density_df.with_columns(
        pl.col("Entity").cast(pl.Categorical).alias("Country"),
        pl.col("Code").cast(pl.Categorical),
        pl.col("Year").cast(pl.Int16),
        pl.col("Population density"),
    ).drop("Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    death_rate_col_name = (
        "Death rate from mental and "
        "substance use disorders among both sexes"
    )
    mental_health_df = mental_health_df.with_columns(
        pl.col("Entity").cast(pl.Categorical).alias("Country"),
        pl.col("Code").cast(pl.Categorical),
        pl.col("Year").cast(pl.Int16),
        pl.col(death_rate_col_name).alias("Death Rate"),
    ).drop(death_rate_col_name, "Entity") \
        .filter(
            (1950 < pl.col("Year"))
            &
            (pl.col("Year") < 2025)
        )

    # Create a list of all dataframes to join
    dfs_to_join = [
        literacy_df,
        gdp_df,
        ischools_df,
        median_age_df,
        population_density_df,
        mental_health_df
    ]

    merged_df = dfs_to_join[0]

    for df in dfs_to_join[1:]:
        merged_df = merged_df.join(
            df,
            on=["Country", "Code", "Year"],
            how="full",
            coalesce=True,
        )

    if world_gdf is not None:
        data_codes = set(filter(
            None,
            merged_df
            .select(pl.col('Code'))
            .unique()
            .collect()
            .to_series()
            .to_list()
        ))

        world_a3 = set(filter(
            None,
            world_gdf['SOV_A3'].unique()
        ))

        no_match_a3 = world_a3 - data_codes
        no_match_codes = data_codes - world_a3

        code_map = {
            "FR1": "FRA",
            "CU1": "CUB",
        }

        for no_a3 in filter(
            lambda x: (
                x[-1].isdigit() and
                x not in code_map
            ),
            no_match_a3
        ):
            no_match_a3_2 = no_a3[:2]
            options = []
            for no_match_code in no_match_codes:
                if no_match_code.startswith(no_match_a3_2):
                    options.append(no_match_code)
            if len(options) == 1:
                code_map[no_a3] = options[0]
                print(no_a3, '->', options[0])
            else:
                print(no_a3, '?', options)

        world_gdf['SOV_A3'] = world_gdf['SOV_A3'].replace(code_map)

    return (
        merged_df.drop_nulls(subset=["Country", "Code", "Year"]),
        world_gdf,
    )
