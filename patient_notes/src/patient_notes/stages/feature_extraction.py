import logging
import random
from pyspark.sql import DataFrame
from patient_notes.config import (
    COGNITIVE_LOCATION,
    EXTRACTED_PREFIX,
    WORKER_COUNT,
    CORE_COUNT,
    TableConfig,
)
from synapse.ml.cognitive import AnalyzeHealthText
from patient_notes.common_types import ColumnType
from functools import reduce


def analyse(df: DataFrame, col: str, key: str) -> DataFrame:
    """Analyse text in a column using a cognitive key

    Args:
        df (DataFrame): containing free-text column to analyse
        col (str): name of the column to analyse
        key (str): cognitive key to use for analysis

    Returns:
        Dataframe: containing the results of the analysis
    """
    return (
        AnalyzeHealthText(
            textCol=col,
            outputCol=f"{col}{EXTRACTED_PREFIX}",
            subscriptionKey=key,
            batchSize=10,
            concurrency=8,
        )
        .setLocation(COGNITIVE_LOCATION)
        .transform(df)
    )


def extract_features(
    df: DataFrame, table_name: str, table_config: TableConfig, cognitive_keys: list[str]
) -> DataFrame:
    """Perform feature extraction on the specified table and configured
    free text column(s). Uses external service.

    Args:
        df (DataFrame): containing free-text columns to enrich
        table_name (str): name of the table being processed
        table_config (TableConfig): containing table configuration
        cognitive_keys (list[str]): Cognitive Services key(s). Calls to Text Analytics
            will be distributed across the keys provided.

    Returns:
        DataFrame: with extracted features
    """

    # Skip tables that don't have free-text columns or any column types configured
    if "column_types" not in table_config:
        logging.info(f"No column types configured for {table_name}. Skipping feature extraction.")
        return df

    if ColumnType.FREE_TEXT not in table_config["column_types"]:
        logging.info(
            f"No free-text columns configured for {table_name}. Skipping feature extraction."
        )
        return df

    columns = table_config["column_types"][ColumnType.FREE_TEXT]

    # Determine if DataFrame is large enough (100 rows or more) to warrant splitting
    # Use take() instead of count() so we don't force full DF collection
    if len(df.take(100)) == 100:
        # Increase partition count for parallel processing
        initial_partitions = df.rdd.getNumPartitions()
        df = df.repartition(max(WORKER_COUNT * CORE_COUNT, initial_partitions))

        # Split the dataframe into multiple sub-dataframes, one for each cognitive key
        sub_dfs = df.randomSplit([1.0] * len(cognitive_keys))

        for column in columns:
            logging.info(
                f"Extracting features from free-text column: {column} in table: {table_name}"
                f" (across {len(cognitive_keys)} cognitive keys)"
            )

            # Iterate over each sub-dataframe and use a different cognitive key for each
            for i, sub_df in enumerate(sub_dfs):
                # Extract health entities/relationships for the free-text column
                sub_df = analyse(sub_df, column, cognitive_keys[i]).select(
                    f"{column}{EXTRACTED_PREFIX}", *table_config["primary_keys"]
                )

                # Join the outputted entities column back to the original sub-dataframe
                sub_dfs[i] = sub_dfs[i].join(sub_df, table_config["primary_keys"])

        # Combine the sub-dataframes back into a single dataframe
        df = reduce(DataFrame.union, sub_dfs)

    else:
        for column in columns:
            logging.info(
                f"Extracting features from free-text column: {column} in table: {table_name}"
            )
            df = analyse(df, column, random.choice(cognitive_keys))

    return df
