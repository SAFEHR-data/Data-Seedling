#  Copyright (c) University College London Hospitals NHS Foundation Trust
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import logging
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat, date_trunc, lit, sha2
from pyspark.sql.udf import UserDefinedFunction
from patient_notes.config import HASH_SUFFIX, HASH_SALT
from patient_notes.common_types import ColumnType, DateTimeRoundOpt, TableConfig


def process_free_text_columns(
    df: DataFrame,
    columns: list[str],
    anonymise_udf: UserDefinedFunction,
    table_name: str,
) -> DataFrame:
    """Pseudonymises free text using the pseudonymiser

    Args:
        df (DataFrame): The input DataFrame containing free text to be pseudonymised.
        columns (list[str]): List of column names in the DataFrame to pseudonymise.
        anonymise_udf (UserDefinedFunction): Anonymise function.
        table_name (str): Name of the table being processed.

    Returns:
        DataFrame: A new DataFrame with pseudonymised values.

    Raises:
        KeyError: If a column specified in 'columns' does not exist in the DataFrame.
    """
    for column in columns:
        if column in df.columns:
            # Perform free text anonymisation
            logging.info(f"Anonymising free-text column: {column} in table: {table_name}")
            df = df.withColumn(column, anonymise_udf(col(column)))
        else:
            raise KeyError(
                f"Unable to pseudonymise column '{column}' as it does not exist in the"
                f" source DataFrame ({table_name})."
            )

    return df


def remove_columns(df: DataFrame, columns: list[str], table_name: str) -> DataFrame:
    """Remove specified columns from Spark DataFrame.

    Args:
        df (DataFrame): The input DataFrame containing columns to be removed.
        columns (list[str]): List of column names in the DataFrame to remove.
        table_name (str): Name of the table being processed.

    Returns:
        DataFrame: A new DataFrame with removed column(s).

    Raises:
        KeyError: If a column specified in 'columns' does not exist in the DataFrame.
    """
    for column in columns:
        if column in df.columns:
            logging.info(f"Removing column: {column} in table: {table_name}")
            df = df.drop(column)
        else:
            raise KeyError(
                f"Unable to drop column '{column}' as it does not exist in the source"
                f" DataFrame ({table_name})."
            )
    return df


def round_datetime_columns(
    df: DataFrame, columns: list[str], round_option: DateTimeRoundOpt, table_name: str
) -> DataFrame:
    """Rounds datetime values in specified DataFrame columns using the provided
    rounding option (most commonly hour).

    Reference:
        https://github.com/M-RIC-TRE/data-pipelines/blob/main/main_pipeline.py

    Args:
        df (DataFrame): The input DataFrame containing datetime columns to be rounded.
        columns (list[str]): List of column names in the DataFrame to round.
        round_option (DateTimeRoundOpt): An enum for the rounding options.
        table_name (str): Name of the table being processed.

    Returns:
        DataFrame: A new DataFrame with datetime values rounded based on round_option.

    Raises:
        KeyError: If a column specified in 'columns' does not exist in the DataFrame.
    """
    for column in columns:
        if column in df.columns:
            # Perform rounding with required precision defined by the value of enum
            logging.info(f"Rounding datetime column: {column} in table: {table_name}")
            df = df.withColumn(column, date_trunc(round_option.value, df[column]))
        else:
            raise KeyError(
                f"Unable to round datetime in column '{column}' as it does not exist in"
                f" the source DataFrame ({table_name})."
            )
    return df


def hash_hashable_id(df: DataFrame, columns: list[str], table_name: str) -> DataFrame:
    """Hashes the specified columns containing IDs in the DataFrame.

    Reference:
        https://github.com/M-RIC-TRE/data-pipelines/blob/main/main_pipeline.py

    Args:
        df (DataFrame): The DataFrame containing IDs to be hashed.
        columns (list[str]): List of column names in the DataFrame.
        table_name (str): Name of the table being processed.

    Returns:
        DataFrame: A new DataFrame with hashed IDs.

    Raises:
        KeyError: If a column specified in 'columns' does not exist in the DataFrame.
    """
    for column in columns:
        if column in df.columns:
            logging.info(f"Hashing hashable ID column: {column} in table: {table_name}")

            # Merge hashable ID with HASH_SALT (salting procedure)
            df = df.withColumn(column, concat(df[column], lit(HASH_SALT)))
            # Compute hash value
            df = df.withColumn(
                # Create a new column with name: ColumnNameSUFFIX with predefined SUFFIX
                "".join([column, HASH_SUFFIX]),
                sha2(df[column].cast("Binary"), 256),
            )
            # Drop old column (one without prefix and without the hashed value)
            df = df.drop(column)
        else:
            raise KeyError(
                f"Unable to hash column '{column}' as it does not exist in"
                f" the source DataFrame ({table_name})."
            )
    return df


def pseudo_transform(
    df: DataFrame,
    table_name: str,
    table_config: TableConfig,
    anonymise_udf: UserDefinedFunction,
) -> DataFrame:
    """Apply pseudonymisation transform to the specified table based on the
    provided configuration.

    Args:
        df (DatatFrame): Dataframe that is the subject of transformation.
        table_name (str): Name of the table.
        table_config (TableConfig): Configuration for the table.
        anonymise_udf (UserDefinedFunction): User defined function for anonymisation.

    Returns:
        DataFrame: Transformed DataFrame.
    """

    if "column_types" not in table_config:
        logging.info(f"No column types configured for {table_name}. Skipping pseudonymisation.")
        return df

    for column_type, columns in table_config["column_types"].items():
        match column_type:
            case ColumnType.FREE_TEXT:
                df = process_free_text_columns(df, columns, anonymise_udf, table_name)
            case ColumnType.OTHER_IDENTIFIABLE:
                df = remove_columns(df, columns, table_name)
            case ColumnType.DATE_TIME:
                df = round_datetime_columns(df, columns, DateTimeRoundOpt.HOUR, table_name)
            case ColumnType.DATE:
                df = round_datetime_columns(df, columns, DateTimeRoundOpt.MONTH, table_name)
            case ColumnType.HASHABLE_ID:
                df = hash_hashable_id(df, columns, table_name)
            case _:
                logging.warning(f"Unsupported column type: {column_type}. Skipping.")
                pass

    return df
