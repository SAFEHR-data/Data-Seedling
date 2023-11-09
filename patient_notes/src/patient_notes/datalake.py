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

from typing import List, Tuple, Any
import logging

from delta import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession

from patient_notes.watermark import (
    get_high_watermark,
    get_low_watermark,
    update_watermark,
)
from patient_notes.common_types import (
    ChangeType,
    DatalakeZone,
    PipelineActivity,
    ReservedColumns,
)
from patient_notes.monitoring import send_rows_updated_metric


def construct_uri(spark: SparkSession, zone: DatalakeZone, table: str) -> str:
    """Construct Azure Datalake URI for reading/writing to specific zone and table.

    Args:
        spark (SparkSession): Spark session.
        zone (str): Datalake zone to target.
        table (str): Table name (schema prefix will be stripped automatically).

    Returns:
        str: Full URI inside storage.
    """
    datalake_uri = spark.conf.get("spark.secret.datalake-uri")
    return f"abfss://{zone.value}@{datalake_uri}/{table}"


def read_delta_table_update(
    spark: SparkSession,
    activity: PipelineActivity,
    path: str,
    watermark_path: str,
    table_name: str,
) -> Tuple[DataFrame, int]:
    """Read Datalake Delta table updates from specified low_watermark

    Args:
        spark (SparkSession): Spark session.
        activity (str): Name of the activity.
        data_dt_path (str): Path of the delta table from where data is to be read.
        watermark_path: (str): Path of the watermark delta table.

    Returns:
        df (DataFrame): PySpark dataframe containing data updates.
            It contains additional columns _change_type, _commit_version, _commit_timestamp
            Can be empty in case there's been no new update since last call.
            If the df is empty, it will still have the schema of the source dataframe.
        high_watermark: Last version read as part of the update.
    """
    low_watermark = get_low_watermark(spark, activity, watermark_path, table_name)
    high_watermark = get_high_watermark(spark, path)

    if low_watermark > high_watermark:
        logging.warn(
            "No delta table update to process."
            "low_watermark {low_watermark},"
            " high_watermark (i.e. last table version) {high_watermark}"
        )
        # Getting table schema
        df_schema = spark.read.format("delta").load(path).schema
        # Returning an empty table with a compatible schema
        return spark.createDataFrame(spark.sparkContext.emptyRDD(), df_schema), high_watermark

    df = (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", low_watermark)
        .option("endingVersion", high_watermark)
        .load(path)
    )
    logging.info(
        f"Read {df.count()} rows from path {path} "
        f"startingVersion {low_watermark} endingVersion {high_watermark}"
    )
    return df, high_watermark


def create_table_in_unity_catalog(
    spark_session: SparkSession,
    path: str,
    catalog_name: str,
    schema_name: str,
    table_name: str,
) -> None:
    """
    Registers table as an External table in Unity Catalog, if it doesn't yet exist.

    Args:
        spark_session (SparkSession): Spark session.
        path (str): Path to a previously saved table in Data lake.
        catalog_name (str)
    """
    spark_session.sql(
        f"CREATE TABLE IF NOT EXISTS `{catalog_name}`.`{schema_name}`.{table_name}"
        f' LOCATION "{path}"'
    )


def merge_by_primary_keys_condition(primary_keys: List[str]) -> Any:
    """
    Helper method constructing merge condition by all primary keys

    Args:
        primary_keys (List[str]): List of all primary key column names
    Returns:
        merge_condition (ExpressionOrColumn): Merge condition that can be passed to merge() method
    """
    primary_key_match = [
        col(f"target.{column_name}") == col(f"source.{column_name}")
        for column_name in primary_keys
    ]
    merge_condition = primary_key_match[0]
    for condition in primary_key_match[1:]:
        merge_condition = merge_condition & condition
    return merge_condition


def write_delta_table_update(
    spark_session: SparkSession,
    path: str,
    processed_df: DataFrame,
    primary_keys: List[str],
    watermark_url: str,
    current_high_watermark: int,
    table_name: str,
    activity: PipelineActivity,
) -> None:
    """
    Merges source data in processed_df arg into delta table at path.
    Merge is done for 'insert' and 'delete' _change_type.
    If the merge completes without error it updates watermark.

    If any rows of 'update_preimage' or 'update_postimage' change type are found,
    this function throws a ValueError.

    Args:
        spark_session (SparkSession): Spark session.
        path (str): Path of the target delta table.
        processed_df (DataFrame): Dataframe that has the updates/ source data.
        primary_keys List[]: Primary key column names to use for merge.
        watermark_url (str): Path of delta table which holds watermarks.
        current_high_watermark (int): Current high watermark.
        table_name (str): Name of the table to store in watermark.
        activity (PipelineActivity): The activity to store in watermark.
    """
    if DeltaTable.isDeltaTable(spark_session, path):
        if processed_df.isEmpty():
            # Don't process and dont update watermark
            return

        target_table = DeltaTable.forPath(spark_session, path)

        # Update ('update_preimage' and 'update_postimage') change types are not expected
        # so raising exception if these change types are found.
        rows_updated = processed_df.where(
            processed_df["_change_type"].isin(
                [ChangeType.PRE_UPDATE.value, ChangeType.POST_UPDATE.value]
            )
        ).count()
        if rows_updated > 0:
            raise ValueError(
                "DataFrame contains update_preimage or update_postimage _change_type(s). "
                "Cannot merge data."
            )

        # Delete rows from target that are deleted from source
        delete_df = processed_df.where(processed_df["_change_type"] == ChangeType.DELETE.value)
        rows_deleted = delete_df.count()
        if rows_deleted:
            target_table.alias("target").merge(
                source=delete_df.alias("source"),
                condition=merge_by_primary_keys_condition(primary_keys=primary_keys),
            ).whenMatchedDelete().execute()
        send_rows_updated_metric(
            value=rows_deleted,
            tags={"table_name": table_name, "operation": "delete", "activity": activity.value},
        )
        logging.info(f"Deleted {rows_deleted} rows from table {table_name} in {path}")

        # Insert rows to target that are inserted to source
        insert_df = processed_df.where(processed_df["_change_type"] == ChangeType.INSERT.value)
        rows_inserted = insert_df.count()
        if rows_inserted:
            target_table.alias("target").merge(
                source=insert_df.alias("source"),
                condition=merge_by_primary_keys_condition(primary_keys=primary_keys),
            ).whenNotMatchedInsertAll().execute()
        send_rows_updated_metric(
            value=rows_inserted,
            tags={"table_name": table_name, "operation": "insert", "activity": activity.value},
        )
        logging.info(f"Inserted {rows_inserted} rows from table {table_name} in {path}")
    else:
        # Create a new table
        df = processed_df.drop(
            ReservedColumns.CHANGE.value,
            ReservedColumns.COMMIT.value,
            ReservedColumns.COMMIT_TIME.value,
        )
        rows_inserted = df.count()
        df.write.format("delta").save(path)
        send_rows_updated_metric(
            value=rows_inserted,
            tags={"table_name": table_name, "operation": "insert", "activity": activity.value},
        )
        logging.info(f"Created an empty table {table_name} with {rows_inserted} rows in {path}")

    # Update watermark
    update_watermark(
        spark_session,
        watermark_url,
        activity,
        current_high_watermark,
        table_name,
    )
