from typing import List, Tuple

from delta import DeltaTable

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession
from patient_notes.watermark import (
    get_high_watermark,
    get_low_watermark,
    update_watermark,
)
from patient_notes.common_types import ChangeType, DatalakeZone, PipelineActivity


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
    table_without_db_schema = table.split(".")[-1]
    return f"abfss://{zone.value}@{datalake_uri}/{table_without_db_schema}"


def read_delta_table(spark: SparkSession, path: str) -> DataFrame:
    """Read Datalake Delta table within a specified zone

    Args:
        spark (SparkSession): Spark session.
        path (str): Path to file inside Azure storage.

    Returns:
        DataFrame: Loads file as a Spark DataFrame.
    """
    return spark.read.format("delta").load(path)


def read_delta_table_updates(
    spark: SparkSession,
    activity: PipelineActivity,
    data_dt_path: str,
    watermark_path: str,
) -> Tuple[DataFrame, int]:
    """Read Datalake Delta table updates from specified low_watermark.

    Args:
        spark (SparkSession): Spark session.
        activity (str): Name of the activity.
        data_dt_path (str): Path of the delta table from where data is to be read.
        watermark_path: (str): Path of the watermark delta table.
    """
    low_watermark = get_low_watermark(spark, activity, watermark_path)
    high_watermark = get_high_watermark(spark, data_dt_path)

    return (
        spark.read.format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", low_watermark)
        .option("endingVersion", high_watermark)
        .load(data_dt_path)
    ), high_watermark


def overwrite_delta_table(df: DataFrame, path: str) -> None:
    """Write Datalake Delta table within a specified zone (in overwrite mode).

    Args:
        df (DataFrame): DataFrame that is the subject of being serialized.
        path (str): Path to file inside Azure storage.
    """
    df.write.format("delta").mode("overwrite").save(path)


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
    # cut `dbo.` from the table_name
    spark_session.sql(
        f"CREATE TABLE IF NOT EXISTS `{catalog_name}`.`{schema_name}`.{table_name}"
        f' LOCATION "{path}"'
    )


def write_data_update(
    spark_session: SparkSession,
    data_dt_path: str,
    processed_df: DataFrame,
    primary_keys: List[str],
    watermark_url: str,
    current_high_watermark: int,
) -> None:
    """
    Merges source data in processed_df arg into delta table at data_dt_path.
    Merge is done for 'insert', 'update_postimage' and 'delete' _change_type.
    If the merge completes without error it updates watermark.

    TODO: handle update

    Args:
        spark_session (SparkSession): Spark session.
        data_dt_path (str): Path of the target delta table.
        processed_df (DataFrame): Dataframe that has the updates/ source data.
        primary_keys List[]: Primary key column names to use for merge.
        watermark_url (str): Path of delta table which holds watermarks.
        current_high_watermark (int): Current high watermark.
    """
    delta_table = DeltaTable.forPath(spark_session, data_dt_path)
    primary_key_match = [
        col(f"target.{col_name}") == col(f"source.{col_name}") for col_name in primary_keys
    ]
    merge_condition = primary_key_match[0]
    for condition in primary_key_match[1:]:
        merge_condition = merge_condition & condition

    # Update
    update_df = processed_df.filter(
        processed_df["_change_type"].isin(
            [ChangeType.PRE_UPDATE.value, ChangeType.POST_UPDATE.value]
        )
    )
    # 'update_preimage' and 'update_postimage' change types are not expected
    # so raising exception if these change types are found.
    if update_df.count() > 0:
        raise ValueError(
            "DataFrame contains update_preimage or update_postimage _change_type(s). "
            "Cannot merge data."
        )

    # Delete
    delete_src_df = processed_df.where(processed_df["_change_type"] == ChangeType.DELETE.value)
    delta_table.alias("target").merge(
        source=delete_src_df.alias("source"),
        condition=merge_condition,
    ).whenMatchedDelete().execute()

    # Insert
    insert_src_df = processed_df.where(processed_df["_change_type"] == ChangeType.INSERT.value)
    delta_table.alias("target").merge(
        source=insert_src_df.alias("source"),
        condition=merge_condition,
    ).whenNotMatchedInsertAll().execute()

    # Update watermark
    update_watermark(
        spark_session,
        watermark_url,
        PipelineActivity.PSEUDONYMISATION,
        current_high_watermark,
    )
