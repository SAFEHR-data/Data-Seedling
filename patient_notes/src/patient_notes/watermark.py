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

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit
from pyspark.sql.session import SparkSession
from patient_notes.common_types import PipelineActivity, WatermarkColumn


def get_low_watermark(
    spark_session: SparkSession,
    activity: PipelineActivity,
    watermark_url: str,
    table_name: str,
    default_value=0,
) -> int:
    """
    Gets the low_watermark for a specific activity to read data from.
    Creates new delta table if activity watermark doesn't exist.
    Inserts new row if watermark for the activity doesn't exist.
    Returns default value if the activity watermark table or row doesn't exist.

    Args:
        spark_session (SparkSession): The SparkSession object.
        activity (PipelineActivity): The activity to include in the DataFrame.
        watermark_url (str): The url of the watermark delta table location.
        default_value (int, optional): The value that gets returned if the table
        or the row doesn't exist.

    Returns:
        int: The low_watermark value for the activity.
    """
    if not activity:
        raise ValueError("Invalid activity argument. Cannot get high watermark.")

    # Create new delta table if needed
    if not DeltaTable.isDeltaTable(spark_session, watermark_url):
        df = spark_session.createDataFrame(
            [(default_value, activity.value, table_name)],
            [
                WatermarkColumn.LOW_WATERMARK.value,
                WatermarkColumn.ACTIVITY.value,
                WatermarkColumn.TABLE_NAME.value,
            ],
        )
        df.write.format("delta").mode("overwrite").save(watermark_url)
        return default_value

    df = spark_session.read.format("delta").load(watermark_url)
    activity_watermark_row = df.filter(
        (col(WatermarkColumn.ACTIVITY.value) == activity.value)
        & (col(WatermarkColumn.TABLE_NAME.value) == table_name)
    ).orderBy(WatermarkColumn.LOW_WATERMARK.value)

    # Insert watermark if needed
    if activity_watermark_row.isEmpty():
        df = spark_session.createDataFrame(
            [(default_value, activity.value, table_name)],
            [
                WatermarkColumn.LOW_WATERMARK.value,
                WatermarkColumn.ACTIVITY.value,
                WatermarkColumn.TABLE_NAME.value,
            ],
        )
        df.write.format("delta").mode("append").save(watermark_url)
        return default_value

    # Extract value
    row_value = activity_watermark_row.select(WatermarkColumn.LOW_WATERMARK.value).first()
    if row_value is None:
        raise ValueError(f"{WatermarkColumn.LOW_WATERMARK.value} value is missing or invalid.")

    return row_value[0]


def get_high_watermark(spark_session: SparkSession, data_dt_url: str) -> int:
    """
    Gets the high_watermark value of the delta table at given data_dt_url.

    Args:
        spark_session (SparkSession): The SparkSession object.
        data_dt_url (str): The url of the data delta table.

    Returns:
        int: The high_watermark value for the delta table.
    """
    delta_table = DeltaTable.forPath(spark_session, data_dt_url)
    versions = delta_table.history().select("version").orderBy("version", ascending=False).first()
    if versions is None:
        raise ValueError("Could not find versions from delta table history.")

    return versions[0]


def update_watermark(
    spark_session: SparkSession,
    watermark_url: str,
    activity: PipelineActivity,
    last_high_watermark: int,
    table_name: str,
) -> None:
    """
    Updates the low watermark in the Delta table for provided activity.

    Parameters:
    - spark_session (SparkSession): The SparkSession object used for Spark operations.
    - watermark_url (str): The URL or path to the Delta table for watermarks.
    - activity (str): The activity to identify the row to update in the Delta table.
    - last_high_watermark (int): Last high watermark value.
    """
    # Increase watermark to represent next start version
    new_low_watermark = last_high_watermark + 1

    dt_watermark = DeltaTable.forPath(spark_session, watermark_url)
    dt_watermark.update(
        condition=(col(WatermarkColumn.ACTIVITY.value) == activity.value)
        & (col(WatermarkColumn.TABLE_NAME.value) == table_name),
        set={WatermarkColumn.LOW_WATERMARK.value: lit(new_low_watermark)},
    )
