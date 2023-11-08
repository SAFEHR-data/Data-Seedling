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

from delta import DeltaTable
import pytest
from patient_notes.common_types import PipelineActivity
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from patient_notes.datalake import (
    DatalakeZone,
    construct_uri,
    read_delta_table_update,
    write_delta_table_update,
)
from patient_notes.tests.conftest import TEST_DATALAKE_URI


def test_construct_uri(spark_session: SparkSession):
    table = "dbo.TestTable"
    zone = DatalakeZone.BRONZE
    uri = construct_uri(spark_session, zone, table)
    assert uri == f"abfss://{zone.value}@{TEST_DATALAKE_URI}/TestTable"


def test_read_delta_table_update_returns_all_data_when_no_low_watermark_found(
    spark_session: SparkSession, delta_dir: str, internal_dir: str
):
    v_zero_df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["key", "value"])
    v_zero_df.write.format("delta").mode("overwrite").save(delta_dir)
    v_one_df = spark_session.createDataFrame([(3, "c")], ["key", "value"])
    v_one_df.write.format("delta").mode("append").save(delta_dir)
    v_two_df = spark_session.createDataFrame([(4, "d")], ["key", "value"])
    v_two_df.write.format("delta").mode("append").save(delta_dir)

    actual_data = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, internal_dir, "t1"
    )
    actual_df = actual_data[0].select(["key", "value"])

    expected_df = spark_session.createDataFrame(
        [(1, "a"), (2, "b"), (3, "c"), (4, "d")], ["key", "value"]
    )
    assert actual_data[1] == 2
    assert actual_df.orderBy("key").collect() == expected_df.orderBy("key").collect()


def test_read_delta_table_update_returns_data_of_multiple_version_given_low_watermark(
    spark_session: SparkSession, delta_dir: str, internal_dir: str
):
    # Create data in versions
    # 0
    v_zero_df = spark_session.createDataFrame([(0, "a")], ["key", "value"])
    v_zero_df.write.format("delta").mode("overwrite").save(delta_dir)
    # 1
    v_one_df = spark_session.createDataFrame([(1, "b")], ["key", "value"])
    v_one_df.write.format("delta").mode("append").save(delta_dir)
    # 2
    v_two_df = spark_session.createDataFrame([(2, "c")], ["key", "value"])
    v_two_df.write.format("delta").mode("append").save(delta_dir)
    # 3
    v_three_df = spark_session.createDataFrame([(3, "d")], ["key", "value"])
    v_three_df.write.format("delta").mode("append").save(delta_dir)

    # Create watermark with low_watermark value
    activity = PipelineActivity.PSEUDONYMISATION
    watermark_df = spark_session.createDataFrame(
        [(activity.value, 2, "table")], ["activity", "low_watermark", "table_name"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(internal_dir)

    actual_data = read_delta_table_update(
        spark_session, activity, delta_dir, internal_dir, "table"
    )
    actual_df = actual_data[0].select(["key", "value"])

    expected_df = spark_session.createDataFrame([(2, "c"), (3, "d")], ["key", "value"])
    # Assert last version is returned as high_watermark
    assert actual_data[1] == 3
    assert actual_df.orderBy("key").collect() == expected_df.orderBy("key").collect()


def test_read_delta_table_update_returns_data_of_last_version_given_low_watermark(
    spark_session: SparkSession, delta_dir: str, internal_dir: str
):
    # Create data in versions
    # 0
    v_zero_df = spark_session.createDataFrame([(0, "a")], ["key", "value"])
    v_zero_df.write.format("delta").mode("overwrite").save(delta_dir)
    # 1
    v_one_df = spark_session.createDataFrame([(1, "b")], ["key", "value"])
    v_one_df.write.format("delta").mode("append").save(delta_dir)
    # 2
    v_two_df = spark_session.createDataFrame([(2, "c")], ["key", "value"])
    v_two_df.write.format("delta").mode("append").save(delta_dir)
    # 3
    v_three_df = spark_session.createDataFrame([(3, "d")], ["key", "value"])
    v_three_df.write.format("delta").mode("append").save(delta_dir)

    # Create watermark with low_watermark
    activity = PipelineActivity.PSEUDONYMISATION
    watermark_df = spark_session.createDataFrame(
        [(activity.value, 3, "table")], ["activity", "low_watermark", "table_name"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(internal_dir)

    actual_data = read_delta_table_update(
        spark_session, activity, delta_dir, internal_dir, "table"
    )
    actual_df = actual_data[0].select(["key", "value"])

    expected_df = spark_session.createDataFrame([(3, "d")], ["key", "value"])
    # Assert last version is returned as high_watermark
    assert actual_data[1] == 3
    assert actual_df.orderBy("key").collect() == expected_df.orderBy("key").collect()


def test_read_delta_table_update_returns_appended_row(
    spark_session: SparkSession, delta_dir: str, internal_dir: str
):
    existing_df = spark_session.createDataFrame([(1, "a"), (2, "b")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(delta_dir)
    first_update_df = spark_session.createDataFrame([(3, "c")], ["key", "value"])
    first_update_df.write.format("delta").mode("append").save(delta_dir)
    second_update_df = spark_session.createDataFrame([(30, "c")], ["key", "value"])
    second_update_df.write.format("delta").mode("append").save(delta_dir)

    # Create watermark with low_watermark
    activity = PipelineActivity.PSEUDONYMISATION
    watermark_df = spark_session.createDataFrame(
        [(activity.value, 2, "table")], ["activity", "low_watermark", "table_name"]
    )
    watermark_df.write.format("delta").mode("overwrite").save(internal_dir)

    data = read_delta_table_update(
        spark_session,
        PipelineActivity.PSEUDONYMISATION,
        delta_dir,
        internal_dir,
        "table",
    )[0]
    actual_df = data.select(["key", "value"])

    expected_df = spark_session.createDataFrame([(30, "c")], ["key", "value"])
    assert actual_df.orderBy("key").collect() == expected_df.orderBy("key").collect()


# Write delta table change
# Insert
def test_write_delta_table_update_inserts_data_from_multiple_versions(
    spark_session: SparkSession, bronze_dir: str, internal_dir: str, silver_dir: str
):
    # Create initial data in Bronze
    existing_df = spark_session.createDataFrame([(1, "a")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Create initial data in Silver
    existing_df.write.format("delta").mode("overwrite").save(silver_dir)

    # First insert in Bronze
    first_update_df = spark_session.createDataFrame([(2, "b")], ["key", "value"])
    first_update_df.write.format("delta").mode("append").save(bronze_dir)

    # Second insert in Bronze
    second_update_df = spark_session.createDataFrame([(3, "c")], ["key", "value"])
    second_update_df.write.format("delta").mode("append").save(bronze_dir)

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    # Third insert in Bronze (after on write)
    third_update_df = spark_session.createDataFrame([(4, "d")], ["key", "value"])
    third_update_df.write.format("delta").mode("append").save(bronze_dir)

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    actual_bronze_data = (
        spark_session.read.format("delta").load(bronze_dir).orderBy("key").collect()
    )
    actual_silver_data = (
        spark_session.read.format("delta").load(silver_dir).orderBy("key").collect()
    )
    watermark_data = spark_session.read.format("delta").load(internal_dir).collect()

    assert actual_bronze_data == actual_silver_data
    assert watermark_data[0][0] == 4


# Vacuum
def test_write_delta_table_update_treats_vacuum_operation_as_incremental_update(
    spark_session: SparkSession, bronze_dir: str, internal_dir: str, silver_dir: str
):
    # Create initial data in Bronze
    existing_df = spark_session.createDataFrame([(1, "a")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Create initial data in Silver
    existing_df.write.format("delta").mode("overwrite").save(silver_dir)

    # Vacuum
    delta_table = DeltaTable.forPath(spark_session, bronze_dir)
    delta_table.vacuum()

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    actual_bronze_data = (
        spark_session.read.format("delta").load(bronze_dir).orderBy("key").collect()
    )
    actual_silver_data = (
        spark_session.read.format("delta").load(silver_dir).orderBy("key").collect()
    )
    watermark_data = spark_session.read.format("delta").load(internal_dir).collect()

    assert actual_bronze_data == actual_silver_data
    assert watermark_data[0][0] == 3


# Delete
def test_write_delta_table_update_deletes_overwritten_data(
    spark_session: SparkSession, bronze_dir: str, internal_dir: str, silver_dir: str
):
    # Create initial data in Bronze
    existing_df = spark_session.createDataFrame([(1, "a")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Create initial data in Silver
    existing_df.write.format("delta").mode("overwrite").save(silver_dir)

    # First insert in Bronze
    first_update_df = spark_session.createDataFrame([(2, "b")], ["key", "value"])
    first_update_df.write.format("delta").mode("append").save(bronze_dir)

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    # Delete
    delete_df = spark_session.read.format("delta").load(bronze_dir).filter(col("key") != lit(1))
    delete_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    actual_bronze_data = (
        spark_session.read.format("delta").load(bronze_dir).orderBy("key").collect()
    )
    actual_silver_data = (
        spark_session.read.format("delta").load(silver_dir).orderBy("key").collect()
    )
    watermark_data = spark_session.read.format("delta").load(internal_dir).collect()

    assert actual_bronze_data == actual_silver_data
    assert watermark_data[0][0] == 3


def test_write_delta_table_update_deletes_given_data_deleted_using_whenMatchedDelete(
    spark_session: SparkSession, bronze_dir: str, internal_dir: str, silver_dir: str
):
    # Create initial data in Bronze
    existing_df = spark_session.createDataFrame([(1, "a")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Create initial data in Silver
    existing_df.write.format("delta").mode("overwrite").save(silver_dir)

    # First insert in Bronze
    first_update_df = spark_session.createDataFrame([(2, "b")], ["key", "value"])
    first_update_df.write.format("delta").mode("append").save(bronze_dir)

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    # Delete
    delta_table = DeltaTable.forPath(spark_session, bronze_dir)
    src_df = spark_session.createDataFrame([(1, "a", "delete")], ["key", "value", "_change_type"])
    delta_table.alias("target").merge(
        source=src_df.alias("source"),
        condition="target.key == source.key",
    ).whenMatchedDelete().execute()

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    write_delta_table_update(
        spark_session,
        silver_dir,
        data,
        ["key"],
        internal_dir,
        high_watermark,
        "t1",
        PipelineActivity.PSEUDONYMISATION,
    )

    actual_bronze_data = (
        spark_session.read.format("delta").load(bronze_dir).orderBy("key").collect()
    )
    actual_silver_data = (
        spark_session.read.format("delta").load(silver_dir).orderBy("key").collect()
    )
    watermark_data = spark_session.read.format("delta").load(internal_dir).collect()

    assert actual_bronze_data == actual_silver_data
    assert watermark_data[0][0] == 3


# Update
def test_write_delta_table_update_raises_exception_when_change_type_is_for_update(
    spark_session: SparkSession, bronze_dir: str, internal_dir: str, silver_dir: str
):
    # Create initial data in Bronze
    existing_df = spark_session.createDataFrame([(1, "a")], ["key", "value"])
    existing_df.write.format("delta").mode("overwrite").save(bronze_dir)

    # Create initial data in Silver
    existing_df.write.format("delta").mode("overwrite").save(silver_dir)

    # Update
    delta_table = DeltaTable.forPath(spark_session, bronze_dir)
    src_df = spark_session.createDataFrame(
        [(1, "a - updated", "update")], ["key", "value", "_change_type"]
    )
    delta_table.alias("target").merge(
        source=src_df.alias("source"),
        condition="target.key == source.key",
    ).whenMatchedUpdateAll().execute()

    # Read
    data, high_watermark = read_delta_table_update(
        spark_session, PipelineActivity.PSEUDONYMISATION, bronze_dir, internal_dir, "t1"
    )

    # Write
    with pytest.raises(ValueError) as excinfo:
        write_delta_table_update(
            spark_session,
            silver_dir,
            data,
            ["key"],
            internal_dir,
            high_watermark,
            "t1",
            PipelineActivity.PSEUDONYMISATION,
        )

    assert (
        str(excinfo.value)
        == "DataFrame contains update_preimage or "
        "update_postimage _change_type(s). Cannot merge data."
    )
