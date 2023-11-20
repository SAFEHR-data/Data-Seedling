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

import os
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark_test import assert_pyspark_df_equal
from patient_notes.watermark import (
    get_high_watermark,
    get_low_watermark,
    update_watermark,
)
from patient_notes.common_types import PipelineActivity


# Test get_low_watermark
def test_get_low_watermark_returns_low_watermark(spark_session: SparkSession, delta_dir: str):
    table_name = os.path.basename(delta_dir)
    test_df = spark_session.createDataFrame(
        [(3, "pseudonymisation", table_name), (1, "featue_extraction", table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    test_df.write.format("delta").save(delta_dir)

    actual_low_watermark = get_low_watermark(
        spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name
    )

    assert actual_low_watermark == 3


def test_get_low_watermark_creates_new_delta_table(spark_session: SparkSession, delta_dir: str):
    table_name = os.path.basename(delta_dir)
    get_low_watermark(spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name)

    assert DeltaTable.isDeltaTable(spark_session, delta_dir)
    expected_df = spark_session.createDataFrame(
        [(0, PipelineActivity.PSEUDONYMISATION.value, table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    new_activity_df = (
        spark_session.read.format("delta")
        .load(delta_dir)
        .filter(
            (col("activity") == PipelineActivity.PSEUDONYMISATION.value)
            & (col("table_name") == table_name)
        )
    )
    assert_pyspark_df_equal(new_activity_df, expected_df, order_by="table_name")


def test_get_low_watermark_returns_default_value_when_table_doesnt_exist(
    spark_session: SparkSession, delta_dir: str
):
    table_name = os.path.basename(delta_dir)
    actual_low_watermark = get_low_watermark(
        spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name
    )

    assert actual_low_watermark == 0


def test_get_low_watermark_appends_new_watermark_when_it_doesnt_exist(
    spark_session: SparkSession, delta_dir: str
):
    table_name = os.path.basename(delta_dir)
    existing_df = spark_session.createDataFrame(
        [(1, "existing", table_name), (2, "existing2", table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    existing_df.write.format("delta").save(delta_dir)

    get_low_watermark(spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name)

    expected_df = spark_session.createDataFrame(
        [
            (1, "existing", table_name),
            (2, "existing2", table_name),
            (0, PipelineActivity.PSEUDONYMISATION.value, table_name),
        ],
        ["low_watermark", "activity", "table_name"],
    )
    new_activity_df = spark_session.read.format("delta").load(delta_dir)
    assert_pyspark_df_equal(new_activity_df, expected_df, order_by="activity")


def test_get_low_watermark_creates_activity_row_when_doesnt_exist(
    spark_session: SparkSession,
    delta_dir: str,
):
    table_name = os.path.basename(delta_dir)
    existing_df = spark_session.createDataFrame(
        [(1, "featue_extraction", table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    existing_df.write.format("delta").save(delta_dir)

    get_low_watermark(spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name)

    expected_df = spark_session.createDataFrame(
        [(0, PipelineActivity.PSEUDONYMISATION.value, table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    new_activity_df = (
        spark_session.read.format("delta")
        .load(delta_dir)
        .filter(
            (col("activity") == PipelineActivity.PSEUDONYMISATION.value)
            & (col("table_name") == table_name)
        )
    )
    assert_pyspark_df_equal(new_activity_df, expected_df, order_by="table_name")


def test_get_low_watermark_returns_default_value_when_row_doesnt_exist(
    spark_session: SparkSession,
    delta_dir: str,
):
    table_name = os.path.basename(delta_dir)
    existing_df = spark_session.createDataFrame(
        [(1, "featue_extraction", table_name)],
        ["low_watermark", "activity", "table_name"],
    )
    existing_df.write.format("delta").save(delta_dir)

    actual_low_watermark = get_low_watermark(
        spark_session, PipelineActivity.PSEUDONYMISATION, delta_dir, table_name
    )

    assert actual_low_watermark == 0


# Test update_watermark
def test_update_watermark_updates_and_doesnt_overwrite_other_existing_data(
    spark_session, delta_dir
):
    table_name = os.path.basename(delta_dir)
    existing_df = spark_session.createDataFrame(
        [
            (5, PipelineActivity.PSEUDONYMISATION.value, table_name),
            (2, "act1", table_name),
            (3, "other", table_name),
        ],
        ["low_watermark", "activity", "table_name"],
    )
    existing_df.write.format("delta").save(delta_dir)

    update_watermark(spark_session, delta_dir, PipelineActivity.PSEUDONYMISATION, 20, table_name)

    actual_df = spark_session.read.format("delta").load(delta_dir)
    expected_df = spark_session.createDataFrame(
        [
            (21, PipelineActivity.PSEUDONYMISATION.value, table_name),
            (2, "act1", table_name),
            (3, "other", table_name),
        ],
        ["low_watermark", "activity", "table_name"],
    )
    assert expected_df.orderBy("table_name").collect() == actual_df.orderBy("table_name").collect()


# Test get_high_watermark
def test_get_high_watermark_returns_high_watermark(spark_session: SparkSession, delta_dir: str):
    v_zero_df = spark_session.createDataFrame(
        [(1, "colv1")],
        ["some_col", "some_col_2"],
    )
    v_zero_df.write.format("delta").mode("overwrite").save(delta_dir)
    v_one_df = spark_session.createDataFrame(
        [(2, "colv2")],
        ["some_col", "some_col_2"],
    )
    v_one_df.write.format("delta").mode("append").save(delta_dir)
    v_two_df = spark_session.createDataFrame(
        [(3, "colv1")],
        ["some_col", "some_col_2"],
    )
    v_two_df.write.format("delta").mode("append").save(delta_dir)

    actual_high_watermark = get_high_watermark(spark_session, delta_dir)

    assert actual_high_watermark == 2


def test_get_high_watermark_returns_high_watermark_when_no_updates_are_done(
    spark_session: SparkSession, delta_dir: str
):
    test_df = spark_session.createDataFrame(
        [(1, "colv1"), (2, "colv2")],
        ["some_col", "some_col_2"],
    )
    test_df.write.format("delta").save(delta_dir)

    actual_high_watermark = get_high_watermark(spark_session, delta_dir)

    assert actual_high_watermark == 0
