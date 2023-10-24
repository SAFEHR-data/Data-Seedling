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

import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.udf import UserDefinedFunction
from pyspark_test import assert_pyspark_df_equal
from patient_notes.config import TableConfig
from patient_notes.stages.pseudonymisation.transform import pseudo_transform
from patient_notes.common_types import ColumnType


# Pseudonymisation - Remove column tests
def test_pseudo_transform_removes_multiple_columns(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value", "another text"),
            ("This is a note value", 2, "text value", "another text"),
        ],
        ["NoteText", "ID", "Text", "Another"],
    )
    table: TableConfig = {
        "column_types": {ColumnType.OTHER_IDENTIFIABLE: ["Text", "Another"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [("This is a note value", 1), ("This is a note value", 2)], ["NoteText", "ID"]
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_pseudo_transform_raises_exception_when_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("This is a note value", 1, "text value"),
            ("This is a note value", 2, "text value"),
        ],
        ["NoteText", "ID", "Text"],
    )
    table: TableConfig = {
        "column_types": {ColumnType.OTHER_IDENTIFIABLE: ["ColumnThatDoesntExist"]},
        "analysed_columns": [],
    }

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, "table1", table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


# Pseudonymisation - Free text tests
def test_pseudo_transform_pseudonymises_multiple_free_text_columns(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("John Smith is in London", 1, "This is London"),
            ("Adam is in London", 2, "This is London"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    table: TableConfig = {
        "column_types": {ColumnType.FREE_TEXT: ["NoteText", "OtherText"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [
            ("<PERSON> is in <LOCATION>", 1, "This is <LOCATION>"),
            ("<PERSON> is in <LOCATION>", 2, "This is <LOCATION>"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert expected_df.collect() == output_df.collect()


def test_pseudo_transform_doesnt_throw_error_when_no_columns_are_configured(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [
            ("John Smith is in London", 1, "This is London"),
            ("Adam is in London", 2, "This is London"),
        ],
        ["NoteText", "ID", "OtherText"],
    )
    table: TableConfig = {
        "column_types": {},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)
    assert set(input_df.columns) == set(output_df.columns)
    assert input_df.collect() == output_df.collect()


def test_pseudo_transform_raises_exception_when_free_text_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame(
        [("John Smith is in London", 1), ("Adam is in London", 2)], ["NoteText", "ID"]
    )
    table: TableConfig = {
        "column_types": {ColumnType.FREE_TEXT: ["ColumnThatDoesntExist"]},
        "analysed_columns": [],
    }

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, "table1", table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


# Pseudonymisation - Round Dates tests
def test_pseudo_transform_rounds_date_time(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 3, 4, 5)
    input_df = spark_session.createDataFrame([(1, input_datetime)], ["ID", "SomeDateTimeColumn"])
    table: TableConfig = {
        "column_types": {ColumnType.DATE_TIME: ["SomeDateTimeColumn"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    expected_datetime = datetime.datetime(2023, 8, 16, 3, 0, 0)

    output_datetime = output_df.collect()[0]["SomeDateTimeColumn"]
    assert expected_datetime == output_datetime


def test_pseudo_transform_rounds_date(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 5, 4, 2)
    input_df = spark_session.createDataFrame([(1, input_datetime)], ["ID", "SomeDateColumn"])
    table: TableConfig = {
        "column_types": {ColumnType.DATE: ["SomeDateColumn"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    expected_datetime = datetime.datetime(2023, 8, 1, 0, 0)

    output_datetime = output_df.collect()[0]["SomeDateColumn"]
    assert expected_datetime == output_datetime


def test_pseudo_transform_raises_exception_when_date_column_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = datetime.datetime(2023, 8, 16, 5, 4, 2)
    input_df = spark_session.createDataFrame([(1, input_datetime)], ["ID", "SomeDateTimeColumn"])
    table: TableConfig = {
        "column_types": {ColumnType.DATE: ["ColumnThatDoesntExist"]},
        "analysed_columns": [],
    }

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, "table1", table, presidio_udf)

    assert "ColumnThatDoesntExist" in str(excinfo.value)


def test_pseudo_transform_returns_none_for_dttm_when_col_has_invalid_value(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_datetime = "invalid value"
    input_df = spark_session.createDataFrame([(1, input_datetime)], ["ID", "SomeDateTimeColumn"])
    table: TableConfig = {
        "column_types": {ColumnType.DATE_TIME: ["SomeDateTimeColumn"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    output_datetime = output_df.collect()[0]["SomeDateTimeColumn"]
    assert output_datetime is None


# Pseudonymisation - Hash client id tests
def test_pseudo_transform_hashes_id(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame([(1, "Text1")], ["ID", "Text"])
    table: TableConfig = {
        "column_types": {ColumnType.HASHABLE_ID: ["ID"]},
        "analysed_columns": [],
    }

    output_df = pseudo_transform(input_df, "table1", table, presidio_udf)

    expected_df = spark_session.createDataFrame(
        [("e59cb3f3ffba6255f0f32b278a76f8a44780fde36bb7a1b3428a394ff4c39596", "Text1")],
        ["ID_hashed", "Text"],
    )
    assert set(expected_df.columns) == set(output_df.columns)
    assert_pyspark_df_equal(output_df, expected_df, order_by="Text")


def test_pseudo_transform_raises_exception_when_hashable_id_columns_doesnt_exist(
    spark_session: SparkSession, presidio_udf: UserDefinedFunction
) -> None:
    input_df = spark_session.createDataFrame([(1, "Text1")], ["ID", "Text"])
    table: TableConfig = {
        "column_types": {ColumnType.HASHABLE_ID: ["ThisColumnDoesntExist"]},
        "analysed_columns": [],
    }

    with pytest.raises(KeyError) as excinfo:
        pseudo_transform(input_df, "table1", table, presidio_udf)

    assert "ThisColumnDoesntExist" in str(excinfo.value)
