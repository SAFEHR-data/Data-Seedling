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
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark_test import assert_pyspark_df_equal
from patient_notes.config import EXTRACTED_PREFIX, TableConfig
from patient_notes.stages.feature_extraction import extract_features
from patient_notes.common_types import ColumnType

FREETEXT_TABLE_CONFIG_SINGLE_COL: TableConfig = {
    "column_types": {ColumnType.FREE_TEXT: ["NoteText"]},
    "analysed_columns": [],
    "primary_keys": ["ID"],
}

FREETEXT_TABLE_CONFIG_MULTIPLE_COLS: TableConfig = {
    "column_types": {ColumnType.FREE_TEXT: ["NoteText", "MoreNoteText"]},
    "analysed_columns": [],
    "primary_keys": ["ID"],
}

SAMPLE_DATAFRAME_MULTIPLE_COLS = (
    [
        ("This is a note value", 1, "This is another note"),
        ("This is a note value", 2, "This is another note"),
    ],
    ["NoteText", "ID", "MoreNoteText"],
)


def test_extract_features_iterates_over_freetext_columns(spark_session: SparkSession, mocker):
    df = spark_session.createDataFrame(*SAMPLE_DATAFRAME_MULTIPLE_COLS)

    mock_ta4h = mocker.patch(
        "patient_notes.stages.feature_extraction.AnalyzeHealthText",
        # Mock return value for transform method (expand kwarg from dict due to dot)
        return_type=mocker.Mock(
            **{"transform.return_value": df.withColumn("extracted", lit("extracted"))}
        ),
    )

    extract_features(df, "table1", FREETEXT_TABLE_CONFIG_MULTIPLE_COLS, ["cog_key"])

    # TA4H object should be created twice (one per column)
    assert mock_ta4h.call_count == 2


def test_extract_features_skips_tables_without_column_types(spark_session: SparkSession, mocker):
    df = spark_session.createDataFrame(*SAMPLE_DATAFRAME_MULTIPLE_COLS)

    mock_ta4h = mocker.patch(
        "patient_notes.stages.feature_extraction.AnalyzeHealthText",
        return_type=mocker.Mock(
            **{"transform.return_value": df.withColumn("extracted", lit("extracted"))}
        ),
    )

    table_config_without_column_types: TableConfig = {
        "analysed_columns": [],
    }

    processed_df = extract_features(df, "table1", table_config_without_column_types, ["cog_key"])

    # TA4H object should not be created
    assert mock_ta4h.call_count == 0

    # Dataframe should be returned unmodified
    assert_pyspark_df_equal(df, processed_df)


def test_extract_features_skips_tables_without_freetext_columns(
    spark_session: SparkSession, mocker
):
    df = spark_session.createDataFrame(*SAMPLE_DATAFRAME_MULTIPLE_COLS)

    mock_ta4h = mocker.patch(
        "patient_notes.stages.feature_extraction.AnalyzeHealthText",
        return_type=mocker.Mock(
            **{"transform.return_value": df.withColumn("extracted", lit("extracted"))}
        ),
    )

    table_config_without_freetext_columns: TableConfig = {
        "column_types": {ColumnType.CLIENT_ID: ["ID"]},
        "analysed_columns": [],
    }

    processed_df = extract_features(
        df, "table1", table_config_without_freetext_columns, ["cog_key"]
    )

    # TA4H object should not be created
    assert mock_ta4h.call_count == 0

    # Dataframe should be returned unmodified
    assert_pyspark_df_equal(df, processed_df)


def test_multiple_cog_keys_are_used_if_df_is_100_rows_or_more(spark_session: SparkSession, mocker):
    df = spark_session.createDataFrame(
        [("This is a note value", i) for i in range(100)], ["NoteText", "ID"]
    )

    mock_analyse = mocker.patch(
        "patient_notes.stages.feature_extraction.analyse",
        return_value=df.withColumn(f"NoteText{EXTRACTED_PREFIX}", lit("extracted")),
    )

    cog_keys = ["cog_key1", "cog_key2", "cog_key3"]

    processed_df = extract_features(df, "table1", FREETEXT_TABLE_CONFIG_SINGLE_COL, cog_keys)

    # analyse() should be called once per key
    assert mock_analyse.call_count == len(cog_keys)

    # analyse() should be called with different keys each time
    for i, call in enumerate(mock_analyse.call_args_list):
        assert call.args[2] == cog_keys[i]

    # Dataframe should be merged back together and still have 100 rows
    assert processed_df.count() == 100


def test_random_cog_key_used_if_df_is_less_than_100_rows(spark_session: SparkSession, mocker):
    df = spark_session.createDataFrame(
        [("This is a note value", i) for i in range(99)], ["NoteText", "ID"]
    )

    mock_analyse = mocker.patch(
        "patient_notes.stages.feature_extraction.analyse",
        return_value=df.withColumn(f"NoteText{EXTRACTED_PREFIX}", lit("extracted")),
    )

    cog_keys = ["cog_key1", "cog_key2", "cog_key3"]

    extract_features(df, "table1", FREETEXT_TABLE_CONFIG_SINGLE_COL, cog_keys)

    # analyse() should be called once
    assert mock_analyse.call_count == 1

    # analyse() should be called with a random key
    assert mock_analyse.call_args[0][2] in cog_keys


@pytest.mark.ta4h
def test_live_ta4h_service_extracts_expected_features(spark_session: SparkSession, request):
    extracted_col = f"NoteText{EXTRACTED_PREFIX}"
    test_dir = os.path.dirname(request.module.__file__)
    parquet_path = os.path.join(test_dir, "ta4h_test_data.snappy.parquet")
    df = spark_session.read.parquet(parquet_path)

    cognitive_keys_string = os.environ.get("COGNITIVE_KEYS")

    if not cognitive_keys_string:
        raise ValueError("Missing COGNITIVE_KEYS environment variable")

    cog_keys = cognitive_keys_string.split(";")

    # Extract features using live TA4H endpoint
    processed_df = extract_features(df, "table1", FREETEXT_TABLE_CONFIG_SINGLE_COL, cog_keys)

    # Check that extracted column is present
    assert extracted_col in processed_df.columns

    # Flatten entities for easier comparison
    entities = processed_df.select(
        "NoteNum",
        f"{extracted_col}.document.entities.text",
        f"{extracted_col}.document.entities.category",
    )

    # Define expected entities for the first row
    expected_entities = {
        "delusional beliefs": "SymptomOrSign",
        "fluid intake": "SymptomOrSign",
        "this afternoon": "Time",
        "slightly": "ConditionQualifier",
        "distracted": "SymptomOrSign",
        "brittle": "SymptomOrSign",
    }

    # Collect the first row
    actual_entities = entities.collect()[0]

    # Check that all expected entities are present in the extraction output
    for expected_entity_text, expected_entity_category in expected_entities.items():
        entity_index = actual_entities[1].index(expected_entity_text)

        # Check the expected entity is in list
        assert entity_index != -1

        # Check that the entity maps to the expected category
        assert actual_entities[2][entity_index] == expected_entity_category

    # Now look at the relations (between entities) to ensure they are as expected
    relations = processed_df.select(
        "NoteNum",
        f"{extracted_col}.document.relations.relationType",
        f"{extracted_col}.document.relations.entities",
    )

    # Define expected relations for first row
    expected_relations = [
        "QualifierOfCondition",
        "TimeOfCondition",
    ]

    # Collect the first row
    actual_relations = relations.collect()[0]

    # Check that all expected relations are present in the extraction output
    assert all(
        expected_relation in actual_relations[1] for expected_relation in expected_relations
    )
