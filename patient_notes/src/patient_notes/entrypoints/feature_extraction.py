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

from pyspark.sql.session import SparkSession
from patient_notes.config import TABLE_CONFIG, WATERMARK_TABLE_NAME
from patient_notes.datalake import (
    DatalakeZone,
    construct_uri,
    read_delta_table_update,
    write_delta_table_update,
    create_table_in_unity_catalog,
)
from patient_notes.common_types import PipelineActivity
from patient_notes.monitoring import initialize_logging
from patient_notes.stages.feature_extraction import extract_features


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    initialize_logging(export_interval_seconds=1.0)

    # Get cognitive key(s) from Spark secret
    cognitive_keys_string = spark_session.conf.get("spark.secret.cognitive-services-keys")

    if not cognitive_keys_string:
        raise ValueError("Missing cognitive-services-keys in Spark config")

    unity_catalog_catalog_name = spark_session.conf.get("spark.secret.unity-catalog-catalog-name")
    unity_catalog_schema_name = spark_session.conf.get("spark.secret.unity-catalog-schema-name")

    if not unity_catalog_catalog_name or not unity_catalog_schema_name:
        raise ValueError(
            "Missing unity-catalog-catalog-name or unity-catalog-schema-name in Spark config"
        )

    # Split into a list of keys (if multiple keys are provided and separated by a semicolon)
    cognitive_keys = cognitive_keys_string.split(";")

    for table_name, table_config in TABLE_CONFIG.items():
        logging.info(f"Processing table for feature extraction: {table_name}")

        watermark_url = construct_uri(spark_session, DatalakeZone.INTERNAL, WATERMARK_TABLE_NAME)

        # Read change from silver
        df, high_watermark = read_delta_table_update(
            spark_session,
            PipelineActivity.FEATURE_EXTRACTION,
            construct_uri(spark_session, DatalakeZone.SILVER, table_name),
            watermark_url,
            table_name,
        )

        if df.isEmpty():
            logging.warning(f"Skipping feature extraction for table {table_name} as DF is empty")
        else:
            # Extract features to new column
            df = extract_features(df, table_name, table_config, cognitive_keys)

        # Write outputs to gold zone
        gold_uri = construct_uri(spark_session, DatalakeZone.GOLD, table_name)
        write_delta_table_update(
            spark_session,
            gold_uri,
            df,
            table_config.get("primary_keys", []),
            watermark_url,
            high_watermark,
            table_name,
            PipelineActivity.FEATURE_EXTRACTION,
        )

        create_table_in_unity_catalog(
            spark_session,
            gold_uri,
            unity_catalog_catalog_name,
            unity_catalog_schema_name,
            table_name,
        )

        logging.info(
            f"Wrote feature extraction outputs to {table_name} in Datalake gold zone"
            f" ({df.count()} rows)."
        )
