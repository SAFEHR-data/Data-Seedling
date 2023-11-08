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
from patient_notes.config import TABLE_CONFIG, WORKER_COUNT, CORE_COUNT, WATERMARK_TABLE_NAME
from patient_notes.common_types import PipelineActivity
from patient_notes.datalake import (
    DatalakeZone,
    construct_uri,
    read_delta_table_update,
    write_delta_table_update,
)
from patient_notes.monitoring import initialize_logging
from patient_notes.stages.pseudonymisation.presidio import (
    broadcast_presidio_with_anonymise_udf,
)
from patient_notes.stages.pseudonymisation.transform import pseudo_transform

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    initialize_logging(export_interval_seconds=1.0)

    anonymise_udf = broadcast_presidio_with_anonymise_udf(spark_session)

    for table_name, table_config in TABLE_CONFIG.items():
        logging.info(f"Processing table for pseudonymisation: {table_name}")

        watermark_url = construct_uri(spark_session, DatalakeZone.INTERNAL, WATERMARK_TABLE_NAME)

        # Read change from bronze
        df, high_watermark = read_delta_table_update(
            spark_session,
            PipelineActivity.PSEUDONYMISATION,
            construct_uri(spark_session, DatalakeZone.BRONZE, table_name),
            watermark_url,
            table_name,
        )

        if df.isEmpty():
            logging.warning(f"Skipping pseudonymisation for table {table_name} as DF is empty")
        else:
            # Increase partition count for parallel processing
            initial_partitions = df.rdd.getNumPartitions()
            df = df.repartition(max(WORKER_COUNT * CORE_COUNT, initial_partitions))

            # Pseudonymise
            df = pseudo_transform(df, table_name, table_config, anonymise_udf)

        # Write change to silver
        write_delta_table_update(
            spark_session,
            construct_uri(spark_session, DatalakeZone.SILVER, table_name),
            df,
            table_config.get("primary_keys", []),
            watermark_url,
            high_watermark,
            table_name,
            PipelineActivity.PSEUDONYMISATION,
        )

        logging.info(
            f"Wrote pseudonymisation outputs to {table_name} in Datalake silver zone"
            f" ({df.count()} rows)."
        )
