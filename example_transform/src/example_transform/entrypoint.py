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

from pyspark.sql.session import SparkSession

from example_transform.db import feature_store_config, save_feature_store_table
from example_transform.monitoring import initialize_logging
from example_transform.transform import example_transform


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    initialize_logging()

    df = spark_session.createDataFrame([(1,), (2,), (3,), (2,), (3,)], ["value"])
    # This is an example of how transform from a built Python wheel library
    # will be used in the entrypoint pipeline
    out_df = example_transform(df)

    # This shows how the result of the transformation can be saved in Microsoft SQL Feature Store
    output_db_config = feature_store_config(spark_session)
    save_feature_store_table(output_db_config, out_df, "example_table")
