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
import time
from typing import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    """Provide SparkSession fixture to be used in tests.

    The fixture can be used in the same way as spark object is used in the cluster.
    """
    # Used to ensure the tested python environment is using UTC like the Spark
    # session so that there are no differences between timezones.
    os.environ["TZ"] = "UTC"
    time.tzset()

    session = (
        SparkSession.builder.master("local[4]")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()
