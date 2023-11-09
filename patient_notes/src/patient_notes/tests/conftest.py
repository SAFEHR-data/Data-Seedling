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
import random
import shutil
import string
import time
from typing import Generator, Iterator

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from patient_notes.stages.pseudonymisation.presidio import (
    broadcast_presidio_with_anonymise_udf,
)

TEST_DATALAKE_URI = "test.dfs.core.windows.net"


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "ta4h: mark test as calling live Text Analytics For Health endpoint"
    )


@pytest.fixture(scope="session")
def spark_session() -> Iterator[SparkSession]:
    # Used to ensure the tested python environment is using UTC like the Spark
    # session so that there are no differences between timezones.
    os.environ["TZ"] = "UTC"
    time.tzset()

    builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.secret.datalake-uri", TEST_DATALAKE_URI)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.executor.cores", "1")
        .config("spark.executor.instances", "1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()

    yield session

    session.stop()


@pytest.fixture(scope="session")
def presidio_udf(spark_session: SparkSession) -> Generator:
    udf = broadcast_presidio_with_anonymise_udf(spark_session)
    yield udf


def random_temp_path(path_prefix: str) -> str:
    return (
        f"/tmp/{path_prefix}_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"
    )


def cleanup_temp_path(path: str):
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def delta_dir() -> Generator:
    path = random_temp_path(path_prefix="delta")
    yield path
    cleanup_temp_path(path)


@pytest.fixture
def bronze_dir() -> Generator:
    path = random_temp_path(path_prefix="bronze")
    yield path
    cleanup_temp_path(path)


@pytest.fixture
def silver_dir() -> Generator:
    path = random_temp_path(path_prefix="silver")
    yield path
    cleanup_temp_path(path)


@pytest.fixture
def internal_dir() -> Generator:
    path = random_temp_path(path_prefix="internal")
    yield path
    cleanup_temp_path(path)
