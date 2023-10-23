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
from pyspark import SparkConf

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from patient_notes.stages.pseudonymisation.presidio import (
    broadcast_presidio_with_anonymise_udf,
)
from sqlalchemy import Connection, create_engine
from testcontainers.mssql import SqlServerContainer

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

    conf = SparkConf()
    conf.set("spark.jars.repositories", "https://mmlspark.azureedge.net/maven")
    conf.set("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
    conf.set("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
    conf.set("spark.sql.session.timeZone", "UTC")
    conf.set("spark.secret.datalake-uri", TEST_DATALAKE_URI)
    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.executor.cores", "1")
    conf.set("spark.executor.instances", "1")
    conf.set("spark.sql.shuffle.partitions", "1")
    conf.set("spark.driver.memory", "8g")
    conf.set("spark.executor.memory", "8g")
    conf.set(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    conf.set("spark.databricks.delta.properties.defaults.enableChangeDataFeed", "true")

    builder = SparkSession.builder.master("local[*]").appName("test").config(conf=conf)
    session = configure_spark_with_delta_pip(
        builder,
        [
            "com.microsoft.sqlserver:mssql-jdbc:12.4.0.jre11",
            "com.microsoft.azure:synapseml_2.12:0.11.3",
        ],
    ).getOrCreate()

    yield session

    session.stop()


@pytest.fixture
def mssql_container() -> Iterator[SqlServerContainer]:
    container = SqlServerContainer(dialect="mssql+pymssql")

    container.start()
    yield container
    container.stop()


@pytest.fixture
def mssql_connection(mssql_container: SqlServerContainer) -> Iterator[Connection]:
    with create_engine(mssql_container.get_connection_url()).connect() as conn:
        yield conn


@pytest.fixture
def delta_dir() -> Generator:
    path = f"/tmp/delta_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def bronze_dir() -> Generator:
    path = f"/tmp/bronze_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture(scope="session")
def presidio_udf(spark_session: SparkSession) -> Generator:
    udf = broadcast_presidio_with_anonymise_udf(spark_session)
    yield udf


@pytest.fixture
def internal_dir() -> Generator:
    path = f"/tmp/internal_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def silver_dir() -> Generator:
    path = f"/tmp/silver_{''.join(random.choice(string.ascii_lowercase) for i in range(10))}"

    yield path

    shutil.rmtree(path, ignore_errors=True)
