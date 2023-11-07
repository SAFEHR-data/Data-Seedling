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

import abc
import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from helloworld.monitoring import send_rows_updated_metric


@dataclass
class DatabaseConfiguration:
    """
    Dataclass to hold configuration of the database you are connecting to.

    Attributes:
    host: str
        Fully qualified domain name to connect to, e.g. example.database.windows.net
    port: str
        Port to connect to, e.g. 1433
    database: str
        Database name to use, e.g. db
    user: str
        Username to use to connect, e.g. admin
    password: str
        Password to use to connect
    """

    host: str
    port: int
    database: str
    user: str
    password: str

    @abc.abstractmethod
    def connection_string(self) -> str:
        """
        Constructs and returns a connection string from attributes.

        Returns:
            str: URL for database server connection.
        """
        raise NotImplementedError()


@dataclass
class SqlServerConfiguration(DatabaseConfiguration):
    """
    Specific configuration for Microsoft SQL Server.

    Attributes:
    use_aad_service_principal: bool
        Whether to use Service Principal based auth.
        If set to true, pass in App ID as the `user` attribute,
        and App Secret as `password` attribute.
    """

    use_aad_service_principal: bool = False

    def connection_string(self) -> str:
        """
        Constructs and returns a connection string from attributes.
        """
        connection_string = (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"database={self.database};"
            "encrypt=true;"
            "trustServerCertificate=false;"
            "loginTimeout=30;"
        )
        if self.use_aad_service_principal:
            connection_string += "Authentication=ActiveDirectoryServicePrincipal"
        return connection_string


def feature_store_config(spark: SparkSession) -> DatabaseConfiguration:
    """
    :returns: A DatabaseConfiguration object that can be used
        in `save_feature_store_table` to write to Feature Store.
    """
    host = spark.conf.get("spark.secret.feature-store-fqdn")
    database = spark.conf.get("spark.secret.feature-store-database")
    user = spark.conf.get("spark.secret.feature-store-app-id")
    password = spark.conf.get("spark.secret.feature-store-app-secret")
    if not host or not database or not user or not password:
        raise ValueError("Secrets for Feature store configuation must be set")

    return SqlServerConfiguration(
        host=host,
        port=1433,
        database=database,
        user=user,
        password=password,
        use_aad_service_principal=True,
    )


def save_feature_store_table(
    config: DatabaseConfiguration, df: DataFrame, table_name: str
) -> None:
    """
    Saves df as a table under table_name in Feature Store.
    It also increments metric for number of rows_inserted which can be observed on a dashboard
    (see README.md)

    :param config: Config object of type DatabaseConfiguration
    :param df: PySpark DataFrame to save
    :param table_name: Table name to save the table (note that database name comes from config)

    """
    send_rows_updated_metric(value=df.count(), tags={"table_name": table_name})

    writer = (
        df.write.format("jdbc")
        .mode("append")
        .option("url", config.connection_string())
        .option("dbtable", table_name)
        .option("user", config.user)
        .option("password", config.password)
    )
    writer.save()

    logging.info(f"Written {df.count()} rows into table {table_name}")
