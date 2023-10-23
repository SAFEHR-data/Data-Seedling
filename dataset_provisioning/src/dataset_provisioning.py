#!/bin/bash
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
# limitations under the License.

import argparse
import base64
import datetime
from typing import Any

from pyspark.sql.session import SparkSession


# These constants need to be set accordingly to the TRE Workspace template used
# (see README.md)
DEFAULT_CONTAINER_NAME = "datalake"
DEFAULT_STORAGE_ACCOUNT_PREFIX = "stgws"


def parse_args() -> Any:
    """
    Uses argparse to parse arguments from command line.

    :returns: An object with parsed arguments as attributes.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--query_base64",
        type=str,
        help=(
            "Query to run for dataset extraction, encoded in base64."
            'Example of a query: "SELECT * from catalog.schema.table"'
        ),
    )
    parser.add_argument(
        "--dataset_name",
        type=str,
        help="Name of the dataset to provision in TRE Workspace storage",
    )
    parser.add_argument(
        "--workspace_id", type=str, help="TRE Workspace ID to provision the dataset in"
    )

    return parser.parse_args()


def set_up_auth_for_storage_account(spark: SparkSession, storage_account: str) -> None:
    """
    Sets up authentication to be able to use storage account in Workspace.
    Required secrets (AAD Tenant ID, App ID and App Secret) are deployed with FlowEHR

    Parameters:
    :param spark: Spark session
    :param storage_account: Storage account to set up auth for
    """
    directory_id = spark.conf.get("spark.secret.azure-tenant-id")
    app_id = spark.conf.get("spark.secret.external-connection-app-id")
    app_secret = spark.conf.get("spark.secret.exernal-connection-app-secret")

    if not app_id or not app_secret:
        raise ValueError(
            "external-connection-app-id and exernal-connection-app-secret secrets"
            "must be set on the cluster"
        )

    spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net",
        app_id,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net",
        app_secret,
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{directory_id}/oauth2/token",
    )


def get_storage_for_workspace(workspace_id: str) -> str:
    """
    Using a naming convention, returns the expected name of the storage account
    in the TRE Workspace.

    :param workspace_id: A Workspace ID (GUID)

    :returns: Returns predefined name of the Workspace storage account
    """
    return f"{DEFAULT_STORAGE_ACCOUNT_PREFIX}{workspace_id[-4:]}"


if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    args = parse_args()

    if not args.dataset_name or not args.workspace_id or not args.query_base64:
        print("No arguments set")
        exit(0)

    workspace_storage = get_storage_for_workspace(args.workspace_id)
    set_up_auth_for_storage_account(spark_session, workspace_storage)

    query = base64.b64decode(args.query_base64).decode("ascii")
    df = spark_session.sql(query)

    now = datetime.datetime.now()
    str_now = now.strftime("%Y-%m-%d-%H-%M-%S")

    df.write.format("parquet").mode("overwrite").save(
        f"abfss://{DEFAULT_CONTAINER_NAME}@{workspace_storage}"
        f".dfs.core.windows.net/{args.dataset_name}/{str_now}"
    )
