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

from enum import Enum
from typing_extensions import NotRequired
from typing import TypedDict


class ColumnType(Enum):
    """Define column type that has a specific treatment during pseudonymisation."""

    FREE_TEXT = "free_text"
    OTHER_IDENTIFIABLE = "other_identifiable"
    HASHABLE_ID = "hashable_id"
    DATE_TIME = "date_time"
    DATE = "date"
    DATE_OF_BIRTH = "date_of_birth"


class DateTimeRoundOpt(Enum):
    """Define rounding options for the pyspark.sql.functions.date_trunc function"""

    MONTH = "month"
    HOUR = "hour"


class DatalakeZone(Enum):
    """Defines Datalake zones"""

    # Bronze contains only raw data.
    BRONZE = "bronze"
    # Silver layer is more secure (contains pseudonymised data).
    SILVER = "silver"
    # Gold layer contains enriched datasets from silver.
    GOLD = "gold"
    # Internal store for pipeline utilities (e.g. watermarks).
    INTERNAL = "internal"


class TableConfig(TypedDict):
    """Defines config for a specific table"""

    column_types: NotRequired[dict[ColumnType, list[str]]]
    analysed_columns: list[str]
    primary_keys: NotRequired[list[str]]


class WatermarkColumn(Enum):
    ACTIVITY = "activity"
    LOW_WATERMARK = "low_watermark"
    TABLE_NAME = "table_name"


class PipelineActivity(Enum):
    PSEUDONYMISATION = "pseudonymisation"
    FEATURE_EXTRACTION = "feature_extraction"


class ChangeType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    PRE_UPDATE = "update_preimage"
    POST_UPDATE = "update_postimage"


# Referene https://docs.databricks.com/en/delta/delta-change-data-feed.html
class ReservedColumns(Enum):
    CHANGE = "_change_type"
    COMMIT = "_commit_version"
    COMMIT_TIME = "_commit_timestamp"
