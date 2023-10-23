from enum import Enum
from typing_extensions import NotRequired
from typing import TypedDict


class ColumnType(Enum):
    """Define column type that has a specific treatment during pseudonymisation."""

    FREE_TEXT = "free_text"
    OTHER_IDENTIFIABLE = "other_identifiable"
    CLIENT_ID = "client_id"
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


class PipelineActivity(Enum):
    PSEUDONYMISATION = "pseudonymisation"
    FEATURE_EXTRACTION = "feature_extraction"


class ChangeType(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    PRE_UPDATE = "update_preimage"
    POST_UPDATE = "update_postimage"
