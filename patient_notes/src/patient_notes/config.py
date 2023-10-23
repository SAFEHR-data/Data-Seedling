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
from patient_notes.common_types import ColumnType, TableConfig

# Language used for Presidio
PSEUDONYMISATION_LANGUAGE = "en"
# Cognitive Services location
COGNITIVE_LOCATION = "westeurope"
# This string is appended to column name that is hashed
HASH_SUFFIX = "_hashed"
# Salt for hashing Client Index columns
HASH_SALT = "$2b$12$Lrw9ZQwsFNSu/6KGCCTWCu"
# Suffix to apply to column names for new columns created by feature extraction
EXTRACTED_PREFIX = "_extracted"
# Watermarks Delta table name
WATERMARK_TABLE_NAME = "watermarks"
# Number of Spark workers and cores to optimise parallel processing for
WORKER_COUNT = 8
CORE_COUNT = 4

# Entities to anonymise from free text (input for Presidio)
PII_ENTITIES = [
    "PERSON",
    "LOCATION",
    "DATE_TIME",
    "EMAIL_ADDRESS",
    "URL",
    "PHONE_NUMBER",
]


TABLE_CONFIG: dict[str, TableConfig] = {
    "Notes": {
        "column_types": {
            ColumnType.FREE_TEXT: ["NoteText"],
            ColumnType.OTHER_IDENTIFIABLE: ["UserID"],
            ColumnType.DATE_TIME: ["AppointmentDate"],
        },
        "analysed_columns": [
            "NoteID",
        ],
        "primary_keys": ["NoteID"],
    }
}
