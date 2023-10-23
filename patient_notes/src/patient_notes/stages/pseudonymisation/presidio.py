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

from presidio_analyzer import AnalyzerEngine
from presidio_anonymizer import AnonymizerEngine
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from patient_notes.config import PII_ENTITIES, PSEUDONYMISATION_LANGUAGE


def anonymise(text: str, broadcasted_analyser, broadcasted_anonymiser) -> str | None:
    """Anonymise text entry with analyser and anonymiser broadcast by Spark"""
    if text:
        analyser = broadcasted_analyser.value
        anonymiser = broadcasted_anonymiser.value
        results = analyser.analyze(
            text=text, entities=PII_ENTITIES, language=PSEUDONYMISATION_LANGUAGE
        )
        return anonymiser.anonymize(text=text, analyzer_results=results).text
    else:
        return None


def broadcast_presidio_with_anonymise_udf(spark_session: SparkSession):
    """Broadcast Presidio engines across Spark cluster and create anonymise UDF"""
    broadcasted_analyser = spark_session.sparkContext.broadcast(AnalyzerEngine())
    broadcasted_anonymiser = spark_session.sparkContext.broadcast(AnonymizerEngine())

    anonymise_udf = udf(
        lambda text: anonymise(text, broadcasted_analyser, broadcasted_anonymiser),
        StringType(),
    )
    return anonymise_udf
