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

import logging

from pyspark.sql.session import SparkSession
from patient_notes.monitoring import initialize_logging
from patient_notes.datalake import (
    DatalakeZone,
    construct_uri,
    overwrite_delta_table,
)

if __name__ == "__main__":
    spark_session = SparkSession.builder.getOrCreate()
    initialize_logging()

    # Here, for demo purposes, we save a test dataframe onto Delta lake.
    # This code is to be replaced with a project-specific ingest process.
    table_name = "Notes"
    df = spark_session.createDataFrame(
        [
            (
                1,
                (
                    "Jonathan appeared agitated during today's session, reporting heightened"
                    " irritability and difficulty focusing at work. He shared concerns about"
                    " persistent insomnia and a sense of impending doom. Jonathan is currently"
                    " prescribed lorazepam (1mg as needed) for anxiety management, and we"
                    " discussed incorporating relaxation techniques into his daily routine. A"
                    " follow-up session is scheduled for November 19, 2023."
                ),
                483215,
                "November 5, 2023",
            ),
            (
                2,
                (
                    "Olivia conveyed a persistent low mood and feelings of guilt related to a"
                    " recent personal loss. She described disruptions in her sleep pattern and"
                    " appetite. Olivia is not currently taking any medications. We explored grief"
                    " coping strategies and established a plan for ongoing support. Next session:"
                    " November 22, 2023."
                ),
                176824,
                "November 8, 2023",
            ),
            (
                3,
                (
                    "Michael shared concerns about intrusive thoughts and compulsive behaviors"
                    " indicative of obsessive-compulsive disorder. He is currently prescribed"
                    " fluvoxamine (100mg daily). We discussed cognitive-behavioral strategies to"
                    " manage obsessive thoughts. A follow-up is scheduled for November 24, 2023."
                ),
                742309,
                "November 10, 2023",
            ),
            (
                4,
                (
                    "Jasmine expressed feelings of overwhelming sadness and loss of interest in"
                    " activities she once enjoyed. She is prescribed escitalopram (10mg daily) for"
                    " depression. We discussed the importance of self-care and scheduled a"
                    " follow-up for November 29, 2023."
                ),
                589124,
                "November 15, 2023",
            ),
            (
                5,
                (
                    "Lucas described acute anxiety related to social situations, impacting his"
                    " daily life. He is currently taking sertraline (50mg daily). We explored"
                    " exposure therapy techniques and set goals for gradual desensitization. The"
                    " next session is scheduled for December 2, 2023."
                ),
                317468,
                "November 18, 2023",
            ),
            (
                6,
                (
                    "Zoe reported heightened stress levels due to academic pressures and"
                    " challenges with time management. She is not currently on medication. We"
                    " discussed stress reduction techniques and established strategies for"
                    " improved work-life balance. Follow-up session: December 6, 2023."
                ),
                864502,
                "November 22, 2023",
            ),
            (
                7,
                (
                    "Ryan expressed symptoms of attention deficit hyperactivity disorder (ADHD),"
                    " including difficulty sustaining attention and impulsivity. He is prescribed"
                    " methylphenidate (20mg daily). We discussed behavioral strategies to manage"
                    " ADHD symptoms. Next session: December 9, 2023."
                ),
                125739,
                "November 25, 2023",
            ),
            (
                8,
                (
                    "Ava shared concerns about recurrent panic attacks, particularly in crowded"
                    " spaces. She is prescribed clonazepam (0.5mg as needed). We discussed"
                    " breathing exercises and exposure therapy. Follow-up scheduled for December"
                    " 13, 2023"
                ),
                650821,
                "November 29, 2023",
            ),
            (
                9,
                (
                    "Elijah reported persistent feelings of emptiness and identity disturbance. He"
                    " is prescribed aripiprazole (5mg daily). We discussed the importance of mood"
                    " tracking and established goals for emotional regulation. Next session:"
                    " December 16, 2023."
                ),
                294617,
                "December 2, 2023",
            ),
            (
                10,
                (
                    "Sophia discussed challenges with impulse control and emotional dysregulation."
                    " She is currently prescribed lamotrigine (50mg daily). We explored"
                    " dialectical behavior therapy (DBT) skills to enhance emotion regulation."
                    " Follow-up scheduled for December 19, 2023."
                ),
                817403,
                "December 5, 2023",
            ),
        ],
        ["NoteID", "NoteText", "UserID", "AppointmentDate"],
    )

    # Write to silver
    overwrite_delta_table(df, construct_uri(spark_session, DatalakeZone.BRONZE, table_name))
    logging.info(
        f"Wrote pseudonymisation outputs to {table_name} in Datalake silver zone"
        f" ({df.count()} rows)."
    )
