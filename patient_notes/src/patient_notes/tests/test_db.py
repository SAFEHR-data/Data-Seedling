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

from sqlalchemy import CursorResult, create_engine, text
from testcontainers.mssql import SqlServerContainer


def test_mssql_connection(mssql_container: SqlServerContainer) -> None:
    with create_engine(mssql_container.get_connection_url()).connect() as conn:
        result: CursorResult = conn.execute(text("select @@servicename"))
        row = result.fetchone()

        assert row is not None
        assert row[0] == "MSSQLSERVER"
        assert mssql_container.get_logs() != ""
