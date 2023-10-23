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

import json


class SchemaValidationException(Exception):
    """Exception for schema validation errors."""

    def __init__(self, error_dictionary: dict[str, list[str] | dict[str, dict[str, list[str]]]]):
        """Serialize error message as a string"""
        super().__init__(error_dictionary)

    def __str__(self):
        return json.dumps(self.args[0])
