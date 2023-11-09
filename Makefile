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

.PHONY: help

define all-pipelines  # Arguments: <command>
	find . -maxdepth 1 -mindepth 1 -type d | xargs -i bash -c 'if [ -f "{}/pipeline.json" ]; then echo {}; fi' | xargs -i make -C {} $(1)
endef


help: ## Show this help
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m|%s\n", $$1, $$2}' \
        | column -t -s '|'
	@echo

artifacts: ## Create all pipeline artifacts
	$(call all-pipelines,artifacts)

install: ## Install all pre-requisites for all pipelines
	$(call all-pipelines,install)

test: ## Test all pipelines
	$(call all-pipelines,test)

mypy: ## Call mypy on all pipelines
	$(call all-pipelines,mypy)

lint: ## Call lint on all pipelines (will also call mypy)
	$(call all-pipelines,lint)
