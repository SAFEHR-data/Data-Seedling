.PHONY: help

help: ## Show this help
	@echo
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%s\033[0m|%s\n", $$1, $$2}' \
        | column -t -s '|'
	@echo

install-build: ## Installs the required dependencies for building
	python3 -m pip install -e ".[build]"

install-test: ## Installs the required dependencies for testing
	python3 -m pip install -e ".[test]"

install-lint: ## Installs the required dependencies for linting
	python3 -m pip install -e ".[lint]"

build: install-build ## Build the Python wheel(s)
	# SOURCE_DATE_EPOCH is needed to build the wheel idempotently
	SOURCE_DATE_EPOCH=315532800 HTTPS_PROXY= HTTP_PROXY= python3 -m build

test: ## Run Python unit tests including live TA4H tests (requires COGNITIVE_KEYS env var to be set)
	(cd src/patient_notes/tests; pytest -m "not ta4h")

test-without-ta4h: ## Run Python unit tests excluding TA4H tests
	(cd src/patient_notes/tests; pytest)

artifacts: build ## Gather artifacts for sending to Spark
	mkdir -p artifacts
	cp src/patient_notes/entrypoints/*.py artifacts
	cp dist/*.whl artifacts/

clean: ## Clean all built objects
	rm -rf artifacts
	rm -rf dist
	rm -rf src/*.egg-info
	find . -name "*.pyc" -type f -delete

mypy: ## Run mypy (see https://jaredkhan.com/blog/mypy-pre-commit on why it isn't run in pre-commit hook)
	mypy "src"
