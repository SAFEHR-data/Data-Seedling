# Example Transform pipeline

## What is this

This is an example data pipeline that creates a fake DataFrame, then runs a basic transformation on it and saves the result in the Microsoft SQL database (feature store). It shows you how you can write, test and deploy a basic data transformation using FlowEHR, package data transformation code into a Python wheel, and send metrics and logs to Azure Monitor.

## Quick start

Make sure to follow the [quick start guide](../docs/quick_start.md) on working with data pipelines in FlowEHR.

## Code Structure

These are the files that are useful to explore:
- [entrypoint.py](./src/example_transform/entrypoint.py): Entrypoint for the pipeline, this is where the pipeline starts to run from.
- [transform.py](./src/example_transform/transform.py): File that defines transformations.
- [Tests](./src/example_transform/tests/test_transform.py): Tests for the above transformations.
- [Test configuration](./src/example_transform/tests/conftest.py): Helper fixture using [] for writing unit tests with PySpark
- [db.py](./src/example_transform/db.py): Helpers for working with Microsoft SQL database
- [monitoring.py](./src/example_transform/monitoring.py): Helpers for sending logs and metrics to Azure Monitor.
- [Makefile](./Makefile): Used for command shortcuts, and certain commands are expected to be defined to ensure successful deployment of the pipeline to Azure.
- [pyproject.toml](./pyproject.toml): Defines building of the Python wheel that contains all code defined for the pipeline.
