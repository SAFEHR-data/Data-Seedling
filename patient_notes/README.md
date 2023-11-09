# Text processing pipeline

## What is this

This is a data pipeline for processing large text datasets for healthcare. It pseudonymizes data and extracts additional data from it using [Microsoft Presidio](https://microsoft.github.io/presidio/) and [Azure Text Analytics for Health](https://learn.microsoft.com/en-us/azure/ai-services/language-service/text-analytics-for-health/overview?tabs=ner). It also shows an approach to writing Spark pipelines in a testable, CI-friendly way that supports efficient incremental data processing.

## How to use this code

### Deploy FlowEHR

First, go through the [Quick Start guide](../docs/quick_start.md) to set up this repository and your FlowEHR instance.

The above link takes you through defining a file `config.local.yaml`. The `patient_notes` pipeline requires some special setup, so there is a file [config.local.yaml](./config.local.yaml) in this directory that contains a sample configuration you could use. Replace the values that have `REPLACE_ME` set to customize your deployment, and leave the rest as-is.

The values that need to be replaced are:
- `flowehr_id`: A unique identifier to apply to all deployed resources (e.g. `myflwr`). Must be 12 characters or less.
- `location` and `cognitive-services-location`: Set to Azure region you are using, e.g. `uksouth`.
- `storage_account_name` and `resource_group_name` in `unity_catalog_metastore` section: Unity Catalog Metastore can only be created once per Azure tenant. If your tenant does not have Unity Catalog Metastore deployed, pick any suitable globally unique value for `storage_account_name` and any suitable name for `resource_group_name`.
- `databricks_account_id`: An ID for Databricks Account (on a tenant level). Follow the official documentation to obtain it: [Locate your Account ID](https://docs.databricks.com/en/administration-guide/account-settings/index.html#locate-your-account-id).
- `cognitive-services-keys` and `cognitive-services-location`: To run the Feature Extraction part of the pipeline, you need to have a Language service deployed in Azure. It is not deployed automatically during FlowEHR deployment, so you'll need to do it manually. See the screenshot below.
- `azure-tenant-id`: Should contain Azure Tenant ID (also sometimes known as Directory ID) for your Azure Tenant.

![Create Language Service](/assets/CreateLanguageService.png)

After deploying FlowEHR, you will need to change one setting for the metrics to be displayed correctly. Head to the Application Insights resource deployed in your resource group, it should have a name like `transform-ai-${flowehr_id}-dev`. Head to `Usage and estimated costs`, click on `Custom metrics (preview)`, and make sure custom metrics are sent to Azure with dimensions enabled:

![Custom Metrics Set Up](/assets/CustomMetricsSetUp.png)

### Demo notebook

Head over to the Databricks service created in your resource group, and import the IngestDummyData.ipynb notebook there.

To run the notebook, you need to create your personal cluster (as the FlowEHR cluster, created as part of FlowEHR deployment, can be only used by ADF instance). To do this, create a cluster of a desired configuration, make sure to select Single user as the Access mode, and copy the sections Spark config and Environment variables from the FlowEHR cluster. See screenshot:

![Create Cluster Advanced Options](/assets/CreateClusterAdvancedOptions.png)

Follow the instructions in the notebook to initialize the test data to run the pipeline.

To trigger the pipeline, head to the ADF instance in the resource group you have deployed in step 1. It will have a name like `adf-${flowehr_id}-dev` and trigger the PatientsPipeline (click on Add Trigger - Trigger Now). See screenshot: 

![Trigger ADF Pipeline](/assets/TriggerPatientNotesPipeline.png)

### Checking logs and metrics

To check the logs, head to the Application Insights service created in your resource group, as described in step 2. There, head to the `Logs` section. To see logs created by the pipeline, type `traces`. See screenshot: 

![Pipeline Logs](/assets/PatientNotesPipelineLogs.png)

To check the metrics dashboard, look for `PatientNotesPipelineStatusDashboard` created in your resource group. See screenshot:

![Pipeline Metrics Dashboard](/assets/PatientNotesMetricsDashboard.png)

> Note: You might want to add another dimensions to the custom split, so that the dashboard shows rows inserted break down by `activity` (`pseudonymisation` or `feature_extraction`), and `operation` (`insert` or `delete`).

![Metrics Custom Split](/assets/MetricsCustomSplit.png)

### Querying data in Unity Catalog

Optionally, you could use SQL Warehouse to query data in Unity Catalog. To do so, you will need to create a SQL Warehouse. Any default settings will do:

![Create SQL Warehouse](/assets/CreateSQLWarehouse.png)

Once it's created, head to SQL Editor view in Databricks and you can write SQL queries to quickly examine the data.

![SQL Editor Query](/assets/SQLEditorQuery.png)

## Design principles

To learn more about the design principles behind this pipeline, head to the [design document](./docs/design_doc.md).

## Code Structure

These are the files that are useful to explore:
- [entrypoint.py](./src/patient_notes/entrypoint.py): Entrypoint for the pipeline, this is where the pipeline starts to run from.
- [transform.py](./src/patient_notes/transform.py): File that defines transformations.
- [Tests](./src/patient_notes/tests/test_transform.py): Tests for the above transformations.
- [Test configuration](./src/patient_notes/tests/conftest.py): Helper fixture using [] for writing unit tests with PySpark
- [db.py](./src/patient_notes/db.py): Helpers for working with Microsoft SQL database
- [datalake.py](./src/patient_notes/datalake.py): File that contains helper methods for working with Data Lake.
- [watermark.py](./src/patient_notes/watermark.py.py): File that has the logic for the watermark algorithm, used for incremental updates.
- [monitoring.py](./src/patient_notes/monitoring.py): Helpers for sending logs and metrics to Azure Monitor.
- [Makefile](./Makefile): Used for command shortcuts, and certain commands are expected to be defined to ensure successful deployment of the pipeline to Azure.
- [pyproject.toml](./pyproject.toml): Defines building of the Python wheel that contains all code defined for the pipeline.
