# Text Pipeline Design

## Context

This document summarises key design decisions that were implemented while working on customer engagements involving large-volume text processing. It documents key technologies, algorithms and processes implemented to manage the data pipelines.

The document outlines:
- How the data will be stored
- How data will be processed incrementally so that each time new data is added to the pipeline, only that incremental update is processed
- How the pipeline will be triggered

## Requirements

- Cost efficiency: During normal operation, the pipeline processes data only once.
- Maintainability: Easy to set up monitoring and fetch logs, and easy to find and fix any errors, without excessive reprocessing
- Testability: It should be possible to unit-test data processing code
- Developer efficiency: It should be easy to run tests locally, and package and deploy any code to the cloud environment

## Diagram

![Data Pipeline Diagram.png](/assets/DataPipelineDiagram.png)

In this pipeline, we have implemented a medallion architecture. Bronze contains raw data with minimal processing, Silver contains processed data (in our case it is pseudonymized), and Gold contains data enriched with features extracted from the dataset.

### Bronze (Ingestion)

Any external data ingested landed in Bronze, retaining their schema.
Incremental processing is discussed [later in this document](#incremental-data-processing-bronze).
This document assumes that each row in source tables has a Primary Key, uniquely identifying each row.

In this example pipeline, we don't have a pipeline stage for ingestion. Instead, please refer to the [notebook](./Demo.ipynb) that shows how to insert example data into the Bronze layer.

### Silver (Pseudonymisation)

All tables that are present in Bronze get pseudonymized and land in Silver. Therefore, all tables have the same schema as they had in Bronze, but all content is pseudonymized.

### Gold (Feature Extraction and Feature Development)

Tables in Gold contain additional columns as extracted during Feature Extraction.

## Storage

### Delta Tables

Delta Lake is open-source software that extends Parquet data files with a file-based transaction log for ACID transactions and scalable metadata handling. Delta Tables are natively supported in Azure Databricks, Azure Synapse and the upcoming [Microsoft Fabric](https://learn.microsoft.com/en-us/fabric/get-started/microsoft-fabric-overview) platform.

Delta Tables have versioning enabled by default which allows tracking of all updates to the tables. They can also [track row-level changes](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed) which can be read downstream for incremental processing. [Automatic schema validation](https://learn.microsoft.com/en-us/azure/databricks/delta/schema-validation) and [schema evolution](https://docs.databricks.com/delta/update-schema.html) both provide flexibility in managing evolving data schema. For more details, see the [design doc](https://github.com/UCLH-Foundry/Garden-Path/blob/main/designs/data-lake.md) for enabling Data Lake in FlowEHR.

### Unity Catalog 

In addition to being saved in a Storage account as Delta tables, the data in the Gold layer is materialized as an External Table in Databricks Unity Catalog.

Databricks Unity Catalog is optionally enabled during the deployment of FlowEHR (see [this pull request](https://github.com/UCLH-Foundry/FlowEHR/pull/326)). It enables us to save and query tables there directly and use features such as SQL Warehouse and SQL Query Editor in Databricks. Saving Gold tables in Unity Catalogue enables administrators of the system to enable fine-grained permission control on these tables, and simplifies data analysis and discovery.

## Processing

### Scheduling

To schedule the pipeline to run every week and process updates from the past week, we will create a [tumbling window trigger](https://learn.microsoft.com/en-us/azure/data-factory/how-to-create-tumbling-window-trigger) in ADF with a weekly frequency, and `startTime` set to the day when we receive the first batch of data.

This pipeline will run all jobs that process the data, as a single pipeline that consists of multiple activities.

### Incremental data processing: writing data to Bronze

To merge the changed data to the data lake, we need to perform an upsert.

The ["standard" way of performing an upsert](https://learn.microsoft.com/en-us/azure/databricks/delta/merge#python-1) would look like this:

```python
(targetTable
  .merge(sourceDF, "source.key = target.key")
  .whenMatchedUpdateAll()  # problematic
  .whenNotMatchedInsertAll()
  .whenNotMatchedBySourceDelete()
  .execute()
)
```

The problem with this is determining what needs to be updated when the key value matches, as the majority of the keys would match each week. For columns containing large texts, it can be expensive to determine which of them have changed. One of the ways to do it would be to calculate a hash of the text and use this value as a key. Based on that key, we can determine which values have changed.

For simplicity, we are not considering row updates in this example data pipeline.

### Incremental data processing: processing Bronze and Silver

Pipeline jobs that read from Bronze and Silver layers will use [Change Data Feed](https://learn.microsoft.com/en-us/azure/databricks/delta/delta-change-data-feed) to consume only the newly made updates. With Change Data Feed, we can read changes started from a specified version, like so: 

```python
update_df = spark.read.format("delta") \
  .option("readChangeFeed", "true") \
  .option("startingVersion", 0) \
  .option("endingVersion", 1) \
  .table("myDeltaTable")
```

Change Data Feed adds additional columns to the result of the read: `_change_type`, `_commit_version` and `_commit_timestamp`.

We need to ensure that for any run of the pipeline, we process the data that hasn't been processed yet. This means that we have to have a way to store, which was the last processed version of the table, and update it once another incremental update has been processed. To do this, we will maintain the value of `high_watermark`. `high_watermark` is the last version number known to be successfully processed, and stored independently for each job in the pipeline.

Each time the job runs, we will read all data for versions starting from `high_watermark`.

In case the job run **fails** and the job needs to be restarted, we will need to start processing again from the same version as the previous failed run.

In case the job run **succeeds**, we update the `high_watermark` value for this job to the last version number we've successfully processed, so that next time we run this job, we won't be processing these values again.

To keep track of the watermark values for each job, we will create a Delta table. It could look like this:

![DataPipeline-HighWatermark.png](/assets/DataPipeline-HighWatermark.png)

The pseudocode for pipeline logic for pipeline writing to Silver and Gold could then look like this:

```python
# Get the low watermark version from `watermark` table
low_watermark = ...

# Get the latest version from the metadata for the source table.
# This will be our high_watermark
high_watermark = ...

# Read updates starting from one version higher than the last processed one
source_df = spark.read.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", low_watermark) \
    .option("endingVersion", high_watermark) \
    .table("progressNotesBronze")

# Do the processing
update_df = do_the_processing(source_df)

# Write the output, merging it into the next table
target_table.alias("source") \
    .merge(
        update_df[update_df["_change_type"] == "insert"].alias("insert"),
        "source.id = insert.id") \
    .whenNotMatchedInsertAll() \
    .execute()

target_table.alias("source") \
    .merge(
        update_df[update_df["_change_type"] == "delete"].alias("delete"),
        "source.id = delete.id") \
    .whenMatchedDelete() \
    .execute()

# Job has finished successfully at this point
# Update the high_watermark value in the table
```

> Note that in our use case, it is not critical if in very rare cases some data ends up being processed twice. This rare case can only happen if the job fails at the very end during the update of the watermark values.

Find the full implementation of this algorithm (with tests) in [datalake.py](./src/patient_notes/datalake.py) and [watermark.py](./src/patient_notes/watermark.py).
