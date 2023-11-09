# Design Principles

When developing data pipeline solutions based on Spark, it has worked well for us to follow common developer practices and apply them to data engineering: think testability, observability, and developer iteration. We summarise these principles and how we implemented them below.

1. **Testability**: All transformations are written in such a way to be unit-tested and integration-tested.

For writing unit-tests, we use Spark local clusters that are supported by `pyspark` library, as well as `testcontainers` for mocking out databases used as sources.

Writing integration tests is achieved by automating deployment of the pipeline including building artifacts, and enabling testing the pipeline through running it with pre-defined arguments and expecting it to succeed. Often it's enough to just see the pipeline run through successfully, optionally a step can be added to the pipeline which checks the output matches the expected.

2. **Continuous delivery and integration**: CI / CD is enabled for all pipelines.

For this, we use testing and staging environments. The environments are entirely separate from each other, so that data created in testing and staging cannot affect data in production. Engineers should be able to easily examine data outputs in testing and staging environments, which is achieved by using synthetic data. Generation of synthetic data is not covered in this repository.

3. **Fast developer iteration**: Local development workflow is enabled for all pipelines.

A developer is able to write and run unit tests locally, without having to access the cloud environment.

Additionally, all pipeline code lives in `.py` files to enable effective and customisable liniting and type checking. This is achieved by using [pre-commit](./.pre-commit-config.yaml).

4. **Observability**: All pipeline code should be observable to enable running in production at scale.

This is achieved by shipping all logs and metrics to the Azure Monitor. Helper methods are provided to simplify this, additionally FlowEHR creates commonly used [dashboards](./patient_notes/README.md#checking-logs-and-metrics) automatically during deployment.

5. **Extensibility**: It should be easy to add pipelines and pipeline steps as the organisation grows.

This is achieved by supporting any number of data pipeline repositories within FlowEHR, each of them supporting any number of data pipelines. Each pipeline corresponds to a single pipeline in Azure Data Factory. A pipeline only needs to define a `pipeline.json` file at its root, and certain Makefile targets. For more information, see [minimal pipeline example](./minimal/README.md).

## Limitations

1. Only Python pipelines are supported at the moment.
