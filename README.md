# Data-Seedling

This repository serves as an example Data Pipeline designed to work with [FlowEHR](https://github.com/UCLH-Foundry/FlowEHR).

It showcases the way to write, test, deploy, and run data pipelines in production, and does so with a focus on long-term maintainability.

## FlowEHR

FlowEHR is a framework for creating secure data transformation pipelines in Azure. FlowEHR supports authoring multiple data pipelines through multiple repositories and automates the deployment of cloud resources to run these pipelines.

If you don't yet know about FlowEHR, please read [the FlowEHR README](https://github.com/UCLH-Foundry/FlowEHR/blob/main/README.md).

## Quick Start

For instructions on how to develop, run and deploy data pipelines, please see [this guide](./docs/quick_start.md).

## Design Principles

For more details about the design principles behind the solution, please see [design principles](./docs/design_principles.md).

## Pipelines

This repository consists of three data pipelines. 

The first one, the [helloworld](./helloworld/README.md), is the smallest possible data pipeline that works with FlowEHR. When run, it prints "Hello World!" to the output. It is useful to understand what kind of contract a FlowEHR data pipeline expects.

The second one, [example_transform](./example_transform/README.md), is a simple example data pipeline and is a great place to start if you are thinking of writing your data transformations. It shows you how you can write, test and deploy a basic data transformation using FlowEHR, and package data transformation code into a Python wheel. The result of the transformation is then saved into the Microsoft SQL database, and metrics and logs are exported into Azure Monitor. There is a unit test as well that shows how to use the test framework.

The third one, [patient_notes](./patient_notes/README.md), is a more complex data pipeline. It pseudonymizes patient clinical notes using [Microsoft Presidio](https://microsoft.github.io/presidio/) in the first step and extracts useful medical information from it using [Azure Text Analytics for Health](https://learn.microsoft.com/en-us/azure/ai-services/language-service/text-analytics-for-health/overview?tabs=ner) in the second step. It also showcases how processing can be done in an incremental manner using Change Data Capture. It can be useful if you are exploring similar scenarios in your data pipeline project.
