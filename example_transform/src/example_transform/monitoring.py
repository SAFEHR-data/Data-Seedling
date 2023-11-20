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
from typing import Any, Dict
import os

from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure import metrics_exporter
from opencensus.stats import aggregation, stats
from opencensus.stats.measure import MeasureFloat, MeasureInt
from opencensus.stats.view import View
from opencensus.tags.tag_key import TagKey
from opencensus.tags.tag_map import TagMap
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer


APPLICATION_INSIGHTS_EXPORT_INTERVAL = 60.0


def _telemetry_processor_callback_function(envelope: Any) -> None:
    envelope.tags["ai.cloud.role"] = "databricks"


class ExceptionTracebackFilter(logging.Filter):
    """
    If a record contains 'exc_info', it will only show in the 'exceptions'
    section of Application Insights without showing in the 'traces' section.
    In order to show it also in the 'traces' section, we need another log
    that does not contain 'exc_info'.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if record.exc_info:
            logger = logging.getLogger(record.name)
            _, exception_value, _ = record.exc_info
            message = f"{record.getMessage()}\nException message: '{exception_value}'"
            logger.log(record.levelno, message)

        return True


def initialize_logging(
    logging_level: int = logging.INFO,
    correlation_id: str | None = None,
) -> logging.LoggerAdapter:
    """Adds the Application Insights handler for the root logger and sets
    the given logging level. Creates and returns a logger adapter that integrates
    the correlation ID, if given, to the log messages.

    Args:
        logging_level: The logging level to set e.g., logging.WARNING.
        export_interval_seconds: How often the logs will be exported,
            the default is 5 seconds.
        correlation_id: Optional. The correlation ID that is passed on
            to the operation_Id in App Insights.

    Returns:
        logging.LoggerAdapter: A newly created logger adapter.
    """
    logger = logging.getLogger()

    try:
        azurelog_handler = AzureLogHandler(export_interval=APPLICATION_INSIGHTS_EXPORT_INTERVAL)
        azurelog_handler.add_telemetry_processor(_telemetry_processor_callback_function)
        azurelog_handler.addFilter(ExceptionTracebackFilter())
        logger.addHandler(azurelog_handler)
    except ValueError as e:
        logger.error(f"Failed to set Application Insights logger handler: {e}")

    config_integration.trace_integrations(["logging"])
    Tracer(sampler=AlwaysOnSampler())
    logger.setLevel(logging_level)

    extra = {}

    if correlation_id:
        extra = {"traceId": correlation_id}

    adapter = logging.LoggerAdapter(logger, extra)
    adapter.debug(f"Logger adapter initialized with extra: {extra}")

    return adapter


view_manager = stats.stats.view_manager
stats_recorder = stats.stats.stats_recorder
metric_measures: Dict[str, Any] = {}
metric_views: Dict[str, View] = {}
exporter: metrics_exporter.MetricsExporter = None


def send_rows_updated_metric(value: int, tags: dict[str, str]):
    """
    Helper method to send a value for rows_updated metric to Azure Montior.
    This is a wrapper around send_metric() method that sets several opinionated defaults.

    A dashboard using this metric is automatically created during FlowEHR deployment.

    Args:
        value (int | float): Measurement value of the metric to send.
        tags (dict[str, str]): Tags to be associated with the metric.
    """
    # Must match to the value of predefined_metric_name in FlowEHR repo
    # (infrastructure/transform/locals.tf)
    metric_name = "rows_updated"
    # Must match the top-level name of the pipeline for the automatic dashboard creation to work
    pipeline_name: str = "example_transform"

    send_metric(
        name=metric_name,
        view_name=f"{pipeline_name}/{metric_name}",
        value=value,
        tags=tags,
        tag_keys=[key for key in tags],
        description="Number of rows updated",
        unit="1",
        metric_type=MeasureInt,
        aggregation=aggregation.SumAggregation,
    )


def send_metric(
    name: str,
    view_name: str,
    value: int | float,
    tags: dict[str, str],
    tag_keys: list[str],
    description: str,
    unit: str,
    metric_type: MeasureInt | MeasureFloat,
    aggregation: type,
    export_interval_seconds: float = 5.0,
) -> None:
    """
    Sends a measurement of a metric to Azure Montior.

    The first time this method is called, it creates a metric measure, a corresponding
    View with columns and aggregations provided, registers it with the Azure metrics
    exporter, and sends the value of the metric. It initializes the exporter using the
    APPLICATIONINSIGHTS_CONNECTION_STRING env variable. If it isn't set, it will
    exit silently.

    The consequent times this method is called, it only sends another measurement of the metric.

    Args:
        name (str): Name of the metric.
        value (int | float): Measurement value of the metric to send.
        tags (dict[str, str]): Tags to be associated with the metric.
        description (str): Description of the metric.
        unit (str): Description of the unit, which must follow
            https://unitsofmeasure.org/ucum.
        view_name (str): Name to use for the Metric view.
        tag_keys (list[str]): Tag keys to aggregate on for the view created.
        metric_type (MeasureInt | MeasureFloat): Type to use for the created
            metric, can be either:
            (a) opencensus.stats.measure.MeasureInt
            (b) opencensus.stats.measure.MeasureFloat.
        aggregation (type): Type of aggregation as described in
            https://opencensus.io/stats/view/#aggregations.
        export_interval_seconds (float): How often the metrics will be exported,
            the default is every 5 seconds.
    """
    if "APPLICATIONINSIGHTS_CONNECTION_STRING" not in os.environ:
        logging.warning("APPLICATIONINSIGHTS_CONNECTION_STRING is not set, exiting")
        return

    global exporter
    global metric_measures
    global metric_views

    if exporter is None:
        exporter = metrics_exporter.new_metrics_exporter(export_interval=export_interval_seconds)

    if name not in metric_measures:
        metric_measures[name] = metric_type(name=name, description=description, unit=unit)
        metric_views[name] = View(
            name=view_name,
            description=description,
            columns=[TagKey(key) for key in tag_keys],
            measure=metric_measures[name],
            aggregation=aggregation(),
        )
        view_manager.register_view(metric_views[name])
        view_manager.register_exporter(exporter)

    # Prepare measurement map
    measurement_map = stats.stats.stats_recorder.new_measurement_map()
    tag_map = TagMap()
    for tag_name, tag_value in tags.items():
        tag_map.insert(tag_name, tag_value)

    if metric_type == MeasureInt:
        measurement_map.measure_int_put(metric_measures[name], value)
    elif metric_type == MeasureFloat:
        measurement_map.measure_float_put(metric_measures[name], value)
    else:
        raise ValueError("send_metric() works with either MeasureInt or MeasureFloat type metrics")
    measurement_map.record(tag_map)
