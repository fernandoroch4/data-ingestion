import os

from aws_embedded_metrics import MetricsLogger, metric_scope
from aws_embedded_metrics.config import get_config


class EmfMetrics:
    """Cloudwatch Embedded Metrics Format (EMF) for custom metrics"""

    @staticmethod
    def setup():
        metrics_config = get_config()
        metrics_config.namespace = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", metrics_config.namespace) + "_emf"

        # This speeds up unit tests; otherwise it auto-detects and tries to connect to HTTP sockets
        metrics_config.environment = os.environ.get("AWS_EXECUTION_ENV", "local")

    @staticmethod
    @metric_scope
    def put_duration(name: str, duration_seconds: float, metrics: MetricsLogger):
        metrics.put_metric(name, duration_seconds, "Seconds")

    @staticmethod
    @metric_scope
    def put_count(name: str, count: int, metrics: MetricsLogger):
        metrics.put_metric(name, count, "Count")
