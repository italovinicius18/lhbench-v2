"""Metrics collection and persistence for benchmark."""

import json
import time
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, asdict, field
from .logger import get_logger


@dataclass
class ExecutionMetrics:
    """Metrics for a single execution."""

    operation: str
    framework: Optional[str] = None
    table: Optional[str] = None
    query_id: Optional[int] = None
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_seconds: Optional[float] = None
    status: str = "running"
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def complete(self, status: str = "success", error: Optional[str] = None):
        """Mark execution as complete."""
        self.end_time = time.time()
        self.duration_seconds = self.end_time - self.start_time
        self.status = status
        self.error = error

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return asdict(self)


class MetricsCollector:
    """Collects and persists benchmark metrics."""

    def __init__(self, output_path: Optional[Path] = None):
        """Initialize metrics collector."""
        self.logger = get_logger("metrics")

        if output_path is None:
            from .config_loader import get_config
            output_path = Path(get_config('METRICS_OUTPUT_PATH', '/data/gold/metrics'))

        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        self.metrics: List[ExecutionMetrics] = []
        self.benchmark_start = time.time()

    def start_operation(
        self,
        operation: str,
        framework: Optional[str] = None,
        table: Optional[str] = None,
        query_id: Optional[int] = None,
        **metadata
    ) -> ExecutionMetrics:
        """Start tracking an operation."""
        metric = ExecutionMetrics(
            operation=operation,
            framework=framework,
            table=table,
            query_id=query_id,
            metadata=metadata
        )

        self.metrics.append(metric)

        self.logger.info(
            "operation_started",
            operation=operation,
            framework=framework,
            table=table,
            query_id=query_id
        )

        return metric

    def complete_operation(
        self,
        metric: ExecutionMetrics,
        status: str = "success",
        error: Optional[str] = None,
        **additional_metadata
    ):
        """Complete an operation tracking."""
        metric.complete(status=status, error=error)
        metric.metadata.update(additional_metadata)

        self.logger.info(
            "operation_completed",
            operation=metric.operation,
            framework=metric.framework,
            duration=metric.duration_seconds,
            status=status
        )

    def add_metric(self, metric: ExecutionMetrics):
        """Add a completed metric."""
        self.metrics.append(metric)

    def save_metrics(self, filename: Optional[str] = None):
        """Save metrics to JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"benchmark_metrics_{timestamp}.json"

        output_file = self.output_path / filename

        metrics_data = {
            "benchmark_start": self.benchmark_start,
            "benchmark_duration": time.time() - self.benchmark_start,
            "total_operations": len(self.metrics),
            "metrics": [m.to_dict() for m in self.metrics],
            "summary": self._generate_summary()
        }

        with open(output_file, 'w') as f:
            json.dump(metrics_data, f, indent=2)

        self.logger.info("metrics_saved", file=str(output_file))
        return output_file

    def _generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics."""
        summary = {
            "by_operation": {},
            "by_framework": {},
            "failures": []
        }

        for metric in self.metrics:
            # By operation
            op = metric.operation
            if op not in summary["by_operation"]:
                summary["by_operation"][op] = {
                    "count": 0,
                    "total_duration": 0,
                    "avg_duration": 0,
                    "failures": 0
                }

            summary["by_operation"][op]["count"] += 1
            if metric.duration_seconds:
                summary["by_operation"][op]["total_duration"] += metric.duration_seconds

            if metric.status != "success":
                summary["by_operation"][op]["failures"] += 1

            # By framework
            if metric.framework:
                fw = metric.framework
                if fw not in summary["by_framework"]:
                    summary["by_framework"][fw] = {
                        "count": 0,
                        "total_duration": 0,
                        "avg_duration": 0,
                        "failures": 0
                    }

                summary["by_framework"][fw]["count"] += 1
                if metric.duration_seconds:
                    summary["by_framework"][fw]["total_duration"] += metric.duration_seconds

                if metric.status != "success":
                    summary["by_framework"][fw]["failures"] += 1

            # Track failures
            if metric.status != "success":
                summary["failures"].append({
                    "operation": metric.operation,
                    "framework": metric.framework,
                    "error": metric.error
                })

        # Calculate averages
        for op_stats in summary["by_operation"].values():
            if op_stats["count"] > 0:
                op_stats["avg_duration"] = op_stats["total_duration"] / op_stats["count"]

        for fw_stats in summary["by_framework"].values():
            if fw_stats["count"] > 0:
                fw_stats["avg_duration"] = fw_stats["total_duration"] / fw_stats["count"]

        return summary

    def get_metrics_by_framework(self, framework: str) -> List[ExecutionMetrics]:
        """Get all metrics for a specific framework."""
        return [m for m in self.metrics if m.framework == framework]

    def get_metrics_by_operation(self, operation: str) -> List[ExecutionMetrics]:
        """Get all metrics for a specific operation."""
        return [m for m in self.metrics if m.operation == operation]
