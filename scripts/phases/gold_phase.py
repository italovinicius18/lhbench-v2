"""Gold Phase: Execute TPC-H queries and collect performance metrics."""

import sys
import subprocess
from pathlib import Path
from typing import Dict, Any
import json

sys.path.append('/app')
sys.path.append('/opt/benchmark')

from utils.logger import setup_logger
from utils.config_loader import ConfigLoader
from utils.metrics_collector import MetricsCollector


class GoldPhase:
    """Executes TPC-H queries against all frameworks."""

    def __init__(self):
        self.config = ConfigLoader().get_config()
        self.logger = setup_logger("gold_phase")
        self.metrics = MetricsCollector()
        self.scale_factor = self.config['TPCH_SCALE_FACTOR']
        self.frameworks = self.config['FRAMEWORKS']
        self.spark_master = self.config['SPARK_MASTER_URL']

    def run_framework_queries(self, framework: str) -> Dict[str, Any]:
        """Run TPC-H queries for a framework."""
        self.logger.info(
            f"📊 Starting {framework.upper()} query execution",
            framework=framework,
            scale_factor=self.scale_factor
        )

        metric = self.metrics.start_operation(
            operation="gold_queries",
            framework=framework
        )

        try:
            # Path to query executor script
            job_script = "/opt/spark/jobs/gold/query_executor.py"

            # Submit Spark job
            cmd = [
                "/opt/spark/bin/spark-submit",
                "--master", self.spark_master,
                "--deploy-mode", "client",
                "--name", f"gold-{framework}-sf{self.scale_factor}",
                job_script
            ]

            self.logger.info(
                "submitting_spark_job",
                framework=framework,
                command=" ".join(cmd)
            )

            env = dict(subprocess.os.environ)
            env['FRAMEWORK'] = framework
            env['TPCH_SCALE_FACTOR'] = str(self.scale_factor)

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                env=env
            )

            if result.returncode != 0:
                self.logger.error(
                    "spark_job_failed",
                    framework=framework,
                    stderr=result.stderr[-1000:]
                )
                self.metrics.complete_operation(
                    metric,
                    status="failed",
                    error=result.stderr[-500:]
                )
                raise Exception(f"Query execution failed for {framework}")

            self.logger.info(
                "✅ Query execution completed",
                framework=framework
            )

            self.metrics.complete_operation(metric, status="success")

            return {
                "status": "success",
                "framework": framework,
                "scale_factor": self.scale_factor
            }

        except Exception as e:
            self.logger.error(
                "query_execution_failed",
                framework=framework,
                error=str(e)
            )
            self.metrics.complete_operation(metric, status="failed", error=str(e))
            raise

    def execute(self) -> Dict[str, Any]:
        """Execute Gold phase for all frameworks."""
        self.logger.info(
            "🚀 Starting Gold Phase",
            frameworks=self.frameworks,
            scale_factor=self.scale_factor
        )

        results = {
            "scale_factor": self.scale_factor,
            "frameworks": {}
        }

        for framework in self.frameworks:
            try:
                result = self.run_framework_queries(framework)
                results["frameworks"][framework] = result
            except Exception as e:
                results["frameworks"][framework] = {
                    "status": "failed",
                    "error": str(e)
                }
                # Continue with other frameworks
                self.logger.warning(
                    f"Continuing with remaining frameworks after {framework} failure"
                )

        # Save summary metrics
        metrics_file = Path(f"/data/gold/metrics/gold_summary_sf{self.scale_factor}.json")
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_file, 'w') as f:
            json.dump(results, f, indent=2)

        self.logger.info(
            "✅ Gold Phase completed",
            results=results
        )

        return results


def execute() -> Dict[str, Any]:
    """Execute Gold phase."""
    phase = GoldPhase()
    return phase.execute()


if __name__ == "__main__":
    try:
        result = execute()
        print(json.dumps(result, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
