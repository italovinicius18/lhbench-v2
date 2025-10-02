"""Silver Phase: Convert Bronze data to lakehouse frameworks."""

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


class SilverPhase:
    """Converts Bronze Parquet to Iceberg, Delta, and Hudi tables."""

    def __init__(self):
        self.config = ConfigLoader().get_config()
        self.logger = setup_logger("silver_phase")
        self.metrics = MetricsCollector()
        self.scale_factor = self.config['TPCH_SCALE_FACTOR']
        self.frameworks = self.config['FRAMEWORKS']
        self.spark_master = self.config['SPARK_MASTER_URL']

    def run_framework_conversion(self, framework: str) -> Dict[str, Any]:
        """Run Spark job to convert all tables for a framework."""
        self.logger.info(
            f"🔄 Starting {framework.upper()} conversion",
            framework=framework,
            scale_factor=self.scale_factor
        )

        metric = self.metrics.start_operation(
            operation="silver_conversion",
            framework=framework
        )

        try:
            # Path to conversion script
            job_script = f"/opt/spark/jobs/silver/{framework}/convert_tables.py"

            # Submit Spark job
            cmd = [
                "/opt/spark/bin/spark-submit",
                "--master", self.spark_master,
                "--deploy-mode", "client",
                "--name", f"silver-{framework}-sf{self.scale_factor}",
                job_script
            ]

            self.logger.info(
                "submitting_spark_job",
                framework=framework,
                command=" ".join(cmd)
            )

            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                check=False,
                env={**dict(subprocess.os.environ), 'TPCH_SCALE_FACTOR': str(self.scale_factor)}
            )

            if result.returncode != 0:
                self.logger.error(
                    "spark_job_failed",
                    framework=framework,
                    stderr=result.stderr[-1000:]  # Last 1000 chars
                )
                self.metrics.complete_operation(
                    metric,
                    status="failed",
                    error=result.stderr[-500:]
                )
                raise Exception(f"Spark job failed for {framework}")

            self.logger.info(
                "✅ Framework conversion completed",
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
                "framework_conversion_failed",
                framework=framework,
                error=str(e)
            )
            self.metrics.complete_operation(metric, status="failed", error=str(e))
            raise

    def execute(self) -> Dict[str, Any]:
        """Execute Silver phase for all frameworks."""
        self.logger.info(
            "🚀 Starting Silver Phase",
            frameworks=self.frameworks,
            scale_factor=self.scale_factor
        )

        results = {
            "scale_factor": self.scale_factor,
            "frameworks": {}
        }

        for framework in self.frameworks:
            try:
                result = self.run_framework_conversion(framework)
                results["frameworks"][framework] = result
            except Exception as e:
                results["frameworks"][framework] = {
                    "status": "failed",
                    "error": str(e)
                }
                # Continue with other frameworks even if one fails
                self.logger.warning(
                    f"Continuing with remaining frameworks after {framework} failure"
                )

        # Save summary metrics
        metrics_file = Path(f"/data/gold/metrics/silver_summary_sf{self.scale_factor}.json")
        metrics_file.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_file, 'w') as f:
            json.dump(results, f, indent=2)

        self.logger.info(
            "✅ Silver Phase completed",
            results=results
        )

        return results


def execute() -> Dict[str, Any]:
    """Execute Silver phase."""
    phase = SilverPhase()
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
