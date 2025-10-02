"""Bronze Phase: TPC-H data generation using tpchgen-cli."""

import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime

import sys
sys.path.append('/app')
sys.path.append('/opt/benchmark')

from scripts.utils.logger import setup_logger
from scripts.utils.config_loader import load_config
from scripts.utils.metrics_collector import MetricsCollector


class BronzePhase:
    """Generates TPC-H data using high-performance tpchgen-cli."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = setup_logger("bronze_phase")
        self.metrics = MetricsCollector()

        self.scale_factor = config['TPCH_SCALE_FACTOR']
        self.tables = config['TPCH_TABLES']
        self.bronze_path = Path(config['BRONZE_PATH'])
        self.metadata_path = Path(config['METADATA_PATH'])
        self.force_regenerate = config['FORCE_REGENERATE']

        # tpchgen-cli settings
        self.format = config['TPCHGEN_FORMAT']
        self.compression = config['TPCHGEN_COMPRESSION']
        self.parts = config['TPCHGEN_PARTS']
        self.row_group_bytes = config['TPCHGEN_PARQUET_ROW_GROUP_BYTES']

    def execute(self) -> Dict[str, Any]:
        """Execute Bronze phase."""
        self.logger.info("🔨 Starting Bronze Phase - TPC-H Data Generation")

        # Check if data already exists
        if self._is_data_generated() and not self.force_regenerate:
            self.logger.info("✓ Data already generated, skipping regeneration")
            return self._load_metadata()

        # Create directories
        self._prepare_directories()

        # Generate data for each scale factor
        results = self._generate_data()

        # Save metadata
        metadata = self._save_metadata(results)

        self.logger.info("✅ Bronze Phase completed successfully")
        return metadata

    def _is_data_generated(self) -> bool:
        """Check if data is already generated."""
        metadata_file = self.metadata_path / f"sf{self.scale_factor}_info.json"
        if not metadata_file.exists():
            return False

        # Verify all expected tables exist
        sf_path = self.bronze_path / f"sf{self.scale_factor}"
        if not sf_path.exists():
            return False

        for table in self.tables:
            table_file = sf_path / f"{table}.parquet"
            if not table_file.exists():
                return False

        return True

    def _load_metadata(self) -> Dict[str, Any]:
        """Load existing metadata."""
        metadata_file = self.metadata_path / f"sf{self.scale_factor}_info.json"
        with open(metadata_file, 'r') as f:
            return json.load(f)

    def _prepare_directories(self):
        """Prepare output directories."""
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        self.metadata_path.mkdir(parents=True, exist_ok=True)

        sf_path = self.bronze_path / f"sf{self.scale_factor}"
        sf_path.mkdir(parents=True, exist_ok=True)

        self.logger.info("directories_prepared", path=str(self.bronze_path))

    def _generate_data(self) -> Dict[str, Any]:
        """Generate TPC-H data using tpchgen-cli."""
        sf_path = self.bronze_path / f"sf{self.scale_factor}"

        self.logger.info(
            "generating_tpch_data",
            scale_factor=self.scale_factor,
            tables=len(self.tables),
            output_path=str(sf_path)
        )

        results = {
            "scale_factor": self.scale_factor,
            "tables": {},
            "total_duration": 0,
            "generation_time": datetime.now().isoformat()
        }

        # Generate each table
        for table in self.tables:
            metric = self.metrics.start_operation(
                operation="generate_table",
                table=table,
                scale_factor=self.scale_factor
            )

            try:
                table_result = self._generate_table(table, sf_path)
                results["tables"][table] = table_result
                results["total_duration"] += table_result["duration"]

                self.metrics.complete_operation(metric, **table_result)

            except Exception as e:
                self.logger.error("table_generation_failed", table=table, error=str(e))
                self.metrics.complete_operation(metric, status="failed", error=str(e))
                raise

        return results

    def _generate_table(self, table: str, output_path: Path) -> Dict[str, Any]:
        """Generate single table using tpchgen-cli."""

        # Build tpchgen-cli command
        cmd = [
            "tpchgen-cli",
            f"--scale-factor={self.scale_factor}",
            f"--tables={table}",
            f"--output-dir={output_path}",
            f"--format={self.format}",
            f"--parts={self.parts}",
        ]

        if self.format == "parquet":
            cmd.append(f"--parquet-row-group-bytes={self.row_group_bytes}")
            cmd.append(f"--parquet-compression={self.compression}")

        self.logger.info("running_tpchgen", table=table, command=" ".join(cmd))

        # Execute tpchgen-cli
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True
        )

        # Parse output for statistics
        output_file = output_path / f"{table}.parquet"
        file_size = output_file.stat().st_size if output_file.exists() else 0

        # Extract duration from stderr (tpchgen logs timing info)
        duration = self._extract_duration(result.stderr)

        table_result = {
            "status": "success",
            "file_path": str(output_file),
            "file_size_bytes": file_size,
            "file_size_mb": round(file_size / (1024 * 1024), 2),
            "duration": duration,
            "stdout": result.stdout,
        }

        self.logger.info(
            "table_generated",
            table=table,
            size_mb=table_result["file_size_mb"],
            duration=duration
        )

        return table_result

    def _extract_duration(self, stderr: str) -> float:
        """Extract duration from tpchgen stderr output."""
        # tpchgen-cli outputs timing info in stderr
        # Example: "Generated in 1.234s"
        import re
        match = re.search(r'(\d+\.?\d*)\s*s', stderr)
        if match:
            return float(match.group(1))
        return 0.0

    def _save_metadata(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Save generation metadata."""
        metadata = {
            "scale_factor": self.scale_factor,
            "generation_time": results["generation_time"],
            "total_duration_seconds": results["total_duration"],
            "config": {
                "format": self.format,
                "compression": self.compression,
                "parts": self.parts,
                "row_group_bytes": self.row_group_bytes,
            },
            "tables": results["tables"],
            "total_size_mb": sum(
                t["file_size_mb"] for t in results["tables"].values()
            ),
        }

        # Save scale-factor specific metadata
        sf_metadata_file = self.metadata_path / f"sf{self.scale_factor}_info.json"
        with open(sf_metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)

        # Update generation index
        self._update_generation_index(metadata)

        # Save metrics
        self.metrics.save_metrics(f"bronze_sf{self.scale_factor}.json")

        self.logger.info("metadata_saved", file=str(sf_metadata_file))

        return metadata

    def _update_generation_index(self, metadata: Dict[str, Any]):
        """Update generation index file."""
        index_file = self.metadata_path / "generation_index.json"

        # Load existing index
        index = {}
        if index_file.exists():
            with open(index_file, 'r') as f:
                index = json.load(f)

        # Update index
        sf_key = f"sf{self.scale_factor}"
        index[sf_key] = {
            "generation_time": metadata["generation_time"],
            "total_size_mb": metadata["total_size_mb"],
            "tables": list(metadata["tables"].keys()),
        }

        # Save index
        with open(index_file, 'w') as f:
            json.dump(index, f, indent=2)


def execute(config: Dict[str, Any] = None) -> Dict[str, Any]:
    """Execute Bronze phase."""
    if config is None:
        config = load_config()

    phase = BronzePhase(config)
    return phase.execute()


if __name__ == "__main__":
    import sys

    # Load config
    config = load_config()

    # Execute Bronze phase
    try:
        result = execute(config)
        print(json.dumps(result, indent=2))
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
