"""Benchmark phases."""

from .bronze_phase import BronzePhase, execute as execute_bronze
from .silver_phase import SilverPhase, execute as execute_silver
from .gold_phase import GoldPhase, execute as execute_gold
from .report_phase import ReportPhase, execute as execute_report

__all__ = [
    'BronzePhase',
    'SilverPhase',
    'GoldPhase',
    'ReportPhase',
    'execute_bronze',
    'execute_silver',
    'execute_gold',
    'execute_report',
]
