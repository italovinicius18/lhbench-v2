"""Structured logging setup for benchmark."""

import os
import sys
import logging
from pathlib import Path
from typing import Optional
import structlog


def setup_logger(name: str = "benchmark", log_file: Optional[Path] = None) -> structlog.BoundLogger:
    """Setup structured logger with console and file outputs."""

    log_level = os.getenv('LOG_LEVEL', 'INFO')

    # Configure standard logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level),
    )

    # Configure structlog
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
        structlog.dev.set_exc_info,
        structlog.processors.TimeStamper(fmt="iso"),
    ]

    # Add JSON or console renderer
    log_format = os.getenv('LOG_FORMAT', 'json')
    if log_format == 'json':
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, log_level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logger = structlog.get_logger(name)

    # Add file handler if requested
    log_to_file = os.getenv('LOG_TO_FILE', 'false').lower() == 'true'
    if log_file or log_to_file:
        file_path = log_file or Path(os.getenv('LOG_FILE_PATH', 'results/logs')) / f"{name}.log"
        file_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(file_path)
        file_handler.setLevel(getattr(logging, log_level))
        logging.getLogger().addHandler(file_handler)

    return logger


def get_logger(name: str = "benchmark") -> structlog.BoundLogger:
    """Get logger instance."""
    return structlog.get_logger(name)
