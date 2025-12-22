import logging
from typing import Optional
from pathlib import Path
import sys


def build_logger(
    name: str = "fec",
    level: int = logging.INFO,
    log_file: Optional[Path] = None,
) -> logging.Logger:
    """
    Logger that always writes to stdout and optionally to a file.
    Safe to call once per process.
    """

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False  # prevent double logs

    if logger.handlers:
        return logger  # already configured

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # stdout handler
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(level)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)

    # optional file handler
    if log_file:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


logger = build_logger(
    name="fec",
    level=logging.INFO,
    log_file=Path("logs/fec.log"),  # set to None if you don't want a file
)
