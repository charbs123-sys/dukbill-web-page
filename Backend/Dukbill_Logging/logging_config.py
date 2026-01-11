import logging
import sys
import json

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)

    formatter = logging.Formatter(
        "%(message)s"
    )

    handler.setFormatter(formatter)

    # Avoid duplicate logs
    if not logger.handlers:
        logger.addHandler(handler)

    return logger
