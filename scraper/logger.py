import logging
import os
from config.setting import LOG_PATH


def get_logger(name: str = "scraper") -> logging.Logger:
    """
    Single shared logger -> logs/scraper.log + console
    Calling get_logger() multiple times returns the same instance (no duplicate handlers)
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger
    
    logger.setLevel(logging.DEBUG)

    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)


    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)-8s %(message)s]",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler - DEBUG and above
    fh = logging.FileHandler(LOG_PATH, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)

    # Console handler - INFO and above
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)

    return logger