"""Logging File for debug."""

import logging.config
import os
import pathlib


def setup(
        logging_ini: str = "logging.ini", loglevel: str = "INFO", debug: str = "false"
):
    """Set up logging system, using sane defaults.

    Set DEBUG to "true" to enable debug logging.
    Otherwise, set LOGLEVEL to the desired loglevel.
    """
    logging_ini = pathlib.Path(logging_ini)
    if not (logging_ini.exists() and logging_ini.is_file()):
        logging_ini = pathlib.Path(__file__).parent / logging_ini
    logging.config.fileConfig(logging_ini)
    loglevel = os.environ.get("LOGLEVEL", loglevel).upper()
    if os.environ.get("DEBUG", debug).casefold() == "true":
        loglevel = logging.DEBUG
    logging.getLogger().setLevel(loglevel)


setup()
