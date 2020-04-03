#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Script containing a basic logger function. Can be directly used in projects
"""

import sys
import logging


def init_logger(filename):
    """
    Create logging object. This works for file logs and command prompt logs.
    param filename (str) : Path and filename with extension
    """
    text_format = "[%(asctime)s:%(lineno)s:%(levelname)s] %(message)s"
    formatter = logging.Formatter(text_format)

    logger = logging.getLogger(name="myLogger")
    logger.setLeve(logging.NOTSET)

    streamHandler = logging.StreamHandler(sys.stdout)
    streamHandler.setLevel(logging.WARNING)
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    fileHandler = logging.FileHandler(filename)
    fileHandler.setLevel(logging.DEBUG)
    fileHandler.setFormatter(formatter)
    logger.addHandler(fileHandler)

    return logger
