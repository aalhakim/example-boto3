#!python3

"""
Dictionary schema for logging.config
"""

# Standard Library Imports
import os
import sys
import logging


########################################################################
# Determine root directory
if sys.platform.lower().startswith('win'):
    ROOT = "C:/Bboxx/.eventlogs"
else:
    ROOT = "~/.eventlogsBboxx"
TOOLNAME = "example-boto"

# Create a location to store the logs in
PATH = os.path.join(ROOT, ".eventlogs", TOOLNAME)
if not os.path.exists(PATH):
    os.makedirs(PATH)


########################################################################
config = {
    "version": 1,

    "formatters": {
        "fileFormatter": {
            "format": "%(asctime)s | %(thread)6d | %(levelname)8s | %(name)s.%(funcName)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        },
        "streamFormatter": {
            "format": "%(asctime)s | %(levelname)7s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },

    "handlers": {
        "debugFileHandler": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "fileFormatter",
            "level": "DEBUG",
            "filename": "{}".format(os.path.join(PATH, "debug.log")),
            "mode": "a",
            "maxBytes": 1024*10,
            "backupCount": 3
        },
        "consoleFileHandler": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "fileFormatter",
            "level": "INFO",
            "filename": "{}".format(os.path.join(PATH, "console.log")),
            "mode": "a",
            "maxBytes": 1024*10,
            "backupCount": 3
        },
        "errorFileHandler": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "fileFormatter",
            "level": "ERROR",
            "filename": "{}".format(os.path.join(PATH, "error.log")),
            "mode": "a",
            "maxBytes": 1024*10,
            "backupCount": 3
        },
        "streamHandler": {
            "class": "logging.StreamHandler",
            "formatter": "streamFormatter",
            "level": "INFO",
            "stream": sys.stdout
        }
    },

    "root": {
        "level": "DEBUG",
        "handlers": ["debugFileHandler", "consoleFileHandler", "errorFileHandler", "streamHandler"]
    }
}
