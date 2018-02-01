# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""An API and scripts for submitting datasetss to the ENCODE Portal.
"""

import os
import json
import logging
import sys

#see to it that only upper-case vars get exported
package_path = __path__[0]

#: Define constants for a few properties that are common to all ENCODE profiles:
#: The award property name that is common to all ENCODE Portal object profiles.
AWARD_PROP_NAME = "award"

#: The lab property name that is common to all ENCODE Portal object profiles.
LAB_PROP_NAME = "lab"

#: dict. Stores the 'lab' property to the value of the environment variable DCC_LAB to act as
#: the default lab when submitting an object to the Portal.
#: encode_utils.connection.Connection.post() will use this default if this property doesn't appear
#: in the payload.
LAB = {}
try:
  LAB = {LAB_PROP_NAME: os.environ["DCC_LAB"]}
except KeyError:
  pass

#: dict. Stores the prefix to add to each object alias when submitting to the Portal.
#: Most profiles have an 'alias' key, which stores a list of aliase names that are
#: useful to the lab.  When submitting objects to the Portal, these aliases must be prefixed
#: with the lab name and end with a colon, and this configuration variable stores that
#: prefix value.
LAB_PREFIX = ""
if LAB:
  LAB_PREFIX = LAB[LAB_PROP_NAME] + ":"

#: dict. Stores the 'award' property to the value of the environment variable DCC_AWARD to act as
#: the default award when submiting an object to the Portal.
#: encode_utils.connection.Connection.post() will use this default if this property doesn't appear
#: in the payload, and the profile at hand isn't a member of the list
#: `encode_utils.utils.Profile.AWARDLESS_PROFILES`.
AWARD = {}
try:
  AWARD = {AWARD_PROP_NAME: os.environ["DCC_AWARD"]}
except KeyError:
  pass

#: THE ENCODE Portal URL that contains all the profiles (schemas).
PROFILES_URL = "https://www.encodeproject.org/profiles/"

DCC_DEV_MODE = "dev"
DCC_PROD_MODE = "prod"

DCC_MODES = {
  DCC_DEV_MODE: {"host": "test.encodedcc.org","url": "https://test.encodedcc.org"},
  DCC_PROD_MODE: {"host": "www.encodeproject.org","url": "https://www.encodeproject.org"}
}

#: Indicates the mode of the Portal to use, being either prod or dev, which in turn determines
#: which host to make HTTP requests to.
DCC_MODE = DCC_DEV_MODE
try:
  DCC_MODE = os.environ["DCC_MODE"]
except KeyError:
  pass

try:
  assert(DCC_MODE in DCC_MODES)
except AssertionError:
  print("Error: Environment variable DCC_MODE must be equal to {} or {}.".format(DCC_DEV_MODE,DCC_PROD_MODE))
  sys.exit(1)

#: The prod or dev Portal host name, determined by the value of DCC_MODE.
DCC_HOST = DCC_MODES[DCC_MODE]["host"]
#: The prod or dev Portal URL, determined by the value of the DCC_MODE.
DCC_URL = DCC_MODES[DCC_MODE]["url"]

#: The timeout in seconds when making HTTP requests via the 'requests' module.
TIMEOUT = 20

def get_logfile_name(log_level):
  filename = "log_eu_" + DCC_MODE + "_" + log_level + ".txt"
  return filename

#: The name of the debug logging instance.
DEBUG_LOGGER_NAME="debug"
#: The name of the error logging instance.
ERROR_LOGGER_NAME="error"

def _create_debug_logger():
  """
  Creates a verbose logger that logs all messages sent to it. There are two handlers - the first
  for STDOUT and the other for a file by the name of log_eu_$DCC_MODE_debug.txt.
  """
  level = DEBUG_LOGGER_NAME
  level_attr = getattr(logging,level.upper())
  f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(pathname)s:\t%(message)s')
  logger = logging.getLogger(level)
  logger.setLevel(level_attr)
  #Create debug file handler.
  filename = get_logfile_name(level)
  debug_fh = logging.FileHandler(filename=filename,mode="a")
  debug_fh.setLevel(level_attr)
  debug_fh.setFormatter(f_formatter)
  logger.addHandler(debug_fh)
  #Create console handler.
  ch = logging.StreamHandler(stream=sys.stdout)
  ch.setLevel(level_attr)
  ch.setFormatter(f_formatter)
  logger.addHandler(ch)

def _create_error_logger():
  """
  Creates a logger that logs messages at the ERROR level or greater. There is a single handler,
  which logs its messages to a file by the name of log_eu_$DCC_MODE_error.txt.
  """
  level = ERROR_LOGGER_NAME
  level_attr = getattr(logging,level.upper())
  f_formatter = logging.Formatter('%(asctime)s:%(name)s:%(pathname)s:\t%(message)s')
  logger = logging.getLogger(level)
  logger.setLevel(level_attr)
  #Create error file handler.
  filename = get_logfile_name(level)
  error_fh = logging.FileHandler(filename=filename,mode="a")
  error_fh.setLevel(level_attr)
  error_fh.setFormatter(f_formatter)
  logger.addHandler(error_fh)

#: A logging instance with a STDOUT stream handler and a debug file handler.
#: Both handlers log all messages sent to them.
#: The file handler writes to a file named ${dcc_mode}_debug.txt, which is
#: opened in append mode in the calling directory.
#_create_debug_logger()

#: A logging instance with an error file handler.
#: Messages >= logging.ERROR are logged to a file by the name of ${dcc_mode}_error.txt, which
#: is opened in append mode in the calling directory.
#_create_error_logger()

del _create_debug_logger
del _create_error_logger
del get_logfile_name
del package_path
