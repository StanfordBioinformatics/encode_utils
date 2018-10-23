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

# see to it that only upper-case vars get exported
package_path = __path__[0]

# Define constants for a few properties that are common to all ENCODE profiles:

#: The award property name that is common to all ENCODE Portal object profiles.
AWARD_PROP_NAME = "award"

#: The aliases property name that is common to almost all ENCODE Portal object profiles.
#: Notably, the following profiles lack this property as of 2018-04-03:
#: ['access_key_admin', 'publication', 'award', 'organism', 'page', 'image', 'user', 'lab']
ALIAS_PROP_NAME = "aliases"

#: The lab property name that is common to all ENCODE Portal object profiles.
LAB_PROP_NAME = "lab"

#: dict. Stores the lab property to the value of the environment variable `DCC_LAB` to serve as
#: the default lab when submitting an object to the Portal.
#: ``encode_utils.connection.Connection.post()`` will use this default if this property doesn't
#: appear in the payload.
LAB = {}
try:
    LAB = {LAB_PROP_NAME: os.environ["DCC_LAB"]}
except KeyError:
    pass

#: str. Stores the prefix to add to each record alias when doing a POST operation.
#: Most profiles have an 'alias' key, which stores a list of alias names that are
#: useful to the lab.  When POSTING objects to the Portal, these aliases must be prefixed
#: with the lab name and end with a colon, and this configuration variable stores that
#: prefix value.
LAB_PREFIX = ""
if LAB:
    LAB_PREFIX = LAB[LAB_PROP_NAME] + ":"

#: dict. Stores the award property to the value of the environment variable `DCC_AWARD` to act as
#: the default award when submiting an object to the Portal.
#: ``encode_utils.connection.Connection.post()`` will use this default if this property doesn't
#: appear in the payload, and the profile at hand isn't a member of the list
#: ``encode_utils.utils.Profile.AWARDLESS_PROFILES``.
AWARD = {}
try:
    AWARD = {AWARD_PROP_NAME: os.environ["DCC_AWARD"]}
except KeyError:
    pass

#: THE ENCODE Portal URL that points to all the profiles (schemas).
PROFILES_URL = "https://www.encodeproject.org/profiles/"

DCC_DEV_MODE = "dev"
DCC_PROD_MODE = "prod"

DCC_MODES = {
    DCC_DEV_MODE: {"host": "test.encodedcc.org", "url": "https://test.encodedcc.org"},
    DCC_PROD_MODE: {"host": "www.encodeproject.org", "url": "https://www.encodeproject.org"}
}

ENCODE_PROD_S3BUCKET="encode-files"
ENCODE_TEST_S3BUCKET="encoded-files-dev"

#: The timeout in seconds when making HTTP requests via the ``requests`` module.
TIMEOUT = 20

#: The name of the debug ``logging`` instance.
DEBUG_LOGGER_NAME = "eu_debug"
#: The name of the error ``logging`` instance created in ``encode_utils.connection.Connection()``,
#: and referenced elsewhere.
ERROR_LOGGER_NAME = "eu_error"
#: The name of the POST ``logging`` instance created in ``encode_utils.connection.Connection()``,
#: and referenced elsewhere.
POST_LOGGER_NAME = "eu_post"

#: A ``logging`` instance that logs all messages sent to it to STDOUT.
debug_logger = logging.getLogger(DEBUG_LOGGER_NAME)
level = logging.DEBUG
debug_logger.setLevel(level)
f_formatter = logging.Formatter('%(asctime)s:%(name)s:\t%(message)s')
ch = logging.StreamHandler(stream=sys.stdout)
ch.setLevel(level)
ch.setFormatter(f_formatter)
debug_logger.addHandler(ch)

del package_path
