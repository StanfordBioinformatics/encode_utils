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

#: The timeout in seconds when making HTTP requests via the 'requests' module.                         
TIMEOUT = 20 

del package_path
