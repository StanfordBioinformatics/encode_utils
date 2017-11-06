import os
import json

#see to it that only upper-case vars get exported
package_path = __path__[0]

CONF_DATA_FILE = os.path.join(package_path,"conf_data.json")
conf = json.load(open(CONF_DATA_FILE))

#Define constants for a few attributes that are common to all ENCODE profiles:
DCC_AWARD_ATTR = "award"
DCC_LAB_ATTR = "lab"
AWARDLESS_PROFILES = ["replicate"] #these profiles don't have the 'award' and 'lab' attributes.


#custom variables
DCC_API_KEYS_FILE = os.path.join(package_path,"dcc_submitters.json")
#The file pointed to by DCC_API_KEYS_FILE should have the following JSON structure:
#
#{
#  "usr1": {
#    "email": "usr1@stanford.edu",
#    "api_key": "######",
#    "secret_key": "###########"
#  },  
#
#  "usr2": {   
#    "email": "usr2@stanford.edu",
#    "api_key": "######",
#    "secret_key": "###########"
#  }
#
#}

LAB = conf["lab"]
LAB_PREFIX = LAB + ":" 
AWARD = conf["award"] 
AWARD_AND_LAB = {DCC_AWARD_ATTR: AWARD, DCC_LAB_ATTR: LAB}

del package_path
del conf
