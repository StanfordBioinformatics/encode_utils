import os
import json

#see to it that only upper-case vars get exported
package_path = __path__[0]


#: The conf data file that sets defaults for things like Lab and Award that are used
#: when submitting datasets to the DCC.
CONF_DATA_FILE = os.path.join(package_path,"conf_data.json")
conf = json.load(open(CONF_DATA_FILE))

#: Define constants for a few properties that are common to all ENCODE profiles:
#: The award property name that is common to all ENCODE Portal object profiles.
DCC_AWARD_ATTR = "award"

#: The lab property name that is common to all ENCODE Portal object profiles.
DCC_LAB_ATTR = "lab"

#: List of profiles that we track which don't have the 'award' and 'lab' properties.
AWARDLESS_PROFILES = ["replicate"] #these profiles don't have the 'award' and 'lab' properties.

#: Sets the default for the submitting lab name.
LAB = conf["lab"]

#: The prefix to add to each alias when submitting an object to the DCC.
#: Most profiles have an 'alias' key, which stores a list of object aliases useful to the lab.
#: When submitting objects to the DCC, these aliases must be prefixed with the lab name.
#: and end with a colon.
LAB_PREFIX = LAB + ":" 

#: Sets the default for the award of the submiting lab to associate submissions with.
AWARD = conf["award"] 

#:
AWARD_AND_LAB = {DCC_AWARD_ATTR: AWARD, DCC_LAB_ATTR: LAB}

del package_path
del conf
