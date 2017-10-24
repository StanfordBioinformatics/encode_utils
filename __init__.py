import os
import json

#see to it that only upper-case vars get exported
package_path = __path__[0]

CONF_DATA_FILE = os.path.join(package_path,"conf_data.json")

DCC_API_KEYS_FILE = os.path.join(package_path,"dcc_submitters.json")

del package_path
