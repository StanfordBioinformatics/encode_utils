# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Contains a Profile class for working with profiles on the ENCODE Portal.
"""

import json
import logging
import os
import requests

import encode_utils as eu
import encode_utils.utils as euu


#: A descendent logger of the debug logger created in `encode_utils`
#: (see the function description for `encode_utils.create_debug_logger`)
DEBUG_LOGGER = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
#: A descendent logger of the error logger created in `encode_utils`
#: (see the function description for `encode_utils.create_error_logger`)
ERROR_LOGGER = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)


class UnknownProfile(Exception):
  """
  Raised when the profile in question doesn't match any valid profile name present in
  """
  pass

def get_profiles():
  """Creates a list of all public profiles on the Portal.

  Returns:
      list: list of dicts (profiles).
  """
  profiles = requests.get(eu.PROFILES_URL + "?format=json",
                          timeout=eu.TIMEOUT,
                          headers=euu.REQUEST_HEADERS_JSON).json()
  #Remove the "private" profiles, since thiese have differing semantics.
  private_profiles = [x for x in profiles if x.startswith("_")] #i.e. _subtypes
  for i in private_profiles:
    profiles.pop(i)

  profile_id_hash = {} #Instead of name as key, profile ID is key.
  for name in profiles: #i.e. name=GeneticModification
    profile_id = profiles[name]["id"].split("/")[-1].split(".json")[0]
    profile_id_hash[profile_id] = profiles[name]
  return profile_id_hash 

class Profile:
  """
  Encapsulates knowledge about the existing profiles on the Portal and contains useful methods
  for working with a given profile.

  The user supplies a profile name, typically the value of a record's `@id` property. It will be
  normalized to match the syntax of the profile IDs in the list returned by the function
  `get_profile_ids()`.
  """
  #: dict of the public profiles on the Portal. The key is the profile's ID.
  #: The profile ID is extracted from the profile's `id` property, after a little
  #: formatting first.  The formatting works by removing the 'profiles' prefix and the '.json' suffix.
  #: For example, the value of the 'id' property for the genetic_modification profile is
  #: `/profiles/genetic_modification.json`. The corresponding key in this dict is 
  #: `genetic_modification`.
  _PROFILES = get_profiles()

  #: List of profile IDs that don't have the 'award' and 'lab' properties.
  AWARDLESS_PROFILE_IDS = []
  for profile_id in _PROFILES:
    if eu.AWARD_PROP_NAME not in _PROFILES[profile_id]["properties"]:
      AWARDLESS_PROFILE_IDS.append(profile_id)

  FILE_PROFILE_ID = "file"
  try:
    assert(FILE_PROFILE_ID in _PROFILES)
  except AssertionError:
    raise Exception("Error: The profile for file.json has underwent a name change apparently and is no longer known to this package.")

  SUBMITTED_FILE_NAME_PROP = "submitted_file_name"
  try:
    assert(SUBMITTED_FILE_NAME_PROP in _PROFILES[FILE_PROFILE_ID]["properties"])
  except AssertionError:
    raise Exception("Error: The profile for file.json no longer includes the property {}.".format(FILE_PROFILE_ID))

  def __init__(self,profile_id):
    """
    Args:
        profile_id: str. Typically the value of a record's `@id` property.
    """

    #: The normalized version of the passed-in profile_id to the constructor. The normalization
    #: is neccessary in order to match the format of the profile IDs in the Profile._PROFILES.
    self.profile_id = self._set_profile_id(profile_id)

  def _set_profile_id(self,profile_id):
    """
    Normalizes profile_id so that it matches the format of the profile IDs in the list
    Profile._PROFILES, and ensures that the normalized profile ID is a member of this list.

    Args:
        profile_id: str. Typeically the value of a record's `@id` property.

    Returns:
        str: The normalized profile ID.
    Raises:
        UnknownProfile: The normalized profile ID is not a member of the list Profile._PROFILES.
    """
    orig_profile = profile_id
    profile_id = profile_id.strip("/").split("/")[0].lower()
    #Multi-word profile names are hypen-separated, i.e. genetic-modifications.
    profile_id = profile_id.replace("-","")
    if not profile_id in Profile._PROFILES:
      profile_id = profile_id.rstrip("s")
      if not profile_id in Profile._PROFILES:
        raise UnknownProfile("Unknown profile ID '{}'.".format(orig_profile))
    return profile_id

  def get_profile(self):
    """Provides the JSON schema for the specified profile ID.

    Returns:
        dict: dict representing the profile's JSON schema.
    """
    return Profile._PROFILES[self.profile_id]

