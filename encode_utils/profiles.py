# -*- coding: utf-8 -*-

###
# © 2018 The Board of Trustees of the Leland Stanford Junior University
# Nathaniel Watson
# nathankw@stanford.edu
###

"""
Contains a ``Profile`` class for working with profiles on the ENCODE Portal.  Note that the terms 
'profile' and 'schema' are used interchangeably in this package.
"""

import json
import logging
import os
import requests

import encode_utils as eu
import encode_utils.utils as euu


#: A debug ``logging`` instance.
DEBUG_LOGGER = logging.getLogger(eu.DEBUG_LOGGER_NAME + "." + __name__)
#: An error ``logging`` instance.
ERROR_LOGGER = logging.getLogger(eu.ERROR_LOGGER_NAME + "." + __name__)


class UnknownProfile(Exception):
  """
  Raised when the profile ID in question doesn't match any known profile ID.
  """
  pass

def get_profiles():
  """Creates a dictionary storing all public profiles on the Portal.

  Returns:
      `dict`: `dict` where each key is the profile's ID, and each value is a given profile's 
      JSON schema.  Each key is extracted from the profile's `id` property, after a 
      little formatting first.  The formatting works by removing the 
      '/profiles/' prefix and the '.json' suffix.  For example, the value of the `id` property 
      for the `genetic_modification.json` profile is
      `/profiles/genetic_modification.json`. The corresponding key in this `dict` is 
      `genetic_modification`.
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

  A defining purpose of this class is to validate the profile ID specified in a POST payload passed
  to ``encode_utils.connection.Connection.post()``.  This class is used to ensure that the profile 
  specified there is a known profile on the Portal.

  Args:
      profile_id: str. Typically the value of a record's `@id` property. It will be
        normalized to match the syntax of the profile ID keys in the `dict`
        ``encode_utils.profiles.Profile.PROFILES`` (which is set to the return value of
        the function ``encode_utils.profiles.Profile``). You can also pass in the pre-normalized 
        profile ID.
  """
  #Constant (`dict`) set to the return value of the function ``encode_utils.profiles.get_profiles()``. 
  # See documentation there for details.
  # Don't comment for sphinx since it will break the build process on Read The Docs because the 
  # list value is so large.
  PROFILES = get_profiles()

  #: List of profile IDs that don't have the `award` and `lab` properties. Consulted in 
  #: ``encode_utils.connection.Connection.post()`` to determine whether to set defaults for the 
  #: `lab` and `award` properties of a given profile.
  AWARDLESS_PROFILE_IDS = []
  for profile_id in PROFILES:
    if eu.AWARD_PROP_NAME not in PROFILES[profile_id]["properties"]:
      AWARDLESS_PROFILE_IDS.append(profile_id)

  #: Constant storing the `file.json` profile's ID.
  #: This is asserted for inclusion in ``Profile.PROFILES``.
  FILE_PROFILE_ID = "file"
  try:
    assert(FILE_PROFILE_ID in PROFILES)
  except AssertionError:
    raise Exception("Error: The profile for file.json has underwent a name change apparently and is no longer known to this package.")

  #: Constant storing a property name of the `file.json` profile.
  #: The stored name is asserted for inclusion in the set of `File` properties.
  SUBMITTED_FILE_PROP_NAME = "submitted_file_name"
  try:
    assert(SUBMITTED_FILE_PROP_NAME in PROFILES[FILE_PROFILE_ID]["properties"])
  except AssertionError:
    raise Exception("Error: The profile for file.json no longer includes the property {}.".format(FILE_PROFILE_ID))

  #: Constant storing a property name of the `file.json` profile.
  #: The stored name is asserted for inclusion in the set of `File` properties.
  MD5SUM_NAME_PROP_NAME = "md5sum"
  try:
    assert(MD5SUM_NAME_PROP_NAME in PROFILES[FILE_PROFILE_ID]["properties"])
  except AssertionError:
    raise Exception("Error: The profile for file.json no longer includes the property {}.".format(MD5SUM_NAME_PROP_NAME))
  

  def __init__(self,profile_id):
    """
    Args:
        profile_id: `str`. Typically the value of a record's `@id` property.
    """

    #: The normalized version of the passed-in `profile_id` to ``self.__init__()``. The normalization
    #: is neccessary in order to match the format of the profile IDs in ``Profile.PROFILES``.
    self.profile_id = self._set_profile_id(profile_id)

  def _set_profile_id(self,profile_id):
    """
    Normalizes the `profile_id` so that it matches the format of the profile IDs stored in 
    ``Profile.PROFILES``, and ensures that the normalized profile ID is a member of this list.

    Args:
        profile_id: `str`. The value of the ``profile_id`` argument in self.__init__()``.

    Returns:
        `str`: The normalized profile ID.
    Raises:
        UnknownProfile: The normalized profile ID is not a member of the list `Profile.PROFILES`.
    """
    orig_profile = profile_id
    profile_id = profile_id.strip("/").split("/")[0].lower()
    #Multi-word profile names are hypen-separated, i.e. genetic-modifications.
    profile_id = profile_id.replace("-","_")
    if not profile_id in Profile.PROFILES:
      profile_id = profile_id.rstrip("s")
      if not profile_id in Profile.PROFILES:
        raise UnknownProfile("Unknown profile ID '{}'.".format(orig_profile))
    return profile_id

  def get_profile(self):
    """Provides the JSON schema for the specified profile ID.

    Returns:
        `dict`: `dict` representing the profile's JSON schema.
    """
    return Profile.PROFILES[self.profile_id]

