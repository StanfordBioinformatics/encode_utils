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

import inflection
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
    # Remove the "private" profiles, since these have differing semantics.
    private_profiles = [x for x in profiles if x.startswith("_")]  # i.e. _subtypes
    for i in private_profiles:
        # _subtypes should be the only one
        profiles.pop(i)
    if "@type" in profiles: # A pseudo profile that doesn't count. 
        profiles.pop("@type")

    profile_id_hash = {}  # Instead of name as key, profile ID is key.
    for name in profiles:  # i.e. name=GeneticModification
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
          the function ``encode_utils.profiles.Profile``). You can also pass in the already normalized
          profile ID.
    """
    # Constant (`dict`) set to the return value of the function ``encode_utils.profiles.get_profiles()``.
    # See documentation there for details.
    # Don't comment for sphinx since it will break the build process on Read The Docs because the
    # list value is so large.
    PROFILES = get_profiles()

    #: List of profile IDs that don't have the `award` and `lab` properties. Consulted in
    #: ``encode_utils.connection.Connection.post()`` to determine whether to set defaults for the
    #: `lab` and `award` properties of a given profile.
    AWARDLESS_PROFILE_IDS = []
    NO_ALIAS_PROFILE_IDS = []
    for profile_id in PROFILES:
        profile_props = PROFILES[profile_id]["properties"]
        if eu.AWARD_PROP_NAME not in profile_props:
            AWARDLESS_PROFILE_IDS.append(profile_id)
        if eu.ALIAS_PROP_NAME not in profile_props:
            NO_ALIAS_PROFILE_IDS.append(profile_id)

    #: Constant storing the `file.json` profile's ID.
    #: This is asserted for inclusion in ``Profile.PROFILES``.
    FILE_PROFILE_ID = "file"
    try:
        assert(FILE_PROFILE_ID in PROFILES)
    except AssertionError:
        raise Exception(
            "Error: The profile for file.json underwent a name change apparently and is no longer known to this package.")

    #: Constant storing a property name of the `file.json` profile.
    #: The stored name is asserted for inclusion in the set of `File` properties.
    SUBMITTED_FILE_PROP_NAME = "submitted_file_name"
    try:
        assert(SUBMITTED_FILE_PROP_NAME in PROFILES[FILE_PROFILE_ID]["properties"])
    except AssertionError:
        raise Exception(
            "Error: The profile for file.json no longer includes the property {}.".format(FILE_PROFILE_ID))

    #: Constant storing a property name of the `file.json` profile.
    #: The stored name is asserted for inclusion in the set of `File` properties.
    MD5SUM_NAME_PROP_NAME = "md5sum"
    #: Constant storing the name of the property in a JSON object sub-schema that indicates whether
    #: the object is read only. 
    READ_ONLY_FLAG = "readonly"
    #: Constant storing the name of the property in a JSON object sub-schema that indicates whether 
    #: the object is submittable.
    NOT_SUBMITTABLE_FLAG = "notSubmittable"
    try:
        assert(MD5SUM_NAME_PROP_NAME in PROFILES[FILE_PROFILE_ID]["properties"])
    except AssertionError:
        raise Exception(
            "Error: The profile for file.json no longer includes the property {}.".format(MD5SUM_NAME_PROP_NAME))

    def __init__(self, profile_id):
        """
        Args:
            profile_id: `str`. Typically the value of a record's `@id` property.
        """

        #: Typically, the value of a record's @id attribute, which stores the profile name for
        #: the given record. For example, the @id value of genetic modification ENCGM701EET is 
        #: */genetic-modifications/ENCGM701EET/*. The provided `profile_id` is normalized
        #: so that it matches the format of the profile IDs stored as keys in the dict ``Profile.PROFILES``.
        #: For example, */genetic-modifications/ENCGM701EET/* would be normalized to genetic_modification.
        #: Of course, the exact name of a profile ID can be alternativly passed in.
        self.profile_id = self._set_profile_id(profile_id)
        #: The JSON schema for the profile.  Also accessible via the helper method `self.get_profile()`.
        self.schema = Profile.PROFILES[self.profile_id] 
        #: Equivalent to the 'properties' property in the schema. 
        self.properties = self.schema["properties"]
        #: A list of the property names that are non-writable. These are determined as properties
        #: in the schema whose subschemas include the property ``Profile.NOT_SUBMITTABLE_FLAG`` or
        #: the property ``Profile.READ_ONLY_FLAG``. 
        self.non_writable_props = []

        #: A list of the property names that are writable, which are those that don't fall into the
        #: self.non_writable_props category.
        self.writable_props = []
        for i in self.properties:
            if self.is_prop_not_submittable(i) or self.is_prop_read_only(i):
                self.non_writable_props.append(i)
            else:
                self.writable_props.append(i)

    def _set_profile_id(self, profile_id):
        """
        Normalizes the `profile_id` so that it matches the format of the profile IDs stored in
        ``Profile.PROFILES``, and ensures that the normalized profile ID is a member of this list.

        Args:
            profile_id: `str`. The value of the ``profile_id`` argument in ``self.__init__()``.

        Returns:
            `str`: The normalized profile ID.
        Raises:
            UnknownProfile: The normalized profile ID is not a member of the list `Profile.PROFILES`.
        """
        orig_profile = profile_id
        profile_id = profile_id.strip("/").split("/")[0].lower()
        # Multi-word profile names are hypen-separated, i.e. genetic-modifications.
        profile_id = profile_id.replace("-", "_")
        profile_id = inflection.singularize(profile_id)
        if not profile_id in Profile.PROFILES:
            raise UnknownProfile("Unknown profile ID '{}'.".format(orig_profile))
        return profile_id

    def filter_non_writable_props(self, rec_json, keep_identifying=False):
        """
        Filters out the non-writable properties from a record, using ``self.non_writable_props`` as 
        a filtering basis. 

        Args:
            rec_json: `dict`. The JSON serialization of a record that belongs to the profile 
              encapsulated through this instance.
            keep_identifying: `bool`. Setting this to True means to retain keys that are in the 
              `identifyingProperties` object property of the schema. 
        Returns:
            `dict`: The input minus any keys that aren't writable. 
        """
        for key in list(rec_json):
            if keep_identifying and self.is_prop_identifying(key):
                continue
            if key in self.non_writable_props:
                rec_json.pop(key)
        return rec_json


    def property(self,prop):
        """Returns the JSON schema of the specifed property name.

        Args:
            prop: `str`. The name of a property found in the the `dict` returned by ``self.properties``.
        Returns:
            `dict`: The JSON schema for the indicated property.
        """
        return self.properties[prop]

    def required_properties(self):
        """
        Returns the list of required properties to submit when creating a new record under the
        given profile. Only works when the profile contains a "required" key at the top level, as it
        is in the biosample profile. Doesn't at this time recognize conditionally required keys
        that appear in a subschema, such as 'anyOf' as demonstrated in the file profile.

        Returns:
            `list`: The list of required properties.
        """
        try:
            return self.schema["required"]
        except KeyError:
            return []

    def is_prop_identifying(self, prop):
        """
        Indicates whether the provided property name is listed as an identifying property in the 
        schema (see the schema property 'identifyingProperties').

        Args: 
            prop: `str`. The name of a property found in the the `dict` returned by ``self.properties``.
        Returns:
            `bool`: `True` if this is a identifying property, `False` otherwise. 
        """
        if prop in self.schema["identifyingProperties"]:
            return True
        return False

    def is_prop_not_submittable(self, prop):
        """
        Indicates whether the provided property name is one that a user can submit when creating
        or updating a record.

        Args:
            prop: `str`. The name of a property found in the the `dict` returned by ``self.properties``.
        Returns:
            `bool`: `True` if this is a non-submittable property, `False` otherwise. 
        """
        if self.NOT_SUBMITTABLE_FLAG in self.property(prop):
            return True
        return False

    def is_prop_read_only(self, prop):
        """
        Indicates whether the provided property name is one that is read-only and hence can't be
        modified by the end-user.

        Args:
            prop: `str`. The name of a property found in the the `dict` returned by ``self.properties``.
        Returns:
            `bool`: `True` if this is a read-only property, `False` otherwise. 
        """
        if self.READ_ONLY_FLAG in self.property(prop):
            return True
        return False
   
    def is_prop_required(self, prop):
        """
        Indicates whether the provided property name is one that is required to specify when
        creating a new record.

        Args:
            prop: `str`. The name of a property found in the the `dict` returned by ``self.properties``.
        Returns:
            `bool`: `True` if this is a required property, `False` otherwise. 
        """
        if prop in self.required_properties():
            return True
        return False

    def get_profile(self):
        """Provides the JSON schema for the specified profile ID.

        Returns:
            `dict`: `dict` being the value of `self.schema`, which is the profile's JSON schema. 
        """
        return self.schema
